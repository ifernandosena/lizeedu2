import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import hashlib
from datetime import datetime
from collections import defaultdict
from constantes import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS, TABELA_ALUNOS_GERAL, ANO_LETIVO_ATUAL

# Configuração de log tabular Enterprise
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

class LizeManager:
    def __init__(self):
        self.stats_trocas = defaultdict(lambda: defaultdict(int))
        self.turmas_ausentes = set()

        self.siglas_diretas = {
            "01": "BR", "02": "MD", "03": "SC", "04": "CD",
            "05": "TQ", "06": "NP", "09": "SP", "10": "BT",
            "11": "CG", "14": "MC", "15": "IG", "16": "FG", "17": "RB"
        }

    def criar_e_atualizar_tabelas(self):
        queries = [
            """CREATE TABLE IF NOT EXISTS turmas_lize (
                id TEXT PRIMARY KEY, nome TEXT, coordination TEXT, school_year INTEGER
            );""",
            """CREATE TABLE IF NOT EXISTS alunos_lize (
                id TEXT, nome TEXT, matricula TEXT, email TEXT, classes TEXT[],
                ativo BOOLEAN DEFAULT TRUE, ano_letivo INTEGER,
                PRIMARY KEY (matricula, ano_letivo)
            );""",
            "ALTER TABLE alunos_lize ADD COLUMN IF NOT EXISTS hash_estado TEXT;"
        ]
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for q in queries: cur.execute(q)

    def atualizar_mapa_turmas(self):
        """Busca TODAS as turmas na API (lidando com paginação) e atualiza o espelho local"""
        logging.info("🏗️  Sincronizando mapa completo de turmas da Lize...")
        url = f"https://app.lizeedu.com.br/api/v2/classes/?school_year={ANO_LETIVO_ATUAL}"
        todas_turmas = []

        try:
            # Loop de Paginação (O segredo para pegar as 215 turmas)
            while url:
                r = requests.get(url, headers=HEADERS, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    todas_turmas.extend(data.get("results", []))
                    url = data.get("next") # Pega a próxima página
                else:
                    logging.error(f"❌ Erro na API de turmas: {r.status_code}")
                    break

            if todas_turmas:
                dados_upsert = [(t["id"], t["name"], t["coordination"], t["school_year"]) for t in todas_turmas]
                sql = """INSERT INTO turmas_lize (id, nome, coordination, school_year)
                         VALUES %s ON CONFLICT (id) DO UPDATE SET
                         nome=EXCLUDED.nome, coordination=EXCLUDED.coordination"""
                with psycopg2.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur: 
                        execute_values(cur, sql, dados_upsert)
                logging.info(f"✅ {len(todas_turmas)} turmas sincronizadas com o banco local.")
        except Exception as e: 
            logging.error(f"❌ Falha crítica ao atualizar mapa de turmas: {e}")

    def definir_etapa_ensino(self, codigo_turma):
        if len(codigo_turma) < 3: return None
        terceiro_digito = codigo_turma[2]
        if codigo_turma.startswith("11"):
            return "Anos Iniciais" if terceiro_digito == "5" else "Anos Finais"
        elif codigo_turma.startswith("2"):
            return "Ensino Médio"
        return None

    def gerar_hash(self, nome, situacao_ativo, id_turma):
        return hashlib.md5(f"{nome.strip()}|{situacao_ativo}|{id_turma}".encode('utf-8')).hexdigest()

    def processar(self):
        self.criar_e_atualizar_tabelas()
        self.atualizar_mapa_turmas()

        logging.info(f"📥 Carregando dados da Tabela: {TABELA_ALUNOS_GERAL}...")
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT matricula, id, nome, classes, ativo, hash_estado FROM alunos_lize WHERE ano_letivo = %s", (ANO_LETIVO_ATUAL,))
                estado_local = {str(r[0]).strip(): {"id_api": r[1], "nome": r[2], "classes": r[3] or [], "ativo": r[4], "hash": r[5]} for r in cur.fetchall()}

                cur.execute("SELECT coordination, nome, id FROM turmas_lize WHERE school_year = %s", (ANO_LETIVO_ATUAL,))
                mapa_turmas = {(str(r[0]).strip(), str(r[1]).strip()): r[2] for r in cur.fetchall()}

                cur.execute(f"SELECT unidade, sit, matricula, nome, turma FROM {TABELA_ALUNOS_GERAL} WHERE turma::NUMERIC >= 11500::NUMERIC")
                alunos_origem = cur.fetchall()
                logging.info(f"📊 Foram encontrados {len(alunos_origem)} alunos na View de produção.")

        logging.info("⚙️  Iniciando motor de diffing...")
        upsert_banco_local = []
        matriculas_processadas = set()

        for unid_cod, sit, mat, nome, turma_n in alunos_origem:
            mat, nome, turma_n = str(mat).strip(), str(nome).strip(), str(turma_n).strip()
            unid_cod_str = str(unid_cod).zfill(2)
            sigla = self.siglas_diretas.get(mat[:2], "??")

            unidade_nome = CODIGO_PARA_UNIDADE.get(unid_cod_str)
            etapa_ensino = self.definir_etapa_ensino(turma_n)
            
            if not unidade_nome or not etapa_ensino: continue

            coord_id = COORDINATION_IDS.get(unidade_nome, {}).get(etapa_ensino)
            if not coord_id: continue

            id_turma_alvo = mapa_turmas.get((str(coord_id).strip(), turma_n))
            
            if not id_turma_alvo:
                self.turmas_ausentes.add(f"{unidade_nome} | Turma: {turma_n}")
                continue

            matriculas_processadas.add(mat)
            deve_estar_ativo = int(sit) not in [2, 4]
            novo_hash = self.gerar_hash(nome, deve_estar_ativo, id_turma_alvo)
            aluno_api = estado_local.get(mat)

            if not aluno_api:
                if deve_estar_ativo:
                    novo_id = self.api_insert(nome, mat, f"{mat}@alunos.smrede.com.br")
                    if novo_id:
                        logging.info(f"➕ INSERT OK | {sigla:<2} | Mat: {mat:<9} | {nome:<35}")
                        upsert_banco_local.append((novo_id, nome, mat, f"{mat}@alunos.smrede.com.br", [], True, ANO_LETIVO_ATUAL, novo_hash))
                        self.stats_trocas["Novos Alunos"][sigla] += 1
                continue

            if str(aluno_api.get("hash")) != novo_hash:
                id_aluno = aluno_api["id_api"]
                
                if aluno_api["ativo"] != deve_estar_ativo:
                    logging.info(f"🛑 STATUS | {sigla:<2} | Mat: {mat:<9} | {nome:<35} | {'Ativar' if deve_estar_ativo else 'Desativar'}")
                    self.api_enable(id_aluno) if deve_estar_ativo else self.api_disable(id_aluno)

                if deve_estar_ativo and id_turma_alvo not in aluno_api["classes"]:
                    logging.info(f"📚 TURMA  | {sigla:<2} | Mat: {mat:<9} | {nome:<35} | -> {turma_n}")
                    if self.api_set_classes(id_aluno, id_turma_alvo):
                        self.stats_trocas[f"Enturmação {turma_n}"][sigla] += 1

                upsert_banco_local.append((id_aluno, nome, mat, f"{mat}@alunos.smrede.com.br", [id_turma_alvo], deve_estar_ativo, ANO_LETIVO_ATUAL, novo_hash))

        matriculas_orfas = set(estado_local.keys()) - matriculas_processadas
        for mat_orfa in matriculas_orfas:
            aluno_orfa = estado_local[mat_orfa]
            if aluno_orfa["ativo"]:
                sigla_o = self.siglas_diretas.get(mat_orfa[:2], "??")
                logging.info(f"🗑️ FAXINA | Mat: {mat_orfa:<9} | {aluno_orfa['nome']:<35} | Desativando.")
                self.api_disable(aluno_orfa["id_api"])
                h_orfa = self.gerar_hash(aluno_orfa['nome'], False, "SEM_TURMA")
                upsert_banco_local.append((aluno_orfa["id_api"], aluno_orfa["nome"], mat_orfa, "", [], False, ANO_LETIVO_ATUAL, h_orfa))
                self.stats_trocas["Órfãos Desativados"][sigla_o] += 1

        if upsert_banco_local:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, """INSERT INTO alunos_lize (id, nome, matricula, email, classes, ativo, ano_letivo, hash_estado)
                                           VALUES %s ON CONFLICT (matricula, ano_letivo) DO UPDATE SET
                                           nome=EXCLUDED.nome, ativo=EXCLUDED.ativo, classes=EXCLUDED.classes, hash_estado=EXCLUDED.hash_estado""", upsert_banco_local)

        self.exibir_relatorio()

    def api_insert(self, nome, mat, email):
        try:
            res = requests.post("https://app.lizeedu.com.br/api/v2/students/", headers=HEADERS, json={"name": nome, "enrollment_number": mat, "email": email}, timeout=5)
            return res.json().get("id") if res.status_code == 201 else None
        except: return None

    def api_set_classes(self, id_aluno, id_t):
        try: return requests.post(f"https://app.lizeedu.com.br/api/v2/students/{id_aluno}/set_classes/", headers=HEADERS, json={"school_classes": [str(id_t)]}, timeout=5).status_code == 200
        except: return False

    def api_disable(self, id_a):
        try: requests.post(f"https://app.lizeedu.com.br/api/v2/students/{id_a}/disable/", headers=HEADERS, timeout=5)
        except: pass

    def api_enable(self, id_a):
        try: requests.post(f"https://app.lizeedu.com.br/api/v2/students/{id_a}/enable/", headers=HEADERS, timeout=5)
        except: pass

    def exibir_relatorio(self):
        print("\n" + "="*95 + "\n📊 RESUMO DE SINCRONIZAÇÃO (API LIZE)\n" + "="*95)
        if self.turmas_ausentes:
            print("⚠️  TURMAS NÃO ENCONTRADAS NO PORTAL (AÇÃO NECESSÁRIA):")
            for t in sorted(self.turmas_ausentes): print(f"   ❌ {t}")
            print("-" * 95)
        if not self.stats_trocas:
            print("✨ Tudo em ordem. Nenhuma alteração pendente detectada.")
        else:
            for categoria, unidades in sorted(self.stats_trocas.items()):
                total = sum(unidades.values())
                detalhe = ", ".join([f"{s}: {q}" for s, q in sorted(unidades.items())])
                print(f"🔸 {categoria:<30} | Total: {total:<4} | Detalhe: [{detalhe}]")
        print("="*95)
        logging.info("🏁 Sincronização Lize concluída.")

if __name__ == "__main__":
    LizeManager().processar()