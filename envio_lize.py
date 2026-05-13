import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import time
import hashlib
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict
from constantes import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS, TABELA_ALUNOS_GERAL, ANO_LETIVO_ATUAL

# Configuração de log tabular Enterprise
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

class LizeManager:
    def __init__(self):
        self.stats_lock = threading.Lock()
        self.stats_trocas = defaultdict(lambda: defaultdict(int))
        self.turmas_ausentes = set()
        self.session = requests.Session()
        # Aumenta o pool para suportar 20 threads simultaneas sem avisos
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.session.headers.update(HEADERS)

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
        logging.info("Sincronizando mapa completo de turmas da Lize...")
        url = f"https://app.lizeedu.com.br/api/v2/classes/?school_year={ANO_LETIVO_ATUAL}"
        todas_turmas = []

        try:
            while url:
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    todas_turmas.extend(data.get("results", []))
                    url = data.get("next")
                else:
                    logging.error(f"Erro na API de turmas: {r.status_code}")
                    break

            if todas_turmas:
                dados_upsert = [(t["id"], t["name"], t["coordination"], t["school_year"]) for t in todas_turmas]
                sql = """INSERT INTO turmas_lize (id, nome, coordination, school_year)
                         VALUES %s ON CONFLICT (id) DO UPDATE SET
                         nome=EXCLUDED.nome, coordination=EXCLUDED.coordination"""
                with psycopg2.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur: 
                        execute_values(cur, sql, dados_upsert)
                logging.info(f"OK: {len(todas_turmas)} turmas sincronizadas com o banco local.")
        except Exception as e: 
            logging.error(f"Falha critica ao atualizar mapa de turmas: {e}")

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

    def _sync_single_student(self, mat, aluno_origem, estado_local, mapa_turmas):
        unid_cod, sit, mat_db, nome, turma_n = aluno_origem
        mat = str(mat).strip()
        nome = str(nome).strip()
        turma_n = str(turma_n).strip()
        sigla = self.siglas_diretas.get(mat[:2], "??")

        try:
            turma_valida = int(turma_n) >= 11500
        except:
            turma_valida = False

        deve_estar_ativo = (int(sit) not in [2, 4]) and turma_valida
        id_turma_alvo = None
        if turma_valida:
            unid_cod_str = str(unid_cod).zfill(2)
            unidade_nome = CODIGO_PARA_UNIDADE.get(unid_cod_str)
            etapa_ensino = self.definir_etapa_ensino(turma_n)
            if unidade_nome and etapa_ensino:
                coord_id = COORDINATION_IDS.get(unidade_nome, {}).get(etapa_ensino)
                id_turma_alvo = mapa_turmas.get((str(coord_id).strip(), turma_n))

        if turma_valida and not id_turma_alvo:
            with self.stats_lock:
                self.turmas_ausentes.add(f"{unidade_nome or unid_cod} | Turma: {turma_n}")
            deve_estar_ativo = False 

        novo_hash = self.gerar_hash(nome, deve_estar_ativo, id_turma_alvo or "SEM_TURMA")
        aluno_api = estado_local.get(mat)

        if not aluno_api:
            if deve_estar_ativo:
                novo_id = self.api_insert(nome, mat, f"{mat}@alunos.smrede.com.br")
                if novo_id:
                    self.api_set_classes(novo_id, id_turma_alvo)
                    return (novo_id, nome, mat, f"{mat}@alunos.smrede.com.br", [id_turma_alvo] if id_turma_alvo else [], True, ANO_LETIVO_ATUAL, novo_hash)
            return None

        if str(aluno_api.get("hash")) != novo_hash:
            id_aluno = aluno_api["id_api"]
            status_acao = "MUDANÇA"
            if aluno_api["ativo"] != deve_estar_ativo:
                status_acao = "ATIVAR" if deve_estar_ativo else "DESATIVAR"
                if deve_estar_ativo: self.api_enable(id_aluno)
                else: self.api_disable(id_aluno)
            
            if deve_estar_ativo and id_turma_alvo:
                self.api_set_classes(id_aluno, id_turma_alvo)
            
            with self.stats_lock:
                self.stats_trocas[status_acao][sigla] += 1
            
            return (id_aluno, nome, mat, f"{mat}@alunos.smrede.com.br", [id_turma_alvo] if id_turma_alvo else [], deve_estar_ativo, ANO_LETIVO_ATUAL, novo_hash)
        return None

    def atualizar_cache_alunos(self):
        logging.info("Atualizando cache local de alunos (alunos_lize) via API (Ano Atual)...")
        
        r = self.session.get(f"https://app.lizeedu.com.br/api/v2/students/?school_year={ANO_LETIVO_ATUAL}&limit=1", timeout=15)
        if r.status_code != 200: return
        total_records = r.json().get("count", 0)
        pages = (total_records // 50) + 1
        
        logging.info(f"   -> Baixando {total_records} registros em {pages} paginas simultaneas...")

        def fetch_page(offset):
            url = f"https://app.lizeedu.com.br/api/v2/students/?school_year={ANO_LETIVO_ATUAL}&limit=50&offset={offset}"
            try:
                resp = self.session.get(url, timeout=15)
                return resp.json().get("results", []) if resp.status_code == 200 else []
            except Exception: return []

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM alunos_lize WHERE ano_letivo = {ANO_LETIVO_ATUAL}")
                
                offsets = [i * 50 for i in range(pages)]
                total_processados = 0
                
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(fetch_page, off) for off in offsets]
                    for future in as_completed(futures):
                        alunos_pg = future.result()
                        for a in alunos_pg:
                            classes = [c.get("id") for c in a.get("classes", []) if c.get("school_year") == ANO_LETIVO_ATUAL]
                            id_turma = str(classes[0]) if classes else "SEM_TURMA"
                            h = self.gerar_hash(a['name'], a['is_active'], id_turma)
                            
                            cur.execute("""
                                INSERT INTO alunos_lize (id, nome, matricula, email, classes, ativo, ano_letivo, hash_estado)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (matricula, ano_letivo) DO UPDATE SET 
                                    id = EXCLUDED.id, nome = EXCLUDED.nome, classes = EXCLUDED.classes, 
                                    ativo = EXCLUDED.ativo, hash_estado = EXCLUDED.hash_estado
                            """, (a['id'], a['name'], a['enrollment_number'], a.get('email'), [id_turma], a['is_active'], ANO_LETIVO_ATUAL, h))
                        
                        total_processados += len(alunos_pg)
                        if total_processados % 1000 == 0 or total_processados >= total_records:
                            logging.info(f"   -> {total_processados}/{total_records} sincronizados no cache...")
                conn.commit()
        logging.info("OK: Cache de alunos atualizado.")

    def processar(self):
        logging.info(f"Iniciando Sincronizacao Lize - Ano Letivo: {ANO_LETIVO_ATUAL}")
        self.criar_e_atualizar_tabelas()
        self.atualizar_mapa_turmas()
        
        # Atualiza o cache se estiver vazio
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM alunos_lize WHERE ano_letivo = {ANO_LETIVO_ATUAL}")
                if cur.fetchone()[0] == 0:
                    logging.info("Banco local vazio. Iniciando carga inicial...")
                    self.atualizar_cache_alunos()
        
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT matricula, id, nome, classes, ativo, hash_estado FROM alunos_lize WHERE ano_letivo = %s", (ANO_LETIVO_ATUAL,))
                estado_local = {str(r[0]).strip(): {"id_api": r[1], "nome": r[2], "classes": r[3] or [], "ativo": r[4], "hash": r[5]} for r in cur.fetchall()}
                cur.execute("SELECT coordination, nome, id FROM turmas_lize WHERE school_year = %s", (ANO_LETIVO_ATUAL,))
                mapa_turmas = {(str(r[0]).strip(), str(r[1]).strip()): r[2] for r in cur.fetchall()}
                # Filtro na Fonte: Somente alunos elegíveis para a Lize (Turma >= 11500 e Ativos)
                query_fonte = f"""
                    SELECT unidade, sit, matricula, nome, turma 
                    FROM {TABELA_ALUNOS_GERAL} 
                    WHERE (turma::NUMERIC >= 11500) AND (sit::NUMERIC NOT IN (2, 4))
                """
                cur.execute(query_fonte)
                alunos_origem = cur.fetchall()
        
        logging.info(f"Fonte da Verdade (Elegíveis): {len(alunos_origem)} alunos encontrados.")
        logging.info(f"Cache Local (alunos_lize): {len(estado_local)} registros.")
        logging.info("Iniciando comparação de dados...")

        upsert_banco_local = []
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submetemos todos os alunos para processamento paralelo
            future_to_mat = {executor.submit(self._sync_single_student, a[2], a, estado_local, mapa_turmas): a[2] for a in alunos_origem}
            
            for future in as_completed(future_to_mat):
                try:
                    res = future.result()
                    if res:
                        upsert_banco_local.append(res)
                except Exception as e:
                    mat_err = future_to_mat[future]
                    logging.error(f"Erro ao processar aluno {mat_err}: {e}")
        
        # Caso 3: Deletados na fonte (Intrusos ou formados)
        mats_origem = {str(a[2]).strip() for a in alunos_origem}
        fantasmas = []
        for mat_del, aluno_del in estado_local.items():
            # Só processa como fantasma se não estiver na fonte E ainda estiver ativo no cache
            if mat_del not in mats_origem and aluno_del.get("ativo") is True:
                fantasmas.append((mat_del, aluno_del))
        
        if fantasmas:
            logging.info(f"Detectados {len(fantasmas)} alunos fantasmas/intrusos. Iniciando desativação paralela...")
            
            def desativar_fantasma(item):
                mat_f, dados_f = item
                sigla = self.siglas_diretas.get(mat_f[:2], "??")
                logging.info(f"SUMIU DA FONTE | Mat: {mat_f} | {dados_f['nome']} | Desativando")
                if self.api_disable(dados_f["id_api"]):
                    h = self.gerar_hash(dados_f['nome'], False, "DELETADO")
                    with self.stats_lock:
                        self.stats_trocas["SUMIU DA FONTE"][sigla] += 1
                    return (dados_f["id_api"], dados_f["nome"], mat_f, "", [], False, ANO_LETIVO_ATUAL, h)
                return None

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(desativar_fantasma, f) for f in fantasmas]
                for future in as_completed(futures):
                    res = future.result()
                    if res: upsert_banco_local.append(res)

        if upsert_banco_local:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, """INSERT INTO alunos_lize (id, nome, matricula, email, classes, ativo, ano_letivo, hash_estado)
                                           VALUES %s ON CONFLICT (matricula, ano_letivo) DO UPDATE SET
                                           nome=EXCLUDED.nome, ativo=EXCLUDED.ativo, classes=EXCLUDED.classes, hash_estado=EXCLUDED.hash_estado""", upsert_banco_local)
        self.exibir_relatorio()

    def api_find_by_enrollment(self, mat):
        try:
            r = self.session.get(f"https://app.lizeedu.com.br/api/v2/students/?enrollment_number={mat}", timeout=15)
            if r.status_code == 200:
                for aluno in r.json().get("results", []):
                    if str(aluno.get("enrollment_number", "")).strip() == str(mat).strip():
                        return aluno.get("id")
            return None
        except Exception: return None

    def api_insert(self, nome, mat, email):
        try:
            res = self.session.post("https://app.lizeedu.com.br/api/v2/students/", json={"name": nome, "enrollment_number": mat, "email": email}, timeout=15)
            if res.status_code == 201: return res.json().get("id")
            if res.status_code == 400: return self.api_find_by_enrollment(mat)
            return None
        except Exception: return None

    def api_set_classes(self, id_aluno, id_t):
        try:
            classes = [str(id_t)] if id_t else []
            r = self.session.post(f"https://app.lizeedu.com.br/api/v2/students/{id_aluno}/set_classes/", json={"school_classes": classes}, timeout=15)
            return r.status_code in (200, 201, 204)
        except Exception: return False

    def api_disable(self, id_a):
        try:
            r = self.session.post(f"https://app.lizeedu.com.br/api/v2/students/{id_a}/disable/", timeout=15)
            if r.status_code in (200, 204):
                return True
            logging.warning(f"api_disable HTTP {r.status_code} | id {id_a}")
            return False
        except Exception as e:
            logging.warning(f"api_disable exception | id {id_a} | {e}")
            return False

    def api_enable(self, id_a):
        try:
            r = self.session.post(f"https://app.lizeedu.com.br/api/v2/students/{id_a}/enable/", timeout=15)
            if r.status_code in (200, 204):
                return True
            logging.warning(f"api_enable HTTP {r.status_code} | id {id_a}")
            return False
        except Exception as e:
            logging.warning(f"api_enable exception | id {id_a} | {e}")
            return False

    def exibir_relatorio(self):
        print("\n" + "="*95)
        print(f"RESUMO DE SINCRONIZACAO (API LIZE) - ANO LETIVO: {ANO_LETIVO_ATUAL}")
        print("="*95)
        if self.turmas_ausentes:
            print("TURMAS NÃO ENCONTRADAS NO PORTAL (AÇÃO NECESSÁRIA):")
            for t in sorted(self.turmas_ausentes): print(f"   - {t}")
            print("-" * 95)
        if not self.stats_trocas:
            print("Tudo em ordem. Nenhuma alteração pendente detectada.")
        else:
            for categoria, unidades in sorted(self.stats_trocas.items()):
                total = sum(unidades.values())
                detalhe = ", ".join([f"{s}: {q}" for s, q in sorted(unidades.items())])
                print(f"  {categoria:<30} | Total: {total:<4} | Detalhe: [{detalhe}]")
        print("="*95)
        logging.info(f"Sincronizacao Lize {ANO_LETIVO_ATUAL} concluida.")

if __name__ == "__main__":
    LizeManager().processar()