import psutil
import requests
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_batch
from constantes import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS, TABELA_ALUNOS_GERAL, ANO_LETIVO_ATUAL

class AlunoProcessor:
    def __init__(self):
        self.alunos_cache = {}
        self.turmas_cache = {}
        
    def criar_tabelas(self):
        """Cria tabelas no banco de dados se não existirem"""
        queries = [
            """CREATE TABLE IF NOT EXISTS alunos_lize (
                id TEXT,
                nome TEXT,
                matricula TEXT,
                email TEXT,
                classes TEXT[],
                ativo BOOLEAN DEFAULT TRUE,
                ano_letivo INTEGER,
                PRIMARY KEY (matricula, ano_letivo)
            );""",
            """CREATE TABLE IF NOT EXISTS turmas_lize (
                id TEXT PRIMARY KEY, 
                nome TEXT, 
                coordination TEXT, 
                school_year INTEGER
            );"""
        ]
        try:
            with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
                for query in queries:
                    cursor.execute(query)
                conexao.commit()
                print("✅ Tabelas criadas/verificadas com sucesso.")
        except Exception as e:
            print(f"❌ Erro ao criar tabelas: {e}")

    def obter_dados_api(self, url, tipo_dado):
        dados, pagina = [], 1
        while url:
            print(f"🔄 Obtendo {tipo_dado} da página {pagina}...")
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                data = response.json()
                dados.extend(data.get("results", []))
                url = data.get("next")
                pagina += 1
            else:
                print(f"❌ Erro ao obter {tipo_dado}: {response.status_code}")
                break
        print(f"✅ Total de {tipo_dado} coletados: {len(dados)}")
        return dados

    def persistir_dados_em_lote(self, tabela, dados, colunas, conflito):
        """Versão Corrigida: Alinha colunas e valores para evitar o TypeError"""
        if not dados: return

        try:
            batch_size = 1000 if psutil.virtual_memory().percent < 70 else 500
            print(f"🛠 Batch size: {batch_size} | RAM: {psutil.virtual_memory().percent}%")

            with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
                # Criar placeholders dinâmicos (%s, %s...)
                placeholders = ", ".join(["%s"] * len(colunas))
                
                # Gerar a query de UPDATE para o conflito
                update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas if c not in conflito)
                
                # Se o conflito for uma tupla (matricula, ano_letivo), formatar corretamente
                conflito_str = ", ".join(conflito) if isinstance(conflito, list) else conflito

                query = f"""INSERT INTO {tabela} ({", ".join(colunas)}) VALUES ({placeholders})
                          ON CONFLICT ({conflito_str}) DO UPDATE SET {update_set};"""

                for i in range(0, len(dados), batch_size):
                    batch = dados[i:i + batch_size]
                    
                    if tabela == "alunos_lize":
                        valores = []
                        for d in batch:
                            # Só persiste no banco local se o aluno estiver ativo na Lize
                            if d.get("is_active") is True:
                                valores.append((
                                    d.get("id"), d.get("name"), str(d.get("enrollment_number")),
                                    d.get("email"), [str(c["id"]) for c in d.get("classes", [])],
                                    True, ANO_LETIVO_ATUAL
                                ))
                    else:
                        valores = [(
                            d.get("id"), d.get("name"), 
                            d.get("coordination"), d.get("school_year")
                        ) for d in batch]

                    execute_batch(cursor, query, valores)
                    conexao.commit()
                    print(f"✅ Lote {i//batch_size + 1} persistido - {len(batch)} registros")

        except Exception as e:
            print(f"❌ Erro ao persistir dados em lote: {e}")
            if 'conexao' in locals(): conexao.rollback()

    def obter_alunos_banco(self):
        try:
            with psycopg2.connect(**DB_CONFIG) as conexao:
                with conexao.cursor(name='alunos_stream', withhold=True) as cursor:
                    cursor.itersize = 500
                    cursor.execute(f"""
                        SELECT unidade, sit, matricula, nome, turma FROM {TABELA_ALUNOS_GERAL}
                        WHERE turma::NUMERIC >= 11500::NUMERIC ORDER BY matricula;
                    """)
                    for aluno in cursor:
                        yield (aluno[0], aluno[1], str(aluno[2]).strip(),
                               aluno[3].strip(), str(aluno[4]).strip())
        except Exception as e:
            print(f"❌ Erro ao obter alunos do banco: {e}")
            yield from ()

    def carregar_caches(self):
        with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
            # Filtra apenas o ano atual para o cache de trabalho
            cursor.execute("SELECT matricula, id, nome, email, ativo, classes FROM alunos_lize WHERE ano_letivo = %s;", (ANO_LETIVO_ATUAL,))
            self.alunos_cache = {
                row[0].strip(): {'id': row[1], 'nome': row[2], 'email': row[3],
                                'ativo': row[4], 'classes': row[5] or []}
                for row in cursor.fetchall()
            }

            cursor.execute("SELECT nome, coordination, id FROM turmas_lize WHERE school_year = %s;", (ANO_LETIVO_ATUAL,))
            self.turmas_cache = {}
            for nome, coordination, id_turma in cursor.fetchall():
                chave = (str(coordination).strip(), str(nome).strip())
                self.turmas_cache.setdefault(chave, []).append(id_turma)

            print(f"✅ Cache carregado: {len(self.alunos_cache)} alunos e {len(self.turmas_cache)} turmas para {ANO_LETIVO_ATUAL}.")

    def definir_etapa_ensino(self, codigo_turma):
        if len(codigo_turma) < 3: return None
        terceiro_digito = codigo_turma[2]
        if codigo_turma.startswith("11"):
            return "Anos Iniciais" if terceiro_digito == "5" else "Anos Finais"
        elif codigo_turma.startswith("2"):
            return "Ensino Médio"
        return None

    def gerenciar_status_aluno(self, aluno_info, nome, matricula, situacao):
        if situacao in [2, 4] and aluno_info and aluno_info['ativo']:
            if self.desativar_aluno(aluno_info['id'], nome, matricula):
                self.alunos_cache[matricula]['ativo'] = False
            return False
        elif situacao not in [2, 4] and aluno_info and not aluno_info['ativo']:
            if self.ativar_aluno(aluno_info['id'], nome, matricula):
                self.alunos_cache[matricula]['ativo'] = True
        return True

    def processar_aluno(self, unidade_cod, sit, matricula, nome, turma):
        unidade_nome = CODIGO_PARA_UNIDADE.get(unidade_cod)
        etapa_ensino = self.definir_etapa_ensino(turma)
        coord_id = COORDINATION_IDS.get(unidade_nome, {}).get(etapa_ensino) if unidade_nome else None

        if not all([unidade_nome, etapa_ensino, coord_id]): return False

        aluno_info = self.alunos_cache.get(matricula)
        if not self.gerenciar_status_aluno(aluno_info, nome, matricula, int(sit)): return False

        email_gerado = f"{matricula}@alunos.smrede.com.br"
        
        # Lógica de Inserção/Atualização original
        if not aluno_info:
            if self.inserir_aluno(nome, matricula, email_gerado):
                aluno_id = self.obter_id_aluno_por_matricula(matricula)
                if aluno_id:
                    self.alunos_cache[matricula] = {'id': aluno_id, 'nome': nome, 'email': email_gerado, 'ativo': True, 'classes': []}
        elif aluno_info['nome'] != nome or aluno_info['email'] != email_gerado:
            if self.atualizar_aluno(aluno_info['id'], nome, matricula, email_gerado):
                self.alunos_cache[matricula].update({'nome': nome, 'email': email_gerado})

        # Enturmação
        if matricula in self.alunos_cache and self.alunos_cache[matricula]['ativo']:
            aluno_id = self.alunos_cache[matricula]['id']
            turma_key = (str(coord_id).strip(), str(turma).strip())

            if turma_key in self.turmas_cache:
                for t_id in self.turmas_cache[turma_key]:
                    if t_id not in self.alunos_cache[matricula]['classes']:
                        if self.associar_aluno_turma(aluno_id, t_id):
                            self.alunos_cache[matricula]['classes'].append(t_id)
            else:
                print(f"⚠️ Turma '{turma}' (Coord: {coord_id}) não encontrada.")
        return True

    def processar_alunos(self):
        print("⏳ Coletando dados...")
        # Chamadas de API originais
        url_alunos = f"https://app.lizeedu.com.br/api/v2/students/?school_year={ANO_LETIVO_ATUAL}&is_active=true"
        alunos_api = self.obter_dados_api(url_alunos, "alunos")
        turmas_api = self.obter_dados_api(f"https://app.lizeedu.com.br/api/v2/classes/?school_year={ANO_LETIVO_ATUAL}", "turmas")

        print("⏳ Persistindo dados...")
        # Ajustado para passar a coluna 'ano_letivo'
        self.persistir_dados_em_lote("alunos_lize", alunos_api, 
            ["id", "nome", "matricula", "email", "classes", "ativo", "ano_letivo"], ["matricula", "ano_letivo"])
        
        self.persistir_dados_em_lote("turmas_lize", turmas_api, 
            ["id", "nome", "coordination", "school_year"], "id")

        self.carregar_caches()
        
        # Streaming dos 6.319 alunos do Monitora
        print(f"🚀 Processando alunos da View...")
        contador = 0
        for aluno in self.obter_alunos_banco():
            if self.processar_aluno(*aluno):
                contador += 1

            # ADICIONE ESTA TRAVA DE LOG:
            if contador % 100 == 0:
                print(f"⏳ Progresso: {contador} alunos verificados/processados...")                
        
        print(f"✅ Concluído! {contador} alunos processados.")

    # Métodos Auxiliares (Exatamente como os seus)
    def obter_id_aluno_por_matricula(self, matricula):
        with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM alunos_lize WHERE matricula = %s AND ano_letivo = %s", (matricula, ANO_LETIVO_ATUAL))
            res = cur.fetchone()
            return res[0] if res else None

    def atualizar_aluno(self, id, nome, mat, email):
        url = f"https://app.lizeedu.com.br/api/v2/students/{id}/"
        try:
            res = requests.put(url, headers=HEADERS, json={"name": nome, "enrollment_number": mat, "email": email}, timeout=5)
            return res.status_code == 200
        except Exception as e:
            print(f"⚠️ Erro ao atualizar {mat}: {e}")
            return False

    def associar_aluno_turma(self, sid, tid):
        url = f"https://app.lizeedu.com.br/api/v2/students/{sid}/set_classes/"
        try:
            res = requests.post(url, headers=HEADERS, json={"school_classes": [str(tid)]}, timeout=5)
            return res.status_code == 200
        except Exception as e:
            print(f"⚠️ Erro na enturmação: {e}")
            return False

    def desativar_aluno(self, id, nome, mat):
        res = requests.post(f"https://app.lizeedu.com.br/api/v2/students/{id}/disable/", headers=HEADERS)
        return res.status_code in [200, 204]

    def ativar_aluno(self, id, nome, mat):
        res = requests.post(f"https://app.lizeedu.com.br/api/v2/students/{id}/enable/", headers=HEADERS)
        return res.status_code in [200, 204]

    def inserir_aluno(self, nome, mat, email):
            try:
                res = requests.post("https://app.lizeedu.com.br/api/v2/students/", headers=HEADERS, 
                                    json={"name": nome, "enrollment_number": mat, "email": email}, timeout=5)
                return res.status_code == 201
            except Exception as e:
                print(f"⚠️ Erro ao inserir {mat}: {e}")
                return False

if __name__ == "__main__":
    p = AlunoProcessor()
    p.criar_tabelas()
    p.processar_alunos()