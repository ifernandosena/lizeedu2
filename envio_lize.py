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
                PRIMARY KEY (matricula, ano_letivo) -- Permite o mesmo aluno em anos diferentes
            );""",
            """CREATE TABLE IF NOT EXISTS turmas_lize (
                id TEXT PRIMARY KEY, nome TEXT, coordination TEXT, school_year INTEGER);"""
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
        """Obtém dados paginados da API"""
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
        """Persiste dados em lotes com gerenciamento dinâmico de memória"""
        if not dados: return
        
        try:
            batch_size = 1000 if psutil.virtual_memory().percent < 70 else 500
            print(f"🔧 Batch size: {batch_size} | RAM: {psutil.virtual_memory().percent}%")

            with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
                placeholders = ", ".join(["%s"] * len(colunas))
                update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in colunas if c != conflito)
                query = f"""INSERT INTO {tabela} ({", ".join(colunas)}) VALUES ({placeholders})
                          ON CONFLICT ({conflito}) DO UPDATE SET {update_set};"""

                for i in range(0, len(dados), batch_size):
                    if i > 0 and i % (5 * batch_size) == 0:
                        batch_size = 1000 if psutil.virtual_memory().percent < 70 else 500
                    
                    batch = dados[i:i + batch_size]
                    valores = [(
                        dado.get("id"), dado.get("name"), dado.get("enrollment_number"),
                        dado.get("email"), [str(c["id"]) for c in dado.get("classes", [])],
                        dado.get("is_active", True),
                        ANO_LETIVO_ATUAL
                    ) if tabela == "alunos_lize" else (
                        dado.get("id"), dado.get("name"), 
                        dado.get("coordination"), dado.get("school_year")
                    ) for dado in batch]

                    execute_batch(cursor, query, valores)
                    conexao.commit()
                    print(f"✅ Lote {i//batch_size + 1} persistido - {len(batch)} registros")
                
                print(f"✅ Todos os dados persistidos na tabela {tabela}")
        except Exception as e:
            print(f"❌ Erro ao persistir dados em lote: {e}")
            if 'conexao' in locals(): conexao.rollback()

    def obter_alunos_banco(self):
        """Gera alunos do banco local em streaming"""
        try:
            with psycopg2.connect(**DB_CONFIG) as conexao:
                with conexao.cursor(name='alunos_stream', withhold=True) as cursor:
                    cursor.itersize = 500
                    cursor.execute(f"""
                        SELECT unidade, sit, matricula, nome, turma FROM {TABELA_ALUNOS_GERAL}
                        WHERE turma::NUMERIC >= 11500::NUMERIC ORDER BY matricula;
                    """)
                    for aluno in cursor:
                        yield (aluno[0], aluno[1], aluno[2].strip(), 
                               aluno[3].strip(), aluno[4].strip())
        except Exception as e:
            print(f"❌ Erro ao obter alunos do banco: {e}")
            yield from ()

    def carregar_caches(self):
        """Carrega caches de alunos e turmas"""
        with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
            cursor.execute("SELECT matricula, id, nome, email, ativo, classes FROM alunos_lize WHERE ano_letivo = %s;", (ANO_LETIVO_ATUAL,))
            self.alunos_cache = {
                row[0]: {'id': row[1], 'nome': row[2], 'email': row[3], 
                         'ativo': row[4], 'classes': row[5] or []}
                for row in cursor.fetchall()
            }
            
            cursor.execute("SELECT nome, coordination, id FROM turmas_lize WHERE school_year = %s;",
                          (datetime.now().year,))
            self.turmas_cache = {}
            for nome, coordination, id_turma in cursor.fetchall():
                self.turmas_cache.setdefault((coordination, nome), []).append(id_turma)

    def definir_etapa_ensino(self, codigo_turma):
        """Define a etapa de ensino baseada no código da turma"""
        if len(codigo_turma) < 3: return None
        terceiro_digito = codigo_turma[2]
        if codigo_turma.startswith("11"):
            return "Anos Iniciais" if terceiro_digito == "5" else "Anos Finais"
        elif codigo_turma.startswith("2"):
            return "Ensino Médio"
        return None

    def gerenciar_status_aluno(self, aluno_info, nome, matricula, situacao):
        """Gerencia ativação/desativação de alunos"""
        if situacao in [2, 4] and aluno_info and aluno_info['ativo']:
            if self.desativar_aluno(aluno_info['id'], nome, matricula):
                self.alunos_cache[matricula]['ativo'] = False
            return False
        elif situacao not in [2, 4] and aluno_info and not aluno_info['ativo']:
            if self.ativar_aluno(aluno_info['id'], nome, matricula):
                self.alunos_cache[matricula]['ativo'] = True
        return True

    def processar_aluno(self, unidade_cod, sit, matricula, nome, turma):
        """Processa um aluno individualmente"""
        unidade_nome = CODIGO_PARA_UNIDADE.get(unidade_cod)
        etapa_ensino = self.definir_etapa_ensino(turma)
        coordination_id = COORDINATION_IDS.get(unidade_nome, {}).get(etapa_ensino) if unidade_nome else None
        
        if not all([unidade_nome, etapa_ensino, coordination_id]):
            return False

        aluno_info = self.alunos_cache.get(matricula)
        if not self.gerenciar_status_aluno(aluno_info, nome, matricula, int(sit)):
            return False

        email_gerado = f"{matricula}@alunos.smrede.com.br"
        if not aluno_info:
            if self.inserir_aluno(nome, matricula, email_gerado):
                aluno_id = self.obter_id_aluno_por_matricula(matricula)
                if aluno_id:
                    self.alunos_cache[matricula] = {
                        'id': aluno_id, 'nome': nome, 'email': email_gerado,
                        'ativo': True, 'classes': []
                    }
        elif aluno_info['nome'] != nome or aluno_info['email'] != email_gerado:
            if self.atualizar_aluno(aluno_info['id'], nome, matricula, email_gerado):
                self.alunos_cache[matricula].update({'nome': nome, 'email': email_gerado})

        if matricula in self.alunos_cache and self.alunos_cache[matricula]['ativo']:
            aluno_id = self.alunos_cache[matricula]['id']
            turma_key = (coordination_id, turma)
            
            if turma_key in self.turmas_cache:
                for turma_id in self.turmas_cache[turma_key]:
                    if turma_id not in self.alunos_cache[matricula]['classes']:
                        if self.associar_aluno_turma(aluno_id, turma_id):
                            self.alunos_cache[matricula]['classes'].append(turma_id)
        return True

    def processar_alunos(self):
        """Processa todos os alunos"""
        print("⏳ Iniciando coleta de dados...")
        alunos_api = self.obter_dados_api("https://app.lizeedu.com.br/api/v2/students/", "alunos")
        turmas_api = self.obter_dados_api(
            f"https://app.lizeedu.com.br/api/v2/classes/?school_year={datetime.now().year}", "turmas")
        
        print("⏳ Persistindo dados da API...")
        self.persistir_dados_em_lote("alunos_lize", alunos_api, 
            ["id", "nome", "matricula", "email", "classes", "ativo"], "matricula")
        self.persistir_dados_em_lote("turmas_lize", turmas_api, 
            ["id", "nome", "coordination", "school_year"], "id")
        
        print("⏳ Preparando ambiente...")
        alunos_banco = list(set(self.obter_alunos_banco()))  # Remove duplicados
        if not alunos_banco:
            print("❌ Nenhum aluno encontrado no banco local.")
            return
        
        self.carregar_caches()
        print(f"⏳ Processando {len(alunos_banco)} alunos...")
        
        tempo_inicio = datetime.now()
        contador = sum(1 for aluno in alunos_banco if self.processar_aluno(*aluno))
        
        tempo_total = (datetime.now() - tempo_inicio).total_seconds()
        print(f"\n✅ Processamento concluído!\n• Alunos processados: {contador}\n"
              f"• Tempo total: {int(tempo_total // 60)}m {int(tempo_total % 60)}s\n"
              f"• Velocidade média: {contador/max(1, tempo_total):.2f} alunos/seg")

    # Métodos auxiliares
    def obter_id_aluno_por_matricula(self, matricula):
        try:
            with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
                cursor.execute("SELECT id FROM alunos_lize WHERE matricula = %s;", (matricula,))
                return cursor.fetchone()[0] if cursor.rowcount > 0 else None
        except Exception as e:
            print(f"❌ Erro ao buscar ID do aluno: {e}")
            return None

    def atualizar_status_aluno_local(self, id_aluno, status, nome_aluno):
        try:
            with psycopg2.connect(**DB_CONFIG) as conexao, conexao.cursor() as cursor:
                cursor.execute("UPDATE alunos_lize SET ativo = %s WHERE id = %s;", 
                             (status, id_aluno))
                conexao.commit()
                print(f"✅ Status do aluno {nome_aluno} atualizado para {'ativo' if status else 'inativo'}.")
        except Exception as e:
            print(f"❌ Erro ao atualizar status do aluno {nome_aluno}: {e}")

    def atualizar_aluno(self, aluno_id, nome, matricula, email):
        """Atualiza os dados de um aluno na API do Lize"""
        url = f"https://app.lizeedu.com.br/api/v2/students/{aluno_id}/"
        data = {"name": nome, "enrollment_number": matricula, "email": email}
        response = requests.put(url, headers=HEADERS, json=data)
        
        if response.status_code == 200:
            print(f"✅ Aluno {nome} atualizado com sucesso!")
            return True
        else:
            print(f"❌ Erro ao atualizar aluno {nome}: {response.status_code}")
            return False            

    def associar_aluno_turma(self, student_id, school_class_id):
        aluno_nome = self.alunos_cache.get(student_id, {}).get('nome', 'Desconhecido')
        turma_nome = next((k[1] for k, v in self.turmas_cache.items() if school_class_id in v), 'Desconhecida')
        
        response = requests.post(
            f"https://app.lizeedu.com.br/api/v2/students/{student_id}/set_classes/",
            headers=HEADERS, json={"school_classes": [str(school_class_id)]})
        
        if response.status_code == 200:
            print(f"✅ Aluno {student_id} associado à turma {turma_nome} com sucesso!")
        return response.status_code == 200

    def desativar_aluno(self, id_aluno, nome_aluno, matricula):
        response = requests.post(
            f"https://app.lizeedu.com.br/api/v2/students/{id_aluno}/disable/",
            headers=HEADERS, json={})
        
        if response.status_code in [200, 204]:
            print(f"❌ Aluno {nome_aluno} desativado com sucesso!")
            self.atualizar_status_aluno_local(id_aluno, False, nome_aluno)
            return True
        else:
            print(f"❌ Erro ao desativar aluno {nome_aluno}: {response.status_code}")
            return False

    def ativar_aluno(self, id_aluno, nome_aluno, matricula):
        response = requests.post(
            f"https://app.lizeedu.com.br/api/v2/students/{id_aluno}/enable/",
            headers=HEADERS, json={})
        
        if response.status_code in [200, 204]:
            print(f"✅ Aluno {nome_aluno} ativado com sucesso!")
            self.atualizar_status_aluno_local(id_aluno, True, nome_aluno)
            return True
        else:
            print(f"❌ Erro ao ativar aluno {nome_aluno}: {response.status_code}")
            return False

    def inserir_aluno(self, nome, matricula, email):
        response = requests.post(
            "https://app.lizeedu.com.br/api/v2/students/",
            headers=HEADERS, json={"name": nome, "enrollment_number": matricula, "email": email})
        return response.status_code == 201

if __name__ == "__main__":
    processor = AlunoProcessor()
    processor.criar_tabelas()
    processor.processar_alunos()