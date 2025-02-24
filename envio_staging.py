import requests
import psycopg2
from datetime import datetime
from constantes2 import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS

# Função para criar tabelas no banco de dados (se não existirem)
def criar_tabelas():
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS alunos_lize_teste (
                    id TEXT PRIMARY KEY,
                    nome TEXT,
                    matricula TEXT UNIQUE,
                    email TEXT,
                    classes TEXT[],
                    ativo BOOLEAN DEFAULT TRUE
                );
                """)
                cursor.execute("""
                CREATE TABLE IF NOT EXISTS turmas_lize_teste (
                    id TEXT PRIMARY KEY,
                    nome TEXT,
                    coordination TEXT,
                    school_year INTEGER
                );
                """)
                conexao.commit()
                print("✅ Tabelas criadas ou verificadas com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao criar tabelas: {e}")

# Função para obter dados da API com paginação
def obter_dados_api(url, tipo_dado):
    dados = []
    pagina = 1
    while url:
        print(f"🔄 Obtendo {tipo_dado} da página {pagina}...")  # Indicador de progresso
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            dados.extend(data.get("results", []))
            url = data.get("next")
            pagina += 1
        else:
            print(f"❌ Erro ao obter {tipo_dado} da API: {response.status_code}")
            break
    print(f"✅ Total de {tipo_dado} coletados: {len(dados)}")
    return dados

# Função para persistir dados em lote no banco de dados
def persistir_dados_em_lote(tabela, dados, colunas, conflito):
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                # Preparar os valores para inserção
                valores = []
                for dado in dados:
                    if tabela == "alunos_lize_teste":
                        # Mapear os campos dos alunos
                        valor = (
                            dado.get("id"),
                            dado.get("name"),
                            dado.get("enrollment_number"),
                            dado.get("email"),
                            [str(c["id"]) for c in dado.get("classes", [])],  # Extrair IDs das classes
                            True  # Campo "ativo"
                        )
                    elif tabela == "turmas_lize_teste":
                        # Mapear os campos das turmas
                        valor = (
                            dado.get("id"),
                            dado.get("name"),
                            dado.get("coordination"),
                            dado.get("school_year")
                        )
                    valores.append(valor)

                # Gerar a query dinamicamente
                placeholders = ", ".join(["%s"] * len(colunas))
                query = f"""
                    INSERT INTO {tabela} ({", ".join(colunas)})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflito}) DO UPDATE
                    SET {", ".join(f"{coluna} = EXCLUDED.{coluna}" for coluna in colunas if coluna != conflito)};
                """
                # Executar a query em lote
                cursor.executemany(query, valores)
                conexao.commit()
                print(f"✅ Dados persistidos em lote na tabela {tabela}.")
    except Exception as e:
        print(f"❌ Erro ao persistir dados em lote: {e}")

# Função para obter alunos do banco de dados local
def obter_alunos_banco():
    try:
        with psycopg2.connect(**DB_CONFIG) as conexao:
            with conexao.cursor() as cursor:
                cursor.execute("""
                SELECT unidade, sit, matricula, nome, turma
                FROM alunos_25_geral
                WHERE turma::NUMERIC >= 11500::NUMERIC LIMIT 5;
                """)
                alunos = cursor.fetchall()
                print(f"✅ Total de alunos coletados do banco: {len(alunos)}")
                return alunos
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return []

# Função para definir a etapa de ensino com base no código da turma
def definir_etapa_ensino(codigo_turma):
    if len(codigo_turma) < 3:
        return None
    terceiro_digito = codigo_turma[2]
    if codigo_turma.startswith("11"):
        return "Anos Iniciais" if terceiro_digito == "5" else "Anos Finais"
    elif codigo_turma.startswith("2"):
        return "Ensino Médio"
    return None

def processar_alunos():
    # Persistir dados da API localmente
    alunos_api = obter_dados_api("https://staging.lizeedu.com.br/api/v2/students/", "alunos")
    turmas_api = obter_dados_api(f"https://staging.lizeedu.com.br/api/v2/classes/?school_year={datetime.now().year}", "turmas")
    # Persistir alunos
    persistir_dados_em_lote("alunos_lize_teste", alunos_api, ["id", "nome", "matricula", "email", "classes", "ativo"], "matricula")
    # Persistir turmas
    persistir_dados_em_lote("turmas_lize_teste", turmas_api, ["id", "nome", "coordination", "school_year"], "id")

    # Obter alunos do banco de dados local (limitado a 5 alunos)
    alunos_banco = obter_alunos_banco()

    # Cache para rastrear matrículas já processadas
    matriculas_processadas = set()

    for unidade_cod, sit, matricula, nome, turma in alunos_banco:
        turma = turma.strip()
        unidade_nome = CODIGO_PARA_UNIDADE.get(unidade_cod)

        if not unidade_nome:
            print(f"❌ Código da unidade '{unidade_cod}' não encontrado.")
            continue

        etapa_ensino = definir_etapa_ensino(turma)
        if not etapa_ensino:
            print(f"❌ Não foi possível determinar a etapa de ensino para a turma {turma}.")
            continue

        coordination_ids = COORDINATION_IDS.get(unidade_nome)
        coordination_id = coordination_ids.get(etapa_ensino) if coordination_ids else None

        if not coordination_id:
            print(f"❌ Coordination ID não encontrado para {etapa_ensino} na unidade {unidade_nome}.")
            continue

        # Verificar se o aluno já existe na API (usando o banco de dados local)
        try:
            with psycopg2.connect(**DB_CONFIG) as conexao:
                with conexao.cursor() as cursor:
                    cursor.execute("SELECT id, nome, email, classes, ativo FROM alunos_lize_teste WHERE matricula = %s;", (matricula,))
                    aluno_api = cursor.fetchone()
        except Exception as e:
            print(f"❌ Erro ao buscar aluno no banco de dados local: {e}")
            continue

        # Verificar se a matrícula já foi processada
        if matricula in matriculas_processadas:
            print(f"⚠️ Matrícula duplicada encontrada: {matricula} (Aluno: {nome})")
            if aluno_api:
                if aluno_api[4]:  # Verificar se o aluno está ativo
                    desativar_aluno(aluno_api[0], nome)
                    print(f"❌ Aluno {nome} desativado por ser duplicado.")
            continue  # Pular para o próximo aluno após desativação

        # Registrar a matrícula como processada
        matriculas_processadas.add(matricula)

        # Se a situação do aluno for 2 ou 4, desativar na API se ele existir
        if int(sit) in [2, 4]:  
            if aluno_api:
                if aluno_api[4]:  # Verificar se o aluno está ativo
                    desativar_aluno(aluno_api[0], nome)
                    print(f"❌ Aluno {nome} desativado devido à situação {sit}.")
            else:
                print(f"❌ Aluno {nome} não encontrado na API para desativação.")
            continue  # Pular para o próximo aluno após desativação

        # Se o aluno não existe no banco local, insira-o
        if not aluno_api:
            email_gerado = f"{matricula}@alunos.smrede.com.br"
            if inserir_aluno(nome, matricula, email_gerado):
                print(f"✅ Aluno {nome} inserido na API.")
                # Após inserir, buscar o aluno novamente para obter o ID
                try:
                    with psycopg2.connect(**DB_CONFIG) as conexao:
                        with conexao.cursor() as cursor:
                            cursor.execute("SELECT id, classes FROM alunos_lize_teste WHERE matricula = %s;", (matricula,))
                            aluno_api = cursor.fetchone()
                except Exception as e:
                    print(f"❌ Erro ao buscar aluno no banco de dados local: {e}")
                    continue
        else:
            # Se o aluno já existe, verifique se precisa ser atualizado
            email_gerado = f"{matricula}@alunos.smrede.com.br"
            if aluno_api[1] != nome or aluno_api[2] != email_gerado:
                if atualizar_aluno(aluno_api[0], nome, matricula, email_gerado):
                    print(f"🔄 Aluno {nome} atualizado na API.")
            else:
                print(f"✅ Aluno {nome} já está atualizado na API.")

        # Associar aluno à turma (após inserção/atualização)
        if aluno_api:
            try:
                with psycopg2.connect(**DB_CONFIG) as conexao:
                    with conexao.cursor() as cursor:
                        cursor.execute("""
                            SELECT t.id AS turma_id, a.classes AS turmas_aluno
                            FROM turmas_lize_teste t
                            LEFT JOIN alunos_lize_teste a ON a.id = %s
                            WHERE t.coordination = %s AND t.nome = %s;
                        """, (aluno_api[0], coordination_id, turma))

                        resultado = cursor.fetchone()

                        if resultado:
                            turma_id, turmas_aluno = resultado

                            # Verifica se o aluno já está na turma correta
                            if turmas_aluno and turma_id in turmas_aluno:
                                print(f"✅ Aluno {nome} já está na turma correta ({turma}) na unidade {unidade_nome}.")
                            else:
                                # Associar o aluno à turma correta
                                if associar_aluno_turma(aluno_api[0], turma_id):
                                    print(f"🎓 Aluno {nome} associado à turma {turma} na unidade {unidade_nome}.")
                                else:
                                    print(f"❌ Erro ao associar aluno {nome} à turma {turma} na unidade {unidade_nome}.")
                        else:
                            print(f"❌ Turma '{turma}' não encontrada para a coordenação {coordination_id}.")
            except Exception as e:
                print(f"❌ Erro ao buscar turma no banco de dados local: {e}")

# Funções da API (mantidas conforme o original)
def associar_aluno_turma(student_id, school_class_id):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{student_id}/set_classes/"
    payload = {"school_classes": [str(school_class_id)]}
    response = requests.post(url, headers=HEADERS, json=payload)
    return response.status_code == 200

def atualizar_aluno(aluno_id, nome, matricula, email):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{aluno_id}/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.put(url, headers=HEADERS, json=data)
    return response.status_code == 200

def desativar_aluno(id_aluno, nome_aluno):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/disable/"
    response = requests.post(url, headers=HEADERS, json={})
    if response.status_code == 200 or response.status_code == 204:
        print(f"❌ Aluno {nome_aluno} desativado com sucesso!")
    else:
        print(f"❌ Erro ao desativar aluno {nome_aluno}: {response.status_code}")

def inserir_aluno(nome, matricula, email):
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.post(url, headers=HEADERS, json=data)
    return response.status_code == 201

# Execução principal
if __name__ == "__main__":
    criar_tabelas()
    processar_alunos()