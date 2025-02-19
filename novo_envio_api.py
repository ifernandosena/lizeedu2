import requests, psycopg2
from datetime import datetime
from constantes import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS

def obter_alunos_api():
    """Obtém os alunos da API, considerando paginação."""
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    alunos = []
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            alunos.extend(data["results"])
            url = data.get("next")
        else:
            print("Erro ao obter alunos da API:", response.status_code, response.text)
            break
    return alunos

def obter_alunos_banco():
    """Obtém os alunos da tabela 'alunos_25_geral' no banco de dados."""
    try:
        conexao = psycopg2.connect(**DB_CONFIG)
        cursor = conexao.cursor()
        cursor.execute("""
        SELECT unidade, sit, matricula, nome, turma
        FROM alunos_25_geral
        WHERE turma::NUMERIC >= 11600 AND turma::NUMERIC <= 11900 limit 3
        """)
        alunos = cursor.fetchall()
        cursor.close()
        conexao.close()
        return alunos
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return []

def obter_turmas_api():
    """Obtém as turmas da API, considerando paginação."""
    ano_atual = datetime.now().year
    url = f"https://staging.lizeedu.com.br/api/v2/classes/?school_year={ano_atual}"
    turmas = []

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            turmas.extend(data["results"])
            url = data.get("next")
        else:
            print("Erro ao obter turmas da API:", response.status_code, response.text)
            break

    return turmas

def persistir_turmas_banco(turmas):
    """Persiste as turmas no banco de dados (criar a tabela turmas_lize se necessário)."""
    try:
        conexao = psycopg2.connect(**DB_CONFIG)
        cursor = conexao.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS turmas_lize (
            id TEXT PRIMARY KEY,
            name TEXT,
            coordination TEXT,
            school_year INT
        );
        """)
        # Inserir ou atualizar turmas
        for turma in turmas:
            cursor.execute("""
            INSERT INTO turmas_lize (id, name, coordination, school_year) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) 
            DO UPDATE SET name = EXCLUDED.name, coordination = EXCLUDED.coordination, school_year = EXCLUDED.school_year;
            """, (turma['id'], turma['name'], turma['coordination'], turma['school_year']))
        conexao.commit()
        cursor.close()
        conexao.close()
    except Exception as e:
        print(f"Erro ao persistir turmas no banco: {e}")

def persistir_alunos_banco(alunos):
    """Persiste os alunos na tabela alunos_lize do banco de dados."""
    try:
        conexao = psycopg2.connect(**DB_CONFIG)
        cursor = conexao.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS alunos_lize (
            matricula TEXT PRIMARY KEY,
            nome TEXT,
            email TEXT,
            turma TEXT,
            status INT
        );
        """)
        # Inserir ou atualizar alunos
        for aluno in alunos:
            cursor.execute("""
            INSERT INTO alunos_lize (matricula, nome, email, turma, status) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (matricula) 
            DO UPDATE SET nome = EXCLUDED.nome, email = EXCLUDED.email, turma = EXCLUDED.turma, status = EXCLUDED.status;
            """, (aluno['matricula'], aluno['nome'], aluno['email'], aluno['turma'], aluno['status']))
        conexao.commit()
        cursor.close()
        conexao.close()
    except Exception as e:
        print(f"Erro ao persistir alunos no banco: {e}")

def atualizar_aluno(aluno_id, nome, matricula, email):
    """Atualiza os dados de um aluno na API, caso haja modificações."""
    url = f"https://staging.lizeedu.com.br/api/v2/students/{aluno_id}/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.put(url, headers=HEADERS, json=data)
    return response.status_code == 200

def desativar_aluno(aluno_id):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{aluno_id}/disable/"
    payload = {}
    response = requests.post(url, headers=HEADERS, json=payload)
    
    return response.status_code == 200

def associar_aluno_turma(student_id, school_class_id):
    """Associa um aluno a uma turma na API."""
    url = f"https://staging.lizeedu.com.br/api/v2/students/{student_id}/set_classes/"
    payload = {"school_classes": [str(school_class_id)]}
    response = requests.post(url, headers=HEADERS, json=payload)
    return response.status_code == 200

def definir_etapa_ensino(codigo_turma):
    if len(codigo_turma) < 3:
        return None
    terceiro_digito = codigo_turma[2]
    if codigo_turma.startswith("11"):
        return "Anos Iniciais" if terceiro_digito == "5" else "Anos Finais"
    elif codigo_turma.startswith("2"):
        return "Ensino Médio"
    return None

def inserir_aluno(nome, matricula, email):
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.post(url, headers=HEADERS, json=data)
    return response.status_code == 201

def processar_alunos():
    """Processa os alunos, comparando as tabelas persistidas com a API."""
    alunos_api = {a["enrollment_number"]: a for a in obter_alunos_api()}
    alunos_banco = obter_alunos_banco()
    turmas_api = obter_turmas_api()

    # Persistir turmas e alunos no banco de dados
    persistir_turmas_banco(turmas_api)
    
    # Processar alunos
    for unidade_cod, sit, matricula, nome, turma in alunos_banco:
        # Gerar e-mail do aluno
        email_gerado = f"{matricula}@alunos.smrede.com.br"
        
        turma = turma.strip()
        unidade_nome = CODIGO_PARA_UNIDADE.get(unidade_cod)

        if not unidade_nome:
            print(f"Código da unidade '{unidade_cod}' não encontrado.")
            continue
        
        etapa_ensino = definir_etapa_ensino(turma)
        if not etapa_ensino:
            print(f"Não foi possível determinar a etapa de ensino para a turma {turma}.")
            continue
        
        # Encontrar a coordenação certa
        coordination_ids = COORDINATION_IDS.get(unidade_nome)
        coordination_id = coordination_ids.get(etapa_ensino) if coordination_ids else None
        
        if not coordination_id:
            print(f"Coordination ID não encontrado para {etapa_ensino} na unidade {unidade_nome}.")
            continue
        
        aluno_api = alunos_api.get(matricula)

        if int(sit) in [2, 4]:
            if aluno_api:
                desativar_aluno(aluno_api["id"])
                print(f"Aluno {nome} removido da API.")
            continue
        
        if not aluno_api:
            # Inserir aluno e associar a turma
            if inserir_aluno(nome, matricula, email_gerado):
                print(f"Aluno {nome} inserido na API.")
                # Recarregar a lista de alunos da API
                alunos_api = {a["enrollment_number"]: a for a in obter_alunos_api()}
                aluno_api = alunos_api.get(matricula)

                if aluno_api:
                    # Buscar a turma correta e associar
                    turma_encontrada = next((t for t in turmas_api if t['coordination'] == coordination_id and t['name'] == turma), None)
                    if turma_encontrada and associar_aluno_turma(aluno_api["id"], turma_encontrada["id"]):
                        print(f"Aluno {nome} associado à turma {turma}.")
                    else:
                        print(f"Erro ao associar aluno {nome} à turma {turma}.")
                else:
                    print(f"Erro ao inserir aluno {nome} na API.")
        else:
            # Atualizar dados do aluno apenas se necessário
            if aluno_api["name"] != nome or aluno_api["email"] != email_gerado or aluno_api["classes"] != turma:
                atualizar_aluno(aluno_api["id"], nome, matricula, email_gerado)
                print(f"Aluno {nome} atualizado na API.")

            # Verificar se a turma está correta
            turma_encontrada = next((t for t in turmas_api if t['coordination'] == coordination_id and t['name'] == turma), None)
            if turma_encontrada:
                if turma_encontrada["id"] not in aluno_api.get("classes", []):
                    if associar_aluno_turma(aluno_api["id"], turma_encontrada["id"]):
                        print(f"Aluno {nome} associado à turma {turma}.")
                    else:
                        print(f"Erro ao associar aluno {nome} à turma {turma}.")
                else:
                    print(f"Aluno {nome} já está associado à turma {turma}.")
            else:
                print(f"Turma '{turma}' não encontrada.")

if __name__ == "__main__":
    processar_alunos()
