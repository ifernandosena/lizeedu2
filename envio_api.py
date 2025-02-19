import requests, psycopg2
from datetime import datetime
from constantes import HEADERS, DB_CONFIG, CODIGO_PARA_UNIDADE, COORDINATION_IDS

def obter_alunos_api():
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    response = requests.get(url, headers=HEADERS)
    return response.json().get("results", []) if response.status_code == 200 else []

def obter_alunos_banco():
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
            print("Erro:", response.status_code, response.text)
            break

    print("Total de turmas coletadas:", len(turmas))
    for turma in turmas:
        print(f"ID: {turma['id']} | Nome: {turma['name']} | Ano: {turma['school_year']} | Coordenação: {turma['coordination']}")

    return [{
        "id": t["id"],
        "coordination": t["coordination"],
        "name": t["name"]
    } for t in turmas]


def associar_aluno_turma(student_id, school_class_id):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{student_id}/set_classes/"
    payload = {
        "school_classes": [str(school_class_id)]
    }
    response = requests.post(url, headers=HEADERS, json=payload)
    return response.status_code == 200

def atualizar_aluno(aluno_id, nome, matricula, email):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{aluno_id}/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.put(url, headers=HEADERS, json=data)
    return response.status_code == 200

def desativar_aluno(id_aluno, nome_aluno):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/disable/"
    payload = {}
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code == 200:
        print(f"Aluno {nome_aluno} com id {id_aluno} desativado com sucesso!")
    else:
        print(f"Erro ao desativar aluno {nome_aluno}: {response.status_code}")
        try:
            print("Resposta da API:", response.json())
        except requests.exceptions.JSONDecodeError:
            print("Erro ao decodificar resposta JSON da API ao desativar aluno.")

def inserir_aluno(nome, matricula, email):
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    data = {"name": nome, "enrollment_number": matricula, "email": email}
    response = requests.post(url, headers=HEADERS, json=data)
    return response.status_code == 201

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
    alunos_api = {a["enrollment_number"]: a for a in obter_alunos_api()}
    alunos_banco = obter_alunos_banco()
    turmas_dict = obter_turmas_api()

    for unidade_cod, sit, matricula, nome, turma in alunos_banco:  # Desempacota 5 valores
        # Gerando o e-mail a partir da matrícula
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
        
        # Aqui, usamos o nome da unidade (unidade_nome) para pegar a coordenação certa
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
                # Obter o aluno recém-inserido
                alunos_api = {a["enrollment_number"]: a for a in obter_alunos_api()}  # Recarregar a lista de alunos
                aluno_api = alunos_api.get(matricula)  # Buscar o aluno novamente após a inserção

                if not aluno_api:
                    print(f"Erro ao inserir aluno {nome} na API.")
                    continue  # Pular para o próximo aluno se não for possível obter o aluno

                # Agora, encontramos a turma correta usando a coordenação do aluno
                turma_encontrada = None
                print(turmas_dict)
                for turma_api in turmas_dict:
                    # Certifique-se de que turma_api é um dicionário e tem as chaves esperadas
                    if isinstance(turma_api, dict) and 'coordination' in turma_api and 'name' in turma_api:
                        if turma_api['coordination'] == coordination_id and turma_api['name'] == turma:
                            turma_encontrada = turma_api
                            break
                
                if turma_encontrada:
                    if associar_aluno_turma(aluno_api["id"], turma_encontrada["id"]):
                        print(f"Aluno {nome} associado à turma {turma} ({etapa_ensino}) na unidade {unidade_nome}. ID da turma: {turma_encontrada['id']}.")
                    else:
                        print(f"Erro ao associar aluno {nome} à turma {turma}.")
                else:
                    print(f"Turma '{turma}' não encontrada na coordenação {coordination_id}.")
        else:
            # O aluno já está inserido, vamos verificar se há necessidade de atualização
            if aluno_api["name"] != nome or aluno_api["email"] != email_gerado:
                atualizar_aluno(aluno_api["id"], nome, matricula, email_gerado)
                print(f"Aluno {nome} atualizado na API.")
            
            # Verifique se a turma está na API com a coordenação correta
            turma_encontrada = None
            for turma_api in turmas_dict:
                # if isinstance(turma_api, dict) and 'coordination' in turma_api and 'name' in turma_api:
                if turma_api['coordination'] == coordination_id and turma_api['name'] == turma:
                    turma_encontrada = turma_api
                    break
            
            if turma_encontrada:
                if turma_encontrada["id"] not in aluno_api.get("classes", []):
                    if associar_aluno_turma(aluno_api["id"], turma_encontrada["id"]):
                        print(f"Aluno {nome} associado à turma {turma} ({etapa_ensino}) na unidade {unidade_nome}. ID da turma: {turma_encontrada['id']}.")
                    else:
                        print(f"Erro ao associar aluno {nome} à turma {turma}.")
                else:
                    print(f"Aluno {nome} já está associado à turma {turma}.")
            else:
                print(f"Turma '{turma}' não encontrada para a coordenação {coordination_id}.")

if __name__ == "__main__":
    processar_alunos()