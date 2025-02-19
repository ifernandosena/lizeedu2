import requests
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv("config.env")

# Carregando o token de autenticação
token = os.getenv("API_TOKEN")

# Cabeçalhos para a requisição (sem o X-CSRFTOKEN, se necessário)
HEADERS = {
    "Authorization": f"{token}",
    "Accept": "application/json",
}

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
        print(f"ID: {turma['id']} | Nome: {turma['name']} | Ano: {turma['school_year']} | Coordenação: {turma['coordination']} | Matrícula: {turma['enrollment_number']}")

    return [{
        "id": t["id"],
        "coordination": t["coordination"],
        "name": t["name"],
        "enrollment_number": t["enrollment_number"] 
    } for t in turmas]

# Função para deletar aluno
def deletar_aluno(id_aluno):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/"
    
    response = requests.delete(url, headers=HEADERS)
    
    if response.status_code == 204:
        print(f"Aluno {'name'} com id {id_aluno} deletado com sucesso!")
    else:
        print(f"Erro ao deletar aluno: {response.status_code}")
        print(response.json())

deletar_aluno("ea876ab7-3ab5-4eec-9833-0fae290e7a87")        