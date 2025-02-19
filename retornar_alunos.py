import requests
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo config.env
load_dotenv("config.env")

# Configuração da API
token = os.getenv("API_TOKEN")
HEADERS = {"Authorization": f"{token}", "Accept": "application/json"}

# Função para obter todos os alunos
def obter_todos_alunos():
    url = "https://staging.lizeedu.com.br/api/v2/students/"

    # Parâmetros de consulta
    params = {
    }

    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code == 200:
        data = response.json()
        alunos = data.get("results", [])

        # Imprimir todos os alunos com seus IDs e nomes
        if alunos:
            print("📋 Lista de alunos encontrados:")
            for aluno in alunos:
                print(f"📌 Nome: {aluno['name']} | ID: {aluno['id']} | Matrícula: {aluno['enrollment_number']}")
        else:
            print("❌ Nenhum aluno encontrado.")
    else:
        print(f"❌ Erro ao buscar alunos: {response.status_code} - {response.text}")

# Chamada para obter e imprimir todos os alunos
obter_todos_alunos()
