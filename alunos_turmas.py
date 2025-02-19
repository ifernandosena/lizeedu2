import requests
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo config.env
load_dotenv("config.env")

# Configuração da API
token = os.getenv("API_TOKEN")
HEADERS = {"Authorization": f"{token}", "Accept": "application/json"}

# Função para obter todas as turmas
def obter_todas_turmas():
    url = "https://staging.lizeedu.com.br/api/v2/classes/?school_year=2025"
    turmas = []

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            turmas.extend(data["results"])  # Adiciona os resultados da página atual à lista
            url = data.get("next")  # Atualiza a URL para a próxima página (se houver)
        else:
            print("Erro ao buscar turmas:", response.status_code, response.text)
            break  # Para a execução em caso de erro

    # Exibir o total de turmas coletadas
    print(f"Total de turmas coletadas: {len(turmas)}")
    return turmas

# Função para obter todos os alunos
def obter_todos_alunos():
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    alunos = []

    # Loop para percorrer todas as páginas de alunos
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            alunos.extend(data["results"])  # Adiciona os resultados da página atual à lista
            url = data.get("next")  # Atualiza a URL para a próxima página (se houver)
        else:
            print(f"❌ Erro ao buscar alunos: {response.status_code} - {response.text}")
            break  # Para a execução em caso de erro

    # Imprimir todos os alunos com seus IDs e nomes
    if alunos:
        print("📋 Lista de alunos encontrados:")
        for aluno in alunos:
            print(f"📌 Nome: {aluno['name']} | ID: {aluno['id']} | Matrícula: {aluno['enrollment_number']}")
    else:
        print("❌ Nenhum aluno encontrado.")

# Chamada para obter turmas e alunos
turmas = obter_todas_turmas()
obter_todos_alunos()
