import requests
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo config.env
load_dotenv("config.env")

# Configuração da API
token = os.getenv("API_TOKEN")
HEADERS = {
    "Authorization": f"{token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Função para associar um aluno a uma turma
def associar_aluno_turma(student_id, name, enrollment_number, email, school_classes):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{student_id}/set_classes/"

    # Dados a serem enviados no corpo da requisição
    payload = {
        "name": name,
        "enrollment_number": enrollment_number,
        "email": email,
        "school_classes": school_classes
    }

    response = requests.post(url, headers=HEADERS, json=payload)

    if response.status_code == 200:
        aluno = response.json()
        print("✅ Aluno associado à turma com sucesso!")
        print(f"📌 Nome: {aluno['name']} | ID: {aluno['id']} | Turma: {aluno['classes']}")
    else:
        print(f"❌ Erro ao associar aluno: {response.status_code} - {response.text}")

# Exemplo de uso
associar_aluno_turma(
    student_id="e7e00660-dd06-41cc-8e2f-7855268138d9",  # Substitua pelo ID real do aluno
    name="João",
    enrollment_number="999",
    email="999@smrede.com.br",
    school_classes = ["e327c068-1fd5-4275-8e71-574cf8184510"] ### id da turma
)
