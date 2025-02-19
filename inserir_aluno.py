import requests
import os
from dotenv import load_dotenv

load_dotenv("config.env")

# Carregando o token de autenticação da variável de ambiente
token = os.getenv("API_TOKEN")  # Verifique se a variável de ambiente está configurada corretamente

# Cabeçalhos para a requisição
HEADERS = {
    "Authorization": f"{token}",
    "Accept": "application/json"
}

# Função para inserir aluno
def inserir_aluno(nome, matricula, email):
    url = "https://staging.lizeedu.com.br/api/v2/students/"
    
    data = {
        "name": nome,
        "enrollment_number": matricula,
        "email": email,
    }
    
    response = requests.post(url, headers=HEADERS, json=data)

    if response.status_code == 201:
        print(f"✅ Aluno '{nome}' inserido com sucesso!")
    elif response.status_code == 403:
        print(f"❌ Erro de autenticação. Verifique o token: {response.text}")
    else:
        print(f"❌ Erro ao inserir aluno: {response.status_code} - {response.text}")

inserir_aluno("João", "999", "999@smrede.com.br")