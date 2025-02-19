import requests
import os
from dotenv import load_dotenv

load_dotenv("config.env")

# Carregando o token de autenticação da variável de ambiente
token = os.getenv("API_TOKEN")

# Cabeçalhos para a requisição
HEADERS = {
    "Authorization": f"{token}",
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Função para atualizar aluno
def atualizar_aluno(aluno_id, nome, matricula, email):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{aluno_id}/"
    
    data = {
        "name": nome,
        "enrollment_number": matricula,
        "email": email,
    }
    
    response = requests.put(url, headers=HEADERS, json=data)
    
    if response.status_code == 200:
        print(f"✅ Aluno '{nome}' atualizado com sucesso!")
        return response.json()
    elif response.status_code == 403:
        print(f"❌ Erro de autenticação. Verifique o token: {response.text}")
    elif response.status_code == 400:
        print(f"❌ Erro na requisição: {response.text}")
    else:
        print(f"❌ Erro ao atualizar aluno: {response.status_code} - {response.text}")
    
    return None

# Exemplo de uso
atualizar_aluno(
    aluno_id="e7e00660-dd06-41cc-8e2f-7855268138d9",
    nome="João",
    matricula="9991",
    email="999@smrede.com.br",
)
