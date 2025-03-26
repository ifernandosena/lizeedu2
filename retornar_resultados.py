import requests 
import os
from dotenv import load_dotenv

load_dotenv("config.env")

# Carregando o token de autenticação da variável de ambiente
token = os.getenv("API_TOKEN")

# Verifique se o token está sendo carregado corretamente
print(f"Token: {token}")

# Cabeçalhos para a requisição
HEADERS = {
    "Authorization": f"{token}",
    "Accept": "application/json"
}

# Função para buscar resultados de alunos
def buscar_resultados_alunos(limit=None, offset=None, ordering=None, search=None):
    url = "https://app.lizeedu.com.br/api/v2/application-students-results/"
    
    params = {}
    
    # Adicionando parâmetros caso existam
    if limit:
        params["limit"] = limit
    if offset:
        params["offset"] = offset
    if ordering:
        params["ordering"] = ordering
    if search:
        params["search"] = search
    
    response = requests.get(url, headers=HEADERS, params=params)

    # Exibe a resposta completa da API para depuração
    print("Resposta da API:", response.json())

    if response.status_code == 200:
        data = response.json()
        print(f"✅ Total de resultados encontrados: {data['count']}")
        if data['count'] > 0:
            print(data['results'])  # Exibe os resultados no console
        else:
            print("❌ Nenhum resultado encontrado.")
    elif response.status_code == 403:
        print(f"❌ Erro de autenticação. Verifique o token: {response.text}")
    else:
        print(f"❌ Erro ao buscar resultados: {response.status_code} - {response.text}")

# Exemplo de uso da função sem filtros
buscar_resultados_alunos()