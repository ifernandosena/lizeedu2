import requests, os
from dotenv import load_dotenv

load_dotenv("config.env")

# Configuração da API
token = os.getenv("API_TOKEN")
HEADERS = {"Authorization": f"{token}", "Accept": "application/json"}

# Função para obter todas as coordenações
def obter_todas_coordenações():
    url = "https://app.lizeedu.com.br/api/v2/coordinations/"

    # Parâmetros de consulta
    params = {}

    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code == 200:
        data = response.json()
        coordenações = data.get("results", [])

        # Imprimir todas as coordenações com seus IDs
        if coordenações:
            print("Lista de coordenações encontradas:")
            for coordenacao in coordenações:
                print(f"Nome: {coordenacao['name']} | ID: {coordenacao['id']} | Unidade: {coordenacao['unit']}")
        else:
            print("❌ Nenhuma coordenação encontrada.")
    else:
        print(f"❌ Erro ao buscar coordenações: {response.status_code} - {response.text}")

# Chamada para obter e imprimir todas as coordenações
obter_todas_coordenações()
