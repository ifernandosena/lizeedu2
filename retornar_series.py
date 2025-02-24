import requests, os
from dotenv import load_dotenv

load_dotenv("config.env")

# Configuração da API
token = os.getenv("API_TOKEN")
HEADERS = {"Authorization": f"{token}", "Accept": "application/json"}

# Função para obter todas as séries
def obter_todas_series():
    url = "https://app.lizeedu.com.br/api/v2/series/"

    # Parâmetros de consulta
    params = {}

    response = requests.get(url, headers=HEADERS, params=params)

    if response.status_code == 200:
        data = response.json()
        series = data.get("results", [])

        # Imprimir todas as séries com seus IDs e níveis
        if series:
            print("Lista de séries encontradas:")
            for serie in series:
                print(f"Nome: {serie['name']} | ID: {serie['id']} | Nível: {serie['level']}")
        else:
            print("❌ Nenhuma série encontrada.")
    else:
        print(f"❌ Erro ao buscar séries: {response.status_code} - {response.text}")

# Chamada para obter e imprimir todas as séries
obter_todas_series()
