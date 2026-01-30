import requests
from constantes import HEADERS, ANO_LETIVO_ATUAL

# URL base do endpoint
url = f"https://app.lizeedu.com.br/api/v2/classes/?school_year={ANO_LETIVO_ATUAL}"

# Cabeçalhos com autenticação
headers = {
    "Authorization": "Token 443864674b4a856e86990a6c8b3241d3a08e7d8e",
    "accept": "application/json"
}

turmas = []

# Loop para percorrer todas as páginas
while url:
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        turmas.extend(data["results"])  # Adiciona os resultados da página atual à lista
        url = data.get("next")  # Atualiza a URL para a próxima página (se houver)
    else:
        print("Erro:", response.status_code, response.text)
        break  # Para a execução em caso de erro

# Exibir o total de turmas coletadas
print("Total de turmas coletadas:", len(turmas))

# Exibir os IDs das turmas
for turma in turmas:
    print(f"ID: {turma['id']} | Nome: {turma['name']} | Ano: {turma['school_year']} | Coordenação: {turma['coordination']}")
