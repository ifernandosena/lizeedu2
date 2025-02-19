import requests

# URL base do endpoint
url = "https://staging.lizeedu.com.br/api/v2/classes/?school_year=2025"

# Cabeçalhos com autenticação
headers = {
    "Authorization": "Token f138893cee459fc57c263a02a6ac4451f991554f",
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
