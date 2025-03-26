import requests
from collections import defaultdict
from constantes import HEADERS

API_BASE_URL = "https://staging.lizeedu.com.br/api/v2/students/"

def obter_alunos_api():
    """Busca todos os alunos diretamente da API e retorna uma lista."""
    url = API_BASE_URL
    alunos = []
    
    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            alunos.extend(data.get("results", []))  # Adiciona os alunos da página atual
            url = data.get("next")  # Obtém o link para a próxima página
        else:
            print(f"❌ Erro ao obter alunos da API: {response.status_code}")
            break

    print(f"✅ Total de alunos obtidos: {len(alunos)}")
    return alunos

def desativar_alunos_duplicados():
    """Desativa alunos com matrícula duplicada na API que não estejam em nenhuma turma."""
    alunos_api = obter_alunos_api()
    matriculas_dict = defaultdict(list)

    # Organiza os alunos por matrícula
    for aluno in alunos_api:
        matriculas_dict[aluno["enrollment_number"]].append(aluno)

    # Filtra apenas matrículas duplicadas
    matriculas_duplicadas = {mat: alunos for mat, alunos in matriculas_dict.items() if len(alunos) > 1}

    print(f"🔍 Encontradas {len(matriculas_duplicadas)} matrículas duplicadas.")

    for matricula, alunos in matriculas_duplicadas.items():
        alunos_sem_turma = [a for a in alunos if not a["classes"]]  # Filtra alunos sem turma

        for aluno in alunos_sem_turma:
            id_aluno = aluno["id"]
            nome_aluno = aluno["name"]
            if desativar_aluno(id_aluno, nome_aluno):
                print(f"✅ Aluno {nome_aluno} ({matricula}) desativado.")

def desativar_aluno(id_aluno, nome_aluno):
    """Chama a API para desativar o aluno pelo ID."""
    url = f"{API_BASE_URL}{id_aluno}/disable/"
    response = requests.post(url, headers=HEADERS, json={})

    if response.status_code in [200, 204]:
        return True
    else:
        print(f"❌ Erro ao desativar aluno {nome_aluno}: {response.status_code}")
        return False

if __name__ == "__main__":
    desativar_alunos_duplicados()
