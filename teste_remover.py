import requests
import os
from dotenv import load_dotenv
from collections import defaultdict
import time

load_dotenv("config.env")

token = os.getenv("API_TOKEN")

HEADERS = {
    "Authorization": f"{token}",
    "Accept": "application/json",
}

def obter_alunos_duplicados():
    url = "https://staging.lizeedu.com.br/api/v2/students/?q_duplicated=on&limit=100"
    alunos = []

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            try:
                data = response.json()
                alunos.extend(data["results"])
                url = data.get("next")
                print(f"Coletados {len(alunos)} alunos duplicados até agora...")
                time.sleep(1)  # Pequena pausa para evitar sobrecarga na API
            except requests.exceptions.JSONDecodeError:
                print("Erro ao decodificar JSON da API ao obter alunos duplicados.")
                break
        else:
            print("Erro ao obter alunos duplicados:", response.status_code, response.text)
            break
    
    print(f"Total de alunos duplicados encontrados: {len(alunos)}")
    return alunos

def desativar_aluno(id_aluno, nome_aluno):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/disable/"
    payload = {}
    response = requests.post(url, headers=HEADERS, json=payload)
    
    if response.status_code == 200:
        print(f"Aluno {nome_aluno} com id {id_aluno} desativado com sucesso!")
    else:
        print(f"Erro ao desativar aluno {nome_aluno}: {response.status_code}")
        try:
            print("Resposta da API:", response.json())
        except requests.exceptions.JSONDecodeError:
            print("Erro ao decodificar resposta JSON da API ao desativar aluno.")

def deletar_alunos():
    alunos = obter_alunos_duplicados()
    if not alunos:
        print("Nenhum aluno duplicado encontrado.")
        return
    
    matriculas = defaultdict(list)
    
    for aluno in alunos:
        matriculas[aluno["enrollment_number"]].append(aluno)
    
    for matricula, lista_alunos in matriculas.items():
        if len(lista_alunos) > 1:
            print(f"Matrícula {matricula} duplicada. Removendo alunos...")
            for aluno in lista_alunos[1:]:
                deletar_aluno(aluno["id"], aluno["name"])
    
    for aluno in alunos:
        if not aluno.get("classes"):
            print(f"Aluno {aluno['name']} sem turma. Desativando...")
            desativar_aluno(aluno["id"], aluno["name"])

def deletar_aluno(id_aluno, nome_aluno, tentativas=3):
    url = f"https://staging.lizeedu.com.br/api/v2/students/{id_aluno}/"
    for tentativa in range(tentativas):
        response = requests.delete(url, headers=HEADERS)
        
        if response.status_code == 204:
            print(f"Aluno {nome_aluno} com id {id_aluno} deletado com sucesso!")
            return
        elif response.status_code in [500, 524]:
            print(f"Erro {response.status_code} ao deletar {nome_aluno}, tentativa {tentativa + 1} de {tentativas}. Retentando...")
            time.sleep(2)
        else:
            print(f"Erro ao deletar aluno {nome_aluno}: {response.status_code}")
            try:
                print("Resposta da API:", response.json())
            except requests.exceptions.JSONDecodeError:
                print("Erro ao decodificar resposta JSON da API ao deletar aluno.")
            return
    print(f"Falha ao deletar {nome_aluno} após {tentativas} tentativas.")

deletar_alunos()
