import psycopg2
import requests
from constantes import HEADERS, DB_CONFIG

def desativar_alunos_por_matricula():
    try:
        conexao = psycopg2.connect(**DB_CONFIG)
        cursor = conexao.cursor()

        # Selecionar alunos cuja matrícula tem menos de 9 dígitos
        cursor.execute("""
        SELECT id, nome, matricula FROM alunos_lize
        WHERE LENGTH(matricula) < 9 AND ativo = TRUE;
        """)
        alunos = cursor.fetchall()

        for id_aluno, nome_aluno, matricula in alunos:
            if desativar_aluno(id_aluno, nome_aluno):
                # Atualizar no banco de dados local
                cursor.execute("""
                UPDATE alunos_lize SET ativo = FALSE WHERE id = %s;
                """, (id_aluno,))
                print(f"✅ Aluno {nome_aluno} ({matricula}) desativado.")

        conexao.commit()
        cursor.close()
        conexao.close()
    except Exception as e:
        print(f"❌ Erro ao desativar alunos: {e}")

def desativar_aluno(id_aluno, nome_aluno):
    url = f"https://app.lizeedu.com.br/api/v2/students/{id_aluno}/disable/"
    response = requests.post(url, headers=HEADERS, json={})
    if response.status_code in [200, 204]:
        return True
    else:
        print(f"❌ Erro ao desativar aluno {nome_aluno}: {response.status_code}")
        return False

if __name__ == "__main__":
    desativar_alunos_por_matricula()
