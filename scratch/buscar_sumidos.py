import psycopg2
from constantes import DB_CONFIG

def buscar_aluno(matricula):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'alunos_%_geral'")
    tables = [r[0] for r in cur.fetchall()]
    
    print(f"Buscando matricula {matricula} em {len(tables)} tabelas...")
    
    encontrado = False
    for t in tables:
        try:
            cur.execute(f"SELECT matricula, nome, turma FROM {t} WHERE matricula = %s", (matricula,))
            res = cur.fetchone()
            if res:
                print(f"Encontrado na tabela {t}: {res}")
                encontrado = True
        except Exception as e:
            pass
            
    if not encontrado:
        print("Matricula nao encontrada em nenhuma tabela de alunos.")
    
    conn.close()

if __name__ == "__main__":
    buscar_aluno('162400530') # Arthur de Souza Bezerra
    print("-" * 30)
    buscar_aluno('021701578') # Brayan Oliveira Venancio
