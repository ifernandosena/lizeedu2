import psycopg2
import sys
import os
sys.path.append(os.getcwd())
from constantes import DB_CONFIG

def check_student(name):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    print(f"Buscando por: {name}")
    
    # 2025
    try:
        cur.execute("SELECT matricula, nome, turma, sit FROM alunos_25_geral WHERE nome ILIKE %s", (f"%{name}%",))
        print(f"2025: {cur.fetchall()}")
    except: pass
    
    # 2026
    try:
        cur.execute("SELECT matricula, nome, turma, sit FROM alunos_26_geral WHERE nome ILIKE %s", (f"%{name}%",))
        print(f"2026: {cur.fetchall()}")
    except: pass
    
    # Cache Lize (alunos_lize)
    cur.execute("SELECT matricula, nome, ativo, ano_letivo, classes FROM alunos_lize WHERE nome ILIKE %s", (f"%{name}%",))
    print(f"Cache Lize: {cur.fetchall()}")
    
    conn.close()

if __name__ == '__main__':
    check_student(sys.argv[1])
