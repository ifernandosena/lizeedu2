import psycopg2
import sys
import os
sys.path.append(os.getcwd())
from constantes import DB_CONFIG

def monitor():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM alunos_lize WHERE ano_letivo = 2026")
    print(f"Alunos no cache: {cur.fetchone()[0]}")
    conn.close()

if __name__ == '__main__':
    monitor()
