import psycopg2
import sys
import os
sys.path.append(os.getcwd())
from constantes import DB_CONFIG

def check_class_id():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("SELECT id, nome FROM turmas_lize WHERE nome IN ('27312', '22212')")
    print("Mapeamento de Turmas:")
    for id_lize, nome in cur.fetchall():
        print(f"Turma {nome} -> ID Lize: {id_lize}")
    
    conn.close()

if __name__ == '__main__':
    check_class_id()
