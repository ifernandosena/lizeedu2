import psycopg2
import sys
import os
sys.path.append(os.getcwd())
from constantes import DB_CONFIG

def check_distribution():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT SUBSTRING(turma, 1, 2) as serie, COUNT(*) 
        FROM alunos_26_geral 
        WHERE (turma::NUMERIC >= 11500) AND (sit::NUMERIC NOT IN (2, 4))
        GROUP BY serie 
        ORDER BY serie
    """)
    print("Distribuição na Fonte (2026):")
    for serie, count in cur.fetchall():
        print(f"Série {serie}: {count} alunos")
        
    cur.execute("""
        SELECT matricula, nome, turma 
        FROM alunos_26_geral 
        WHERE (turma::NUMERIC >= 11500) AND (sit::NUMERIC NOT IN (2, 4)) AND (turma LIKE '27%')
        LIMIT 5
    """)
    print("\nAmostra 3ª Série (2026):")
    for mat, nome, turma in cur.fetchall():
        print(f"Mat: {mat} | Nome: {nome} | Turma: {turma}")
    
    conn.close()

if __name__ == '__main__':
    check_distribution()
