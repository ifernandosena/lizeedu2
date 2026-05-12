
import psycopg2
from constantes import DB_CONFIG, ANO_LETIVO_ATUAL, TABELA_ALUNOS_GERAL

def check_stats():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Count in local cache
        cur.execute(f"SELECT COUNT(*) FROM alunos_lize WHERE ano_letivo = {ANO_LETIVO_ATUAL}")
        count_cache = cur.fetchone()[0]
        
        cur.execute(f"SELECT COUNT(*) FROM alunos_lize WHERE ano_letivo = {ANO_LETIVO_ATUAL} AND ativo = TRUE")
        count_cache_active = cur.fetchone()[0]

        # Count in source of truth (eligible students)
        cur.execute(f"SELECT COUNT(*) FROM {TABELA_ALUNOS_GERAL} WHERE (turma::NUMERIC >= 11500) AND (sit::NUMERIC NOT IN (2, 4))")
        count_source = cur.fetchone()[0]

        print(f"Year: {ANO_LETIVO_ATUAL}")
        print(f"Source Table: {TABELA_ALUNOS_GERAL}")
        print(f"Total in Source (Eligible): {count_source}")
        print(f"Total in Cache (alunos_lize): {count_cache}")
        print(f"Active in Cache: {count_cache_active}")
        
        # Check for students in cache (active) but not in source
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM alunos_lize al
            LEFT JOIN {TABELA_ALUNOS_GERAL} source ON al.matricula = source.matricula::text
            WHERE al.ano_letivo = {ANO_LETIVO_ATUAL} 
            AND al.ativo = TRUE
            AND (source.matricula IS NULL OR NOT ((source.turma::NUMERIC >= 11500) AND (source.sit::NUMERIC NOT IN (2, 4))))
        """)
        count_intruders = cur.fetchone()[0]
        print(f"Active in Cache but NOT in Source (Intruders): {count_intruders}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_stats()
