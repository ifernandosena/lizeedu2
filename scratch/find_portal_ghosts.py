
import requests
import psycopg2
from constantes import HEADERS, DB_CONFIG, ANO_LETIVO_ATUAL, TABELA_ALUNOS_GERAL

def find_ghosts():
    try:
        # 1. Get all active students from API (regardless of year)
        # Note: This might be slow if there are many, but let's try to get a sample or first page.
        print(f"Buscando alunos ativos no portal (geral)...")
        url = "https://app.lizeedu.com.br/api/v2/students/?is_active=true&limit=100"
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code != 200:
            print(f"Erro na API: {response.status_code}")
            return
        
        data = response.json()
        active_students = data.get("results", [])
        total_active = data.get("count", 0)
        print(f"Total de ativos no portal: {total_active}")

        # 2. Get the list of matriculas from our source of truth (2026)
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(f"SELECT matricula::text FROM {TABELA_ALUNOS_GERAL} WHERE (turma::NUMERIC >= 11500) AND (sit::NUMERIC NOT IN (2, 4))")
        source_matriculas = {r[0].strip() for r in cur.fetchall()}
        cur.close()
        conn.close()

        print(f"Total na fonte de 2026: {len(source_matriculas)}")

        # 3. Compare
        ghosts = []
        for s in active_students:
            mat = str(s.get("enrollment_number", "")).strip()
            if mat not in source_matriculas:
                ghosts.append(s)
        
        print(f"Fantasmas encontrados na primeira página (100 registros): {len(ghosts)}")
        for g in ghosts[:10]:
            classes = [c.get("name") for c in g.get("classes", [])]
            print(f" - {g.get('name')} | Mat: {g.get('enrollment_number')} | Turmas: {classes}")

    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    find_ghosts()
