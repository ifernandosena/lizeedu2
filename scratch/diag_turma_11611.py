import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
import psycopg2
from constantes import DB_CONFIG, HEADERS, ANO_LETIVO_ATUAL, TABELA_ALUNOS_GERAL

def diag():
    print(f"--- Diagnóstico Turma 11611 ({ANO_LETIVO_ATUAL}) ---")
    
    try:
        print("Buscando no Postgres...")
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                query = f"SELECT matricula, nome, sit FROM {TABELA_ALUNOS_GERAL} WHERE turma = '11611' AND sit::NUMERIC NOT IN (2, 4)"
                cur.execute(query)
                source_students = cur.fetchall()
    except Exception as e:
        print(f"Erro ao acessar Postgres: {e}")
        return

    print(f"Fonte (Postgres): {len(source_students)} alunos encontrados.")
    source_map = {str(s[0]).strip(): str(s[1]).strip() for s in source_students}

    # 2. Buscar ID da Turma 11611 na Lize
    turma_id = None
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM turmas_lize WHERE nome = '11611' AND school_year = %s", (ANO_LETIVO_ATUAL,))
                row = cur.fetchone()
                if row:
                    turma_id = row[0]
    except Exception as e:
        print(f"Erro ao buscar ID da turma: {e}")

    if not turma_id:
        print("Erro: Turma 11611 não encontrada no banco local (turmas_lize).")
        # Tenta buscar via API
        url_t = f"https://app.lizeedu.com.br/api/v2/classes/?name=11611&school_year={ANO_LETIVO_ATUAL}"
        r_t = requests.get(url_t, headers=HEADERS)
        if r_t.status_code == 200:
            results = r_t.json().get("results", [])
            if results:
                turma_id = results[0]["id"]
                print(f"Encontrada via API: {turma_id}")
    
    if not turma_id:
        print("Falha ao localizar ID da turma.")
        return

    # 3. Buscar alunos na Lize para essa turma
    print(f"Buscando alunos na Lize para a turma {turma_id}...")
    lize_students = []
    url_l = f"https://app.lizeedu.com.br/api/v2/students/?classes={turma_id}&is_active=true"
    while url_l:
        print(f"  -> Lendo página: {url_l}")
        r_l = requests.get(url_l, headers=HEADERS, timeout=15)
        if r_l.status_code == 200:
            data = r_l.json()
            lize_students.extend(data.get("results", []))
            url_l = data.get("next")
        else:
            print(f"Erro API Lize: {r_l.status_code}")
            break

    print(f"Lize API: {len(lize_students)} alunos ativos encontrados na turma {turma_id}.")
    lize_map = {str(s.get("enrollment_number", "")).strip(): str(s.get("name", "")).strip() for s in lize_students}
    lize_ids = {str(s.get("enrollment_number", "")).strip(): s.get("id") for s in lize_students}

    # 4. Comparação
    print("\n--- Alunos na Fonte mas NÃO na Lize ---")
    missing_in_lize = []
    for mat, nome in source_map.items():
        if mat not in lize_map:
            print(f"Mat: {mat} | Nome: {nome}")
            missing_in_lize.append(mat)

    print("\n--- Alunos na Lize mas NÃO na Fonte (Fantasmas) ---")
    extra_in_lize = []
    for mat, nome in lize_map.items():
        if mat not in source_map:
            print(f"Mat: {mat} | Nome: {nome}")
            extra_in_lize.append(mat)

    print("\n--- Alunos com nomes divergentes ---")
    divergent_names = []
    for mat, nome_s in source_map.items():
        if mat in lize_map:
            nome_l = lize_map[mat]
            if nome_s != nome_l:
                print(f"Mat: {mat} | Fonte: {nome_s} | Lize: {nome_l}")
                divergent_names.append(mat)

if __name__ == "__main__":
    diag()
