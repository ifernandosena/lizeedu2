import requests
from constantes import HEADERS, DB_CONFIG, ANO_LETIVO_ATUAL
import psycopg2

def faxina_lize():
    # 1. Pegar IDs das matrículas válidas da sua VIEW de 2026
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT matricula FROM public.alunos_26_geral WHERE turma::NUMERIC >= 11500")
            matriculas_validas = set(str(row[0]).strip() for row in cur.fetchall())
    
    print(f"✅ Encontradas {len(matriculas_validas)} matrículas válidas no Monitora para 2026.")

    # 2. Buscar alunos na API da Lize
    url = f"https://app.lizeedu.com.br/api/v2/students/?school_year={ANO_LETIVO_ATUAL}"
    count_removidos = 0

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200: break
        
        data = response.json()
        for aluno in data.get("results", []):
            mat = str(aluno.get("enrollment_number")).strip()
            aluno_id = aluno.get("id")
            
            # 3. Se o aluno está na Lize para 2026 mas não deveria estar
            if mat not in matriculas_validas:
                # Vamos desativar (mais seguro que deletar)
                del_url = f"https://app.lizeedu.com.br/api/v2/students/{aluno_id}/disable/"
                res_del = requests.post(del_url, headers=HEADERS)
                if res_del.status_code in [200, 204]:
                    print(f"🚫 Aluno extra {aluno.get('name')} ({mat}) desativado da Lize.")
                    count_removidos += 1
        
        url = data.get("next")

    print(f"🏁 Faxina concluída. {count_removidos} alunos intrusos foram removidos.")

if __name__ == "__main__":
    faxina_lize()