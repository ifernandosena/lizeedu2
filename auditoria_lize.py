import time
import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from constantes import HEADERS, DB_CONFIG, ANO_LETIVO_ATUAL

# Configuracao de log tabular
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def gerar_hash(nome, situacao_ativo, id_turma):
    """Gera o hash com a mesma regra do script principal"""
    return hashlib.md5(f"{nome.strip()}|{situacao_ativo}|{id_turma}".encode('utf-8')).hexdigest()

def faxina_portal_completa():
    """
    Busca TODOS os alunos ativos no portal Lize (independente do ano)
    e popula o cache local. Isso permite que o envio_lize.py identifique
    quem sao os 'intrusos' que precisam ser inativados.
    """
    logging.info("INICIANDO SCAN COMPLETO DO PORTAL (IDENTIFICACAO DE INTRUSOS)...")
    
    # 1. Pegar o total de registros ativos
    try:
        r_init = requests.get(f"https://app.lizeedu.com.br/api/v2/students/?is_active=true&limit=1", headers=HEADERS, timeout=15)
        if r_init.status_code != 200:
            logging.error(f"Erro ao consultar total de ativos: {r_init.status_code}")
            return
    except Exception as e:
        logging.error(f"Erro de conexao: {e}")
        return

    total_records = r_init.json().get("count", 0)
    pages = (total_records // 50) + 1
    
    logging.info(f"Baixando {total_records} alunos ativos via Turbo Mode ({pages} paginas)...")
    
    alunos_api = []
    def fetch_page(off):
        url = f"https://app.lizeedu.com.br/api/v2/students/?is_active=true&limit=50&offset={off}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            return resp.json().get("results", []) if resp.status_code == 200 else []
        except Exception: return []

    offsets = [i * 50 for i in range(pages)]
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(fetch_page, off) for off in offsets]
        for future in as_completed(futures):
            alunos_pg = future.result()
            alunos_api.extend(alunos_pg)
            if len(alunos_api) % 1000 <= 50:
                logging.info(f"   -> {len(alunos_api)}/{total_records} baixados...")

    logging.info(f"Download concluido: {len(alunos_api)} alunos ativos encontrados.")

    # 2. Preparar os dados para o cache local
    upsert_cache = []
    for aluno in alunos_api:
        id_api = aluno.get("id")
        nome = str(aluno.get("name", "")).strip()
        mat = str(aluno.get("enrollment_number", "")).strip()
        email = aluno.get("email")
        ativo = aluno.get("is_active", True)
        
        if not mat or mat.lower() == "none":
            continue
            
        classes_raw = aluno.get("classes", [])
        classes_ano_atual = [c for c in classes_raw if c.get("school_year") == ANO_LETIVO_ATUAL]
        
        # Se tem turma em 2026, pegamos ela. Se nao, marcamos como SEM_TURMA para o cache identificar como intruso
        id_turma_alvo = str(classes_ano_atual[0]["id"]) if classes_ano_atual else "SEM_TURMA"
        
        hash_atual = gerar_hash(nome, ativo, id_turma_alvo)
        classes_ids = [str(c["id"]) for c in classes_ano_atual]
        
        upsert_cache.append((
            id_api, nome, mat, email, classes_ids, ativo, ANO_LETIVO_ATUAL, hash_atual
        ))

    # 3. Salvar no banco local
    if upsert_cache:
        logging.info(f"Salvando {len(upsert_cache)} registros no cache local...")
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    # Limpa o cache do ano atual antes de inserir a nova visao completa
                    cur.execute(f"DELETE FROM alunos_lize WHERE ano_letivo = {ANO_LETIVO_ATUAL}")
                    
                    sql_upsert = """
                        INSERT INTO alunos_lize (id, nome, matricula, email, classes, ativo, ano_letivo, hash_estado)
                        VALUES %s ON CONFLICT (matricula, ano_letivo) 
                        DO UPDATE SET 
                            id=EXCLUDED.id, nome=EXCLUDED.nome, ativo=EXCLUDED.ativo, 
                            classes=EXCLUDED.classes, hash_estado=EXCLUDED.hash_estado
                    """
                    execute_values(cur, sql_upsert, upsert_cache)
            logging.info("Concluido: Cache local atualizado com todos os ativos do portal.")
            logging.info("Agora rode o 'envio_lize.py' para processar as inativacoes.")
        except Exception as e:
            logging.error(f"Erro ao persistir no banco: {e}")

if __name__ == "__main__":
    faxina_portal_completa()