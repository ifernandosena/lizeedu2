import time
import requests
import psycopg2
from psycopg2.extras import execute_values
import logging
import hashlib
from constantes import HEADERS, DB_CONFIG, ANO_LETIVO_ATUAL

# Configuração de log tabular
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

def gerar_hash(nome, situacao_ativo, id_turma):
    """Gera o hash com a mesma regra do script principal"""
    return hashlib.md5(f"{nome.strip()}|{situacao_ativo}|{id_turma}".encode('utf-8')).hexdigest()

def reconstruir_baseline():
    logging.info("🧹 INICIANDO AUDITORIA E RECONSTRUÇÃO DA BASELINE DA LIZE...")
    
    # 1. Obter todas as turmas da Lize para validar
    logging.info("📥 Baixando Turmas da Lize...")
    url_turmas = f"https://app.lizeedu.com.br/api/v2/classes/?school_year={ANO_LETIVO_ATUAL}"
    try:
        r_turmas = requests.get(url_turmas, headers=HEADERS, timeout=10)
        if r_turmas.status_code != 200:
            logging.error(f"❌ Erro ao baixar turmas: Status {r_turmas.status_code}. Abortando.")
            return
    except Exception as e:
        logging.error(f"❌ Erro de conexão ao baixar turmas: {e}. Abortando.")
        return
    
    # 2. Obter TODOS os alunos da Lize (Ativos e Inativos) via Paginação com Retry (Resiliência)
    url_alunos = f"https://app.lizeedu.com.br/api/v2/students/?school_year={ANO_LETIVO_ATUAL}"
    alunos_api = []
    pagina = 1
    max_tentativas = 3
    
    while url_alunos:
        sucesso_na_pagina = False
        
        for tentativa in range(max_tentativas):
            logging.info(f"📡 Baixando alunos da API Lize (Página {pagina})...")
            try:
                response = requests.get(url_alunos, headers=HEADERS, timeout=15)
                
                if response.status_code == 200:
                    data = response.json()
                    alunos_api.extend(data.get("results", []))
                    url_alunos = data.get("next")
                    pagina += 1
                    sucesso_na_pagina = True
                    break # Sai do loop de tentativas e vai pra próxima página
                
                logging.warning(f"⚠️ Servidor da Lize engasgou (Erro {response.status_code}). Tentativa {tentativa + 1}/{max_tentativas}...")
                time.sleep(3 * (tentativa + 1)) # Backoff: Espera 3s, depois 6s, depois 9s...
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"⚠️ Falha de rede: {e}. Tentativa {tentativa + 1}/{max_tentativas}...")
                time.sleep(3 * (tentativa + 1))
        
        if not sucesso_na_pagina:
            logging.error("❌ FALHA CRÍTICA: A API da Lize caiu definitivamente. Abortando a auditoria para não corromper o banco local.")
            return # Aborta a execução inteira do script. NÃO salva dados parciais.

    logging.info(f"✅ Download concluído: {len(alunos_api)} alunos encontrados na API da Lize.")

    # 3. Preparar os dados para reconstruir o banco local
    upsert_banco_local = []
    
    for aluno in alunos_api:
        id_api = aluno.get("id")
        nome = str(aluno.get("name", "")).strip()
        mat = str(aluno.get("enrollment_number", "")).strip()
        email = aluno.get("email")
        ativo = aluno.get("is_active", False)
        
        # Prevenção extra: ignora "alunos" criados manualmente sem matrícula na plataforma
        if not mat or mat.lower() == "none":
            continue
        
        # A API retorna as turmas do aluno. Vamos pegar a primeira para o hash (conforme regra 1:1)
        classes_raw = aluno.get("classes", [])
        id_turma_alvo = str(classes_raw[0]["id"]) if classes_raw else "SEM_TURMA"
        
        # Gera o hash refletindo EXATAMENTE o que está na Lize hoje
        hash_atual = gerar_hash(nome, ativo, id_turma_alvo)
        classes_ids = [str(c["id"]) for c in classes_raw]
        
        upsert_banco_local.append((
            id_api, nome, mat, email, classes_ids, ativo, ANO_LETIVO_ATUAL, hash_atual
        ))

    # 4. Sobrescrever o banco local (alunos_lize)
    if upsert_banco_local:
        logging.info(f"💾 Salvando {len(upsert_banco_local)} alunos no banco de dados local...")
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    sql_upsert = """
                        INSERT INTO alunos_lize (id, nome, matricula, email, classes, ativo, ano_letivo, hash_estado)
                        VALUES %s ON CONFLICT (matricula, ano_letivo) 
                        DO UPDATE SET 
                            id=EXCLUDED.id, nome=EXCLUDED.nome, ativo=EXCLUDED.ativo, 
                            classes=EXCLUDED.classes, hash_estado=EXCLUDED.hash_estado
                    """
                    execute_values(cur, sql_upsert, upsert_banco_local)
            logging.info("🏁 Baseline reconstruída com sucesso. O banco local agora é um espelho perfeito da API Lize.")
        except Exception as e:
            logging.error(f"❌ Erro fatal ao persistir no banco de dados local: {e}")

if __name__ == "__main__":
    reconstruir_baseline()