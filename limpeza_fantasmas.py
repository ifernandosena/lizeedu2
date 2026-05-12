import requests
import psycopg2
from envio_lize import LizeManager
from constantes import DB_CONFIG, TABELA_ALUNOS_GERAL, ANO_LETIVO_ATUAL
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuração de log
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

class GhostCleaner(LizeManager):
    def processar_fantasma(self, aluno, mats_validas):
        mat = str(aluno.get("enrollment_number")).strip()
        id_aluno = aluno.get("id")
        nome = aluno.get("name")
        
        if mat not in mats_validas:
            logging.info(f"🚫 FANTASMA DETECTADO | Mat: {mat} | Nome: {nome}")
            
            # 1. Remover de todas as turmas
            if self.api_set_classes(id_aluno, None):
                logging.info(f"   - Turmas removidas com sucesso.")
            
            # 2. Desativar se estiver ativo
            if aluno.get("is_active"):
                if self.api_disable(id_aluno):
                    logging.info(f"   - Aluno desativado com sucesso.")
            return True
        return False

    def executar_limpeza(self):
        logging.info("🚀 Iniciando Limpeza Profunda de Alunos Fantasmas (Lize 2026)...")
        
        # 1. Carregar matrículas válidas
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT matricula FROM {TABELA_ALUNOS_GERAL} WHERE (turma::NUMERIC >= 11500) AND (sit::NUMERIC NOT IN (2, 4))")
                mats_validas = {str(r[0]).strip() for r in cur.fetchall()}
        
        logging.info(f"✅ Matrículas válidas na fonte (2026): {len(mats_validas)}")
        
        # 2. Iterar sobre a API (Paginado)
        url = f"https://app.lizeedu.com.br/api/v2/students/?school_year={ANO_LETIVO_ATUAL}"
        total_limpos = 0
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            while url:
                logging.info(f"Buscando página: {url}")
                r = self.session.get(url, timeout=15)
                if r.status_code != 200:
                    logging.error(f"Erro na API: {r.status_code}")
                    break
                
                data = r.json()
                alunos = data.get("results", [])
                
                futures = [executor.submit(self.processar_fantasma, a, mats_validas) for a in alunos]
                for future in as_completed(futures):
                    if future.result():
                        total_limpos += 1
                
                url = data.get("next")
                logging.info(f"Status: {total_limpos} fantasmas removidos até o momento...")

        logging.info("="*60)
        logging.info(f"🏁 LIMPEZA CONCLUÍDA! Total de {total_limpos} fantasmas expulsos de 2026.")
        logging.info("="*60)

if __name__ == "__main__":
    GhostCleaner().executar_limpeza()
