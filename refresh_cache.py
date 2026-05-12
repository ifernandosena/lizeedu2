from envio_lize import LizeManager
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")

if __name__ == "__main__":
    lm = LizeManager()
    lm.atualizar_cache_alunos()
