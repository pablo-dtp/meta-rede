import logging
import os
from datetime import datetime

class Logger:
    def __init__(self, log_dir='Logs'):
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        agora = datetime.now()
        nome_arquivo = f"Log_{agora.strftime('%m-%y')}.log"
        caminho_log = os.path.join(log_dir, nome_arquivo)

        # Configura o logging global (uma vez)
        logging.basicConfig(
            filename=caminho_log,
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            level=logging.INFO
        )

    def get_logger(self, nome):
        return logging.getLogger(nome)
