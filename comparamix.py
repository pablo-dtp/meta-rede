import sqlite3
from dotenv import load_dotenv
import os
from logger import Logger  # importa o módulo de logging centralizado

class ComparadorMixProdutos:
    def __init__(self, db_path=None):
        load_dotenv()
        if db_path is None:
            db_path = os.getenv("DB_LITE_PATH")
        if db_path is None:
            raise ValueError("Variável DB_LITE_PATH não encontrada no ambiente")
        self.db_path = db_path


        # Inicializa o logger configurado e obtém logger com nome da classe
        logger_config = Logger()
        self.logger = logger_config.get_logger(self.__class__.__name__)

    def calcular_percentual_comprados(self, mes_referencia=None, id_loja=None):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if mes_referencia:
            cursor.execute("""
                SELECT COUNT(DISTINCT codigoexterno) 
                FROM produtosrede_historico 
                WHERE mes_referencia = ?
            """, (mes_referencia,))
            total_ofertado = cursor.fetchone()[0]
        else:
            cursor.execute("SELECT COUNT(DISTINCT codigoexterno) FROM produtosrede_historico")
            total_ofertado = cursor.fetchone()[0]

        if mes_referencia and id_loja:
            cursor.execute("""
                SELECT COUNT(DISTINCT codigoexterno) 
                FROM produtoscomprados 
                WHERE mes_referencia = ? AND id_loja = ?
            """, (mes_referencia, id_loja))
            total_comprado = cursor.fetchone()[0]
        elif mes_referencia:
            cursor.execute("""
                SELECT COUNT(DISTINCT codigoexterno) 
                FROM produtoscomprados 
                WHERE mes_referencia = ?
            """, (mes_referencia,))
            total_comprado = cursor.fetchone()[0]
        elif id_loja:
            cursor.execute("""
                SELECT COUNT(DISTINCT codigoexterno) 
                FROM produtoscomprados 
                WHERE id_loja = ?
            """, (id_loja,))
            total_comprado = cursor.fetchone()[0]
        else:
            cursor.execute("SELECT COUNT(DISTINCT codigoexterno) FROM produtoscomprados")
            total_comprado = cursor.fetchone()[0]

        conn.close()

        percentual = 0.0
        if total_ofertado > 0:
            percentual = (total_comprado / total_ofertado) * 100

        self.logger.info(f"Loja {id_loja if id_loja else 'todas'}: Total ofertado: {total_ofertado}")
        self.logger.info(f"Loja {id_loja if id_loja else 'todas'}: Total comprado: {total_comprado}")
        self.logger.info(f"Loja {id_loja if id_loja else 'todas'}: Percentual comprado: {percentual:.2f}%")

        return {
            "total_ofertado": total_ofertado,
            "total_comprado": total_comprado,
            "percentual_comprado": percentual
        }


if __name__ == "__main__":
    comp = ComparadorMixProdutos()
    for loja in [1, 2, 3]:
        comp.calcular_percentual_comprados(mes_referencia="2025-07", id_loja=loja)
