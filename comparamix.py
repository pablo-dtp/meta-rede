import sqlite3
import logging
import os

class ComparadorMixProdutos:
    def __init__(self, db_path='Banco/Produtos.db'):
        self.db_path = db_path
        # Configura o logger da classe
        log_dir = "Logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logging.basicConfig(
            filename=os.path.join(log_dir, 'compararmix.log'),
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )
        self.logger = logging.getLogger(__name__)

    def calcular_percentual_comprados(self, mes_referencia=None, id_loja=None):
        """
        Calcula a porcentagem de produtos comprados em relação aos ofertados pela rede,
        e já registra o resultado no log.

        Args:
            mes_referencia (str): opcional, no formato 'YYYY-MM'. Se informado, filtra pelos produtos desse mês.
            id_loja (int or str): opcional, se informado filtra produtos comprados por loja.

        Returns:
            dict: dicionário com total ofertado, total comprado e percentual.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if mes_referencia:
            cursor.execute("""
                SELECT COUNT(DISTINCT codigoexterno) FROM produtosrede_historico WHERE mes_referencia = ?
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
