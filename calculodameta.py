import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from logger import Logger


class CalculoMeta:
    def __init__(self, id_loja):
        load_dotenv()

        self.id_loja = id_loja
        self.sqlite_conn = None
        self.sqlite_cursor = None

        self.db_path = os.getenv("DB_LITE_PATH")
        if not self.db_path:
            raise ValueError("Variável DB_LITE_PATH não configurada no .env")

        logger_config = Logger()
        self.logger = logger_config.get_logger(self.__class__.__name__)

    def conectar_sqlite(self):
        self.sqlite_conn = sqlite3.connect(self.db_path)
        self.sqlite_cursor = self.sqlite_conn.cursor()
        self.logger.info(f"Conexão SQLite aberta para loja {self.id_loja}.")

    def fechar_sqlite(self):
        if self.sqlite_cursor:
            self.sqlite_cursor.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()
        self.logger.info(f"Conexão SQLite fechada para loja {self.id_loja}.")

    def buscar_vendas_mes_anterior(self, mes_referencia):
        ano, mes = map(int, mes_referencia.split('-'))
        data_ref = datetime(ano, mes, 1)
        mes_ant = data_ref - timedelta(days=1)
        mes_ant_ref = mes_ant.strftime('%Y-%m')

        self.sqlite_cursor.execute("""
            SELECT valor_venda
              FROM vendas_por_mes
             WHERE id_loja = ?
               AND mes_referencia = ?
        """, (self.id_loja, mes_ant_ref))
        row = self.sqlite_cursor.fetchone()

        if row:
            venda = float(row[0])
            self.logger.info(f"Loja {self.id_loja} - Venda mês anterior ({mes_ant_ref}): R$ {venda:,.2f}")
            return venda
        else:
            self.logger.warning(f"Loja {self.id_loja} - Sem dados de venda para {mes_ant_ref}.")
            return 0.0

    def buscar_compras_mes(self, mes_referencia):
        self.sqlite_cursor.execute("""
            SELECT valor_total
              FROM compras_valor_por_mes
             WHERE id_loja = ?
               AND mes_referencia = ?
        """, (self.id_loja, mes_referencia))
        row = self.sqlite_cursor.fetchone()

        if row:
            valor_compra = float(row[0])
            self.logger.info(f"Loja {self.id_loja} - Compras {mes_referencia}: R$ {valor_compra:,.2f}")
            return valor_compra
        else:
            self.logger.warning(f"Loja {self.id_loja} - Sem compras para {mes_referencia}.")
            return 0.0

    def buscar_total_skus_catalogo(self, mes_referencia):
        self.sqlite_cursor.execute("""
            SELECT COUNT(DISTINCT codigoexterno)
              FROM produtosrede_historico
             WHERE mes_referencia = ?
        """, (mes_referencia,))
        row = self.sqlite_cursor.fetchone()
        total = int(row[0]) if row and row[0] else 0
        self.logger.info(f"Loja {self.id_loja} - Total SKUs catálogo em {mes_referencia}: {total}")
        return total

    def buscar_total_skus_comprados(self, mes_referencia):
        self.sqlite_cursor.execute("""
            SELECT COUNT(DISTINCT codigoexterno)
              FROM produtoscomprados
             WHERE mes_referencia = ?
               AND id_loja = ?
        """, (mes_referencia, self.id_loja))
        row = self.sqlite_cursor.fetchone()
        total = int(row[0]) if row and row[0] else 0
        self.logger.info(f"Loja {self.id_loja} - Total SKUs comprados em {mes_referencia}: {total}")
        return total

    def salvar_resultado_meta(self, mes_referencia, meta_25, valor_compra, perc_valor,
                              total_catalogo, total_comprados, perc_mix):
        data_hoje = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.sqlite_cursor.execute("""
            CREATE TABLE IF NOT EXISTS resultado_meta_por_mes (
                id_loja INTEGER,
                mes_referencia TEXT,
                data_ultima_consulta TEXT,
                metavalor REAL,
                metavalorabatido REAL,
                percentual_metavalor REAL,
                skumetamix INTEGER,
                skumetamixcomprado INTEGER,
                percentual_metamix REAL,
                PRIMARY KEY (id_loja, mes_referencia)
            )
        """)

        self.sqlite_cursor.execute("""
            INSERT OR REPLACE INTO resultado_meta_por_mes (
                id_loja, mes_referencia, data_ultima_consulta,
                metavalor, metavalorabatido, percentual_metavalor,
                skumetamix, skumetamixcomprado, percentual_metamix
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.id_loja, mes_referencia, data_hoje,
            meta_25, valor_compra, perc_valor,
            total_catalogo, total_comprados, perc_mix
        ))

        self.sqlite_conn.commit()
        self.logger.info(f"Loja {self.id_loja} - Resultado salvo em resultado_meta_por_mes.")

    def calcular_bonificacao(self, mes_referencia):
        venda_mes_anterior = self.buscar_vendas_mes_anterior(mes_referencia)
        valor_compra = self.buscar_compras_mes(mes_referencia)

        meta_25 = venda_mes_anterior * 0.25
        total_catalogo = self.buscar_total_skus_catalogo(mes_referencia)
        total_comprados = self.buscar_total_skus_comprados(mes_referencia)

        perc_mix = (total_comprados / total_catalogo) * 100 if total_catalogo else 0.0
        perc_valor = (valor_compra / meta_25) * 100 if meta_25 else 0.0

        self.logger.info(f"Loja {self.id_loja} - Meta 25%: R$ {meta_25:,.2f}")
        self.logger.info(f"Loja {self.id_loja} - Comprado: R$ {valor_compra:,.2f} ({perc_valor:.2f}%)")
        self.logger.info(f"Loja {self.id_loja} - MIX: {total_comprados}/{total_catalogo} = {perc_mix:.2f}%")

        self.salvar_resultado_meta(
            mes_referencia, meta_25, valor_compra, perc_valor,
            total_catalogo, total_comprados, perc_mix
        )

    def processar(self, mes_referencia):
        try:
            self.conectar_sqlite()
            self.calcular_bonificacao(mes_referencia)
        except Exception as e:
            self.logger.error(f"Erro no processamento da loja {self.id_loja}: {e}")
        finally:
            self.fechar_sqlite()

    @staticmethod
    def calcular_rede(mes_referencia):
        load_dotenv()
        db_path = os.getenv("DB_LITE_PATH")

        logger_config = Logger()
        logger = logger_config.get_logger("CalculoMeta_GRUPO")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT SUM(metavalorabatido), SUM(metavalor),
                   SUM(skumetamixcomprado), SUM(skumetamix)
              FROM resultado_meta_por_mes
             WHERE mes_referencia = ?
               AND id_loja != 0
        """, (mes_referencia,))
        row = cursor.fetchone()

        if row:
            total_comprado, total_meta, total_skus_comprados, total_skus_catalogo = row
            perc_valor = (total_comprado / total_meta) * 100 if total_meta else 0
            perc_mix = (total_skus_comprados / total_skus_catalogo) * 100 if total_skus_catalogo else 0

            data_hoje = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resultado_meta_por_mes (
                    id_loja INTEGER,
                    mes_referencia TEXT,
                    data_ultima_consulta TEXT,
                    metavalor REAL,
                    metavalorabatido REAL,
                    percentual_metavalor REAL,
                    skumetamix INTEGER,
                    skumetamixcomprado INTEGER,
                    percentual_metamix REAL,
                    PRIMARY KEY (id_loja, mes_referencia)
                )
            """)

            cursor.execute("""
                INSERT OR REPLACE INTO resultado_meta_por_mes (
                    id_loja, mes_referencia, data_ultima_consulta,
                    metavalor, metavalorabatido, percentual_metavalor,
                    skumetamix, skumetamixcomprado, percentual_metamix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                0, mes_referencia, data_hoje,
                total_meta, total_comprado, perc_valor,
                total_skus_catalogo, total_skus_comprados, perc_mix
            ))

            conn.commit()
            logger.info(f"[Deus Te Pague] Totais salvos para {mes_referencia} como id_loja = 0.")
            logger.info(f"[Deus Te Pague] Valor Comprado: R$ {total_comprado:,.2f}")
            logger.info(f"[Deus Te Pague] Meta Valor: R$ {total_meta:,.2f}")
            logger.info(f"[Deus Te Pague] Percentual Meta Valor: {perc_valor:.2f}%")
            logger.info(f"[Deus Te Pague] Total SKUs catálogo: {total_skus_catalogo}")
            logger.info(f"[Deus Te Pague] Total SKUs comprados: {total_skus_comprados}")
            logger.info(f"[Deus Te Pague] Percentual MIX: {perc_mix:.2f}%")

        else:
            logger.warning(f"[Deus Te Pague] Nenhum dado consolidado encontrado para {mes_referencia}.")

        conn.close()


if __name__ == "__main__":
    agora = datetime.now()
    mes_referencia = agora.strftime("%Y-%m")

    for loja in [1, 2, 3]:
        calc = CalculoMeta(id_loja=loja)
        calc.processar(mes_referencia)

    CalculoMeta.calcular_rede(mes_referencia)
