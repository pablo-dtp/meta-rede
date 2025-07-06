import psycopg2
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime
from logger import Logger  # importa o logger centralizado

class VendasPorMes:
    def __init__(self, id_loja):
        load_dotenv()
        self.id_loja = id_loja
        self.conn_pg = None
        self.cursor_pg = None
        self.conn_sqlite = None
        self.cursor_sqlite = None

        # Pega o path do banco SQLite do .env
        self.db_path = os.getenv("DB_LITE_PATH")
        if not self.db_path:
            raise ValueError("Variável DB_LITE_PATH não configurada no .env")

        # Inicializa logger
        logger_config = Logger()
        self.logger = logger_config.get_logger(self.__class__.__name__)

    def conectar_postgres(self):
        self.conn_pg = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        self.cursor_pg = self.conn_pg.cursor()
        self.logger.info(f"Conectado ao PostgreSQL para loja {self.id_loja}.")

    def conectar_sqlite(self):
        self.conn_sqlite = sqlite3.connect(self.db_path)
        self.cursor_sqlite = self.conn_sqlite.cursor()

        self.cursor_sqlite.execute("""
            CREATE TABLE IF NOT EXISTS vendas_por_mes (
                id_loja INTEGER,
                mes_referencia TEXT,
                valor_venda REAL,
                PRIMARY KEY (id_loja, mes_referencia)
            )
        """)
        self.conn_sqlite.commit()

        self.logger.info("Tabela vendas_por_mes criada/verificada no SQLite.")

    def buscar_vendas_pg(self, ano, mes):
        query = """
            SELECT
                EXTRACT(MONTH FROM data) AS mes,
                ROUND(SUM(subtotalimpressora - valordesconto + valoracrescimo), 2) as venda
            FROM pdv.venda
            WHERE EXTRACT(YEAR FROM data) = %s
              AND EXTRACT(MONTH FROM data) = %s
              AND cancelado = false
              AND id_loja = %s
            GROUP BY EXTRACT(MONTH FROM data)
            ORDER BY mes;
        """
        self.cursor_pg.execute(query, (ano, mes, self.id_loja))
        resultado = self.cursor_pg.fetchall()
        self.logger.info(f"Buscadas vendas para loja {self.id_loja} em {ano}-{mes:02d}.")
        return resultado

    def salvar_sqlite(self, ano, dados):
        for mes_pg, venda in dados:
            mes_pg_int = int(mes_pg) if mes_pg is not None else None
            venda_float = float(venda) if venda is not None else 0.0

            mes_referencia = f"{ano}-{mes_pg_int:02d}"

            self.cursor_sqlite.execute("""
                INSERT OR REPLACE INTO vendas_por_mes (id_loja, mes_referencia, valor_venda)
                VALUES (?, ?, ?)
            """, (
                self.id_loja,
                mes_referencia,
                venda_float
            ))
        self.conn_sqlite.commit()
        self.logger.info(f"Salvo SQLite para loja {self.id_loja} em {ano}.")

    def consultar_venda(self):
        try:
            agora = datetime.now()
            ano = agora.year
            mes = agora.month

            self.conectar_postgres()
            self.conectar_sqlite()

            dados = self.buscar_vendas_pg(ano, mes)
            if dados:
                self.salvar_sqlite(ano, dados)
            else:
                self.logger.info(f"Sem vendas encontradas para loja {self.id_loja} em {ano}-{mes:02d}.")

            self.logger.info(f"Processo finalizado para loja {self.id_loja} em {ano}-{mes:02d}.")
        except Exception as e:
            self.logger.error(f"Erro para loja {self.id_loja}: {e}")
        finally:
            self.fechar_conexoes()

    def fechar_conexoes(self):
        if self.cursor_pg:
            self.cursor_pg.close()
        if self.conn_pg:
            self.conn_pg.close()
        if self.cursor_sqlite:
            self.cursor_sqlite.close()
        if self.conn_sqlite:
            self.conn_sqlite.close()
        self.logger.info(f"Conexões fechadas para loja {self.id_loja}.")


if __name__ == "__main__":
    lojas = [1, 2, 3]
    for loja in lojas:
        vp = VendasPorMes(id_loja=loja)
        vp.consultar_venda()
