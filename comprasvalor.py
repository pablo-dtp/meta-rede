import psycopg2
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime
from logger import Logger


class ComprasValorPorMes:
    def __init__(self, id_loja, mes_referencia=None):
        load_dotenv()

        self.id_loja = id_loja

        # Se mes_referencia não for passado, usa data atual formatada YYYY-MM
        if mes_referencia is None:
            agora = datetime.now()
            self.mes_referencia = f"{agora.year}-{agora.month:02d}"
        else:
            self.mes_referencia = mes_referencia

        # Extrai ano e mês do mes_referencia
        try:
            self.ano, self.mes = map(int, self.mes_referencia.split('-'))
        except Exception:
            raise ValueError("mes_referencia deve estar no formato 'YYYY-MM'")

        self.pg_conn = None
        self.pg_cursor = None
        self.sqlite_conn = None
        self.sqlite_cursor = None

        # Pega o caminho do SQLite do .env
        self.db_path = os.getenv("DB_LITE_PATH")
        if not self.db_path:
            raise ValueError("Variável DB_LITE_PATH não configurada no .env")

        # Configura o logger
        logger_config = Logger()
        self.logger = logger_config.get_logger(self.__class__.__name__)

    def conectar_postgres(self):
        self.pg_conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        self.pg_cursor = self.pg_conn.cursor()
        self.logger.info(f"Conectado ao PostgreSQL para loja {self.id_loja}.")

    def conectar_sqlite(self):
        self.sqlite_conn = sqlite3.connect(self.db_path)
        self.sqlite_cursor = self.sqlite_conn.cursor()

        # Cria tabela se não existir
        self.sqlite_cursor.execute("""
            CREATE TABLE IF NOT EXISTS compras_valor_por_mes (
                id_loja INTEGER,
                mes_referencia TEXT,
                valor_total REAL,
                PRIMARY KEY (id_loja, mes_referencia)
            )
        """)
        self.sqlite_conn.commit()

        self.logger.info("Tabela compras_valor_por_mes criada/verificada no SQLite.")

    def buscar_compras_pg(self):
        query = """
            SELECT 
                EXTRACT(MONTH FROM dataemissao) AS mes,
                SUM(valortotal) AS total_mes
            FROM public.notaentrada
            WHERE EXTRACT(YEAR FROM dataemissao) = %s
              AND EXTRACT(MONTH FROM dataemissao) = %s
              AND id_loja = %s
              AND id_tipoentrada != 3
              AND id_fornecedor = 2
            GROUP BY EXTRACT(MONTH FROM dataemissao)
            ORDER BY mes;
        """
        self.pg_cursor.execute(query, (self.ano, self.mes, self.id_loja))
        resultado = self.pg_cursor.fetchall()

        self.logger.info(
            f"Buscadas compras para loja {self.id_loja}, {self.mes_referencia}."
        )
        return resultado

    def salvar_sqlite(self, dados_pg):
        for mes_pg, total_mes in dados_pg:
            mes_int = int(mes_pg) if mes_pg is not None else None
            total_float = float(total_mes) if total_mes is not None else 0.0

            mes_referencia = f"{self.ano}-{mes_int:02d}"

            self.sqlite_cursor.execute("""
                INSERT OR REPLACE INTO compras_valor_por_mes 
                (id_loja, mes_referencia, valor_total)
                VALUES (?, ?, ?)
            """, (
                self.id_loja,
                mes_referencia,
                total_float
            ))
        
        self.sqlite_conn.commit()
        self.logger.info(f"Dados salvos no SQLite para loja {self.id_loja} em {self.mes_referencia}.")

    def consultar_compras(self):
        try:
            self.conectar_postgres()
            self.conectar_sqlite()

            dados = self.buscar_compras_pg()
            if dados:
                self.salvar_sqlite(dados)
            else:
                self.logger.info(
                    f"Sem notas encontradas para loja {self.id_loja} em {self.mes_referencia}."
                )

            self.logger.info(
                f"Processo finalizado para loja {self.id_loja} em {self.mes_referencia}."
            )
        except Exception as e:
            self.logger.error(f"Erro para loja {self.id_loja}: {e}")
        finally:
            self.fechar_conexoes()

    def fechar_conexoes(self):
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
        if self.sqlite_cursor:
            self.sqlite_cursor.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()
        self.logger.info(f"Conexões fechadas para loja {self.id_loja}.")


if __name__ == "__main__":
    for loja in [1, 2, 3]:
        compras = ComprasValorPorMes(id_loja=loja, mes_referencia="2025-06")
        compras.consultar_compras()
