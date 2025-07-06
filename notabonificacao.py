import psycopg2
import sqlite3
import os
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from logger import Logger

class BonificacaoPorMes:
    def __init__(self, id_loja, mes_referencia=None):
        load_dotenv()

        self.id_loja = id_loja

        if mes_referencia is None:
            agora = datetime.now()
            self.ano = agora.year
            self.mes = agora.month
            self.mes_referencia = f"{agora.year}-{agora.month:02d}"
        else:
            try:
                partes = mes_referencia.split("-")
                self.ano = int(partes[0])
                self.mes = int(partes[1])
                self.mes_referencia = mes_referencia
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

        # Cria a tabela com a nova estrutura
        self.sqlite_cursor.execute("""
            CREATE TABLE IF NOT EXISTS bonificacao_por_mes (
                mes_referencia_meta TEXT,
                mes_lancamento TEXT,
                id_loja INTEGER,
                bonificacao BOOLEAN,
                valortotal REAL,
                PRIMARY KEY (mes_referencia_meta, id_loja)
            )
        """)
        self.sqlite_conn.commit()
        self.logger.info("Tabela bonificacao_por_mes criada/verificada no SQLite.")

    def mes_referencia_meta(self):
        dt = datetime(self.ano, self.mes, 1)
        dt_meta = dt - relativedelta(months=1)
        return dt_meta.strftime("%Y-%m")

    def buscar_bonificacao_pg(self):
        query = """
            SELECT 
                EXTRACT(MONTH FROM dataemissao) AS mes,
                SUM(valortotal) AS total_mes
            FROM public.notaentrada
            WHERE EXTRACT(YEAR FROM dataemissao) = %s
              AND EXTRACT(MONTH FROM dataemissao) = %s
              AND id_loja = %s
              AND id_tipoentrada = 3
              AND id_fornecedor = 2
            GROUP BY EXTRACT(MONTH FROM dataemissao)
            ORDER BY mes;
        """
        self.pg_cursor.execute(query, (self.ano, self.mes, self.id_loja))
        resultado = self.pg_cursor.fetchall()

        self.logger.info(
            f"Buscadas bonificações para loja {self.id_loja} em {self.mes_referencia}."
        )
        return resultado

    def salvar_sqlite(self, dados_pg):
        if dados_pg:
            total = float(dados_pg[0][1]) if dados_pg[0][1] is not None else 0.0
            bonificacao = True
        else:
            total = 0.0
            bonificacao = False

        mes_meta = self.mes_referencia_meta()

        self.sqlite_cursor.execute("""
            INSERT OR REPLACE INTO bonificacao_por_mes
            (mes_referencia_meta, mes_lancamento, id_loja, bonificacao, valortotal)
            VALUES (?, ?, ?, ?, ?)
        """, (
            mes_meta,
            self.mes_referencia,  # mês em que a bonificação chegou
            self.id_loja,
            bonificacao,
            total
        ))
        self.sqlite_conn.commit()

        self.logger.info(
            f"Salvo bonificação no SQLite: loja={self.id_loja}, "
            f"meta={mes_meta}, lancamento={self.mes_referencia}, "
            f"bonificacao={bonificacao}, total={total:.2f}"
        )

    def verificar_bonificacao(self):
        try:
            self.conectar_postgres()
            self.conectar_sqlite()

            dados = self.buscar_bonificacao_pg()
            self.salvar_sqlite(dados)

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
        bonif = BonificacaoPorMes(id_loja=loja, mes_referencia="2025-07")
        bonif.verificar_bonificacao()
