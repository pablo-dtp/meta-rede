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
        self.criar_tabela_resultado_meta()  # Garante tabela criada ao abrir conexão

    def fechar_sqlite(self):
        if self.sqlite_cursor:
            self.sqlite_cursor.close()
        if self.sqlite_conn:
            self.sqlite_conn.close()
        self.logger.info(f"Conexão SQLite fechada para loja {self.id_loja}.")

    def criar_tabela_resultado_meta(self):
        self.sqlite_cursor.execute("""
            CREATE TABLE IF NOT EXISTS resultado_meta_por_mes (
                id_loja INTEGER,
                mes_referencia TEXT,
                data_ultima_consulta TEXT,
                metavalor REAL,
                metavalorbatido REAL,
                percentual_metavalor REAL,
                skumetamix INTEGER,
                skumetamixcomprado INTEGER,
                percentual_metamix REAL,
                bonificacao_pct REAL,
                valor_bonificacao REAL,
                motivo TEXT,
                PRIMARY KEY (id_loja, mes_referencia)
            )
        """)
        self.sqlite_conn.commit()

    # Funções de busca (vendas, compras, sku etc) permanecem as mesmas

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
                              total_catalogo, total_comprados, perc_mix,
                              bonificacao_pct, valor_bonificacao, motivo):
        data_hoje = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        self.sqlite_cursor.execute("""
            INSERT OR REPLACE INTO resultado_meta_por_mes (
                id_loja, mes_referencia, data_ultima_consulta,
                metavalor, metavalorbatido, percentual_metavalor,
                skumetamix, skumetamixcomprado, percentual_metamix,
                bonificacao_pct, valor_bonificacao, motivo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            self.id_loja, mes_referencia, data_hoje,
            meta_25, valor_compra, perc_valor,
            total_catalogo, total_comprados, perc_mix,
            bonificacao_pct, valor_bonificacao, motivo
        ))

        self.sqlite_conn.commit()
        self.logger.info(f"Loja {self.id_loja} - Resultado salvo em resultado_meta_por_mes.")

    def calcular_bonificacao_loja(self, mes_referencia):
        venda_mes_anterior = self.buscar_vendas_mes_anterior(mes_referencia)
        valor_compra = self.buscar_compras_mes(mes_referencia)

        meta_25 = venda_mes_anterior * 0.25
        meta_20 = venda_mes_anterior * 0.20

        total_catalogo = self.buscar_total_skus_catalogo(mes_referencia)
        total_comprados = self.buscar_total_skus_comprados(mes_referencia)

        perc_mix = (total_comprados / total_catalogo) * 100 if total_catalogo else 0.0
        perc_valor = (valor_compra / meta_25) * 100 if meta_25 else 0.0

        bonificacao_pct = 0.0
        motivo = ""

        if valor_compra >= meta_25 and perc_mix >= 50:
            bonificacao_pct = 0.02
            motivo = "Bateu 25% da meta de valor e 50% do mix"
        elif valor_compra >= meta_20 and perc_mix >= 50:
            bonificacao_pct = 0.015
            motivo = "Bateu 20% da meta de valor e 50% do mix"
        elif valor_compra >= meta_20 and perc_mix < 50:
            bonificacao_pct = 0.01
            motivo = "Não bateu mix, mas bateu 20% da meta de valor"
        else:
            bonificacao_pct = 0.0
            motivo = "Não bateu critérios de bonificação"

        valor_bonificacao = valor_compra * bonificacao_pct

        self.logger.info(f"Loja {self.id_loja} - Meta 25%: R$ {meta_25:,.2f}")
        self.logger.info(f"Loja {self.id_loja} - Comprado: R$ {valor_compra:,.2f} ({perc_valor:.2f}%)")
        self.logger.info(f"Loja {self.id_loja} - MIX: {total_comprados}/{total_catalogo} = {perc_mix:.2f}%")
        self.logger.info(f"Loja {self.id_loja} - Bonificação: {bonificacao_pct*100:.2f}% ({motivo})")
        self.logger.info(f"Loja {self.id_loja} - Valor Bonificação: R$ {valor_bonificacao:,.2f}")

        self.salvar_resultado_meta(
            mes_referencia, meta_25, valor_compra, perc_valor,
            total_catalogo, total_comprados, perc_mix,
            bonificacao_pct, valor_bonificacao, motivo
        )

    @staticmethod
    def calcular_bonificacao_grupo(mes_referencia):
        load_dotenv()
        db_path = os.getenv("DB_LITE_PATH")

        logger_config = Logger()
        logger = logger_config.get_logger("CalculoMeta_GRUPO")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Garante que a tabela existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS resultado_meta_por_mes (
                id_loja INTEGER,
                mes_referencia TEXT,
                data_ultima_consulta TEXT,
                metavalor REAL,
                metavalorbatido REAL,
                percentual_metavalor REAL,
                skumetamix INTEGER,
                skumetamixcomprado INTEGER,
                percentual_metamix REAL,
                bonificacao_pct REAL,
                valor_bonificacao REAL,
                motivo TEXT,
                PRIMARY KEY (id_loja, mes_referencia)
            )
        """)
        conn.commit()

        # Soma dados das lojas
        cursor.execute("""
            SELECT 
                SUM(metavalorbatido),
                SUM(metavalor),
                SUM(skumetamixcomprado),
                SUM(skumetamix)
            FROM resultado_meta_por_mes
            WHERE mes_referencia = ?
            AND id_loja != 0
        """, (mes_referencia,))
        row = cursor.fetchone()

        if row:
            total_comprado, total_meta, total_skus_comprados, total_skus_catalogo = row

            perc_valor = (total_comprado / total_meta) * 100 if total_meta else 0.0
            perc_mix = (total_skus_comprados / total_skus_catalogo) * 100 if total_skus_catalogo else 0.0

            # Define bonificação do grupo
            bonificacao_pct = 0.0
            motivo = ""

            # Regras de bonificação GRUPO
            if total_comprado >= total_meta and perc_mix >= 50:
                bonificacao_pct = 0.02
                motivo = "Grupo bateu 100% da meta de valor e 50% do mix"
            elif total_comprado >= (total_meta * 0.80) and perc_mix >= 50:
                bonificacao_pct = 0.015
                motivo = "Grupo bateu 80% da meta de valor e 50% do mix"
            elif total_comprado >= (total_meta * 0.80) and perc_mix < 50:
                bonificacao_pct = 0.01
                motivo = "Grupo bateu 80% da meta de valor, mas não o mix"
            else:
                bonificacao_pct = 0.0
                motivo = "Grupo não bateu critérios de bonificação"

            # Soma bonificações das lojas
            cursor.execute("""
                SELECT SUM(valor_bonificacao)
                FROM resultado_meta_por_mes
                WHERE mes_referencia = ?
                AND id_loja != 0
            """, (mes_referencia,))
            row_bonificacao = cursor.fetchone()
            valor_bonificacao_total = row_bonificacao[0] if row_bonificacao and row_bonificacao[0] else 0.0

            # Insere ou atualiza linha do grupo (id_loja = 0)
            data_hoje = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("""
                INSERT OR REPLACE INTO resultado_meta_por_mes (
                    id_loja, mes_referencia, data_ultima_consulta,
                    metavalor, metavalorbatido, percentual_metavalor,
                    skumetamix, skumetamixcomprado, percentual_metamix,
                    bonificacao_pct, valor_bonificacao, motivo
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                0, mes_referencia, data_hoje,
                total_meta, total_comprado, perc_valor,
                total_skus_catalogo, total_skus_comprados, perc_mix,
                bonificacao_pct, valor_bonificacao_total, motivo
            ))

            conn.commit()

            logger.info(f"[Grupo] Totais salvos para {mes_referencia} como id_loja = 0.")
            logger.info(f"[Grupo] Valor Comprado: R$ {total_comprado:,.2f}")
            logger.info(f"[Grupo] Meta Valor: R$ {total_meta:,.2f}")
            logger.info(f"[Grupo] Percentual Meta Valor: {perc_valor:.2f}%")
            logger.info(f"[Grupo] Total SKUs catálogo: {total_skus_catalogo}")
            logger.info(f"[Grupo] Total SKUs comprados: {total_skus_comprados}")
            logger.info(f"[Grupo] Percentual MIX: {perc_mix:.2f}%")
            logger.info(f"[Grupo] Bonificação % do grupo: {bonificacao_pct * 100:.2f}%")
            logger.info(f"[Grupo] Valor Bonificação do grupo (somatório lojas): R$ {valor_bonificacao_total:,.2f}")

        else:
            logger.warning(f"[Grupo] Nenhum dado consolidado encontrado para {mes_referencia}.")

        conn.close()

    def processar(self, mes_referencia):
        try:
            self.conectar_sqlite()
            if self.id_loja == 0:
                self.calcular_bonificacao_grupo(mes_referencia)
            else:
                self.calcular_bonificacao_loja(mes_referencia)
        except Exception as e:
            self.logger.error(f"Erro no processamento da loja {self.id_loja}: {e}")
        finally:
            self.fechar_sqlite()


if __name__ == "__main__":
    agora = datetime.now()
    mes_referencia = agora.strftime("%Y-%m")

    # Calcula grupo primeiro
    calc_grupo = CalculoMeta(id_loja=0)
    calc_grupo.processar(mes_referencia)

    # Depois calcula as lojas individualmente
    for loja in [1, 2, 3]:
        calc = CalculoMeta(id_loja=loja)
        calc.processar(mes_referencia)
