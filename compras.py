import sqlite3
import xml.etree.ElementTree as ET
import psycopg2
from dotenv import load_dotenv
import os
from datetime import datetime, date
import logging

class ProdutosComprados:
    def __init__(self, id_loja):
        load_dotenv()
        self.id_loja = id_loja
        self.conn_pg = None
        self.cursor_pg = None
        self.conn_sqlite = None
        self.cursor_sqlite = None

        # Setup logging (ajuste conforme quiser)
        log_dir = "Logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        logging.basicConfig(
            filename=os.path.join(log_dir, 'produtoscomprados.log'),
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )

    def conectar_postgres(self):
        self.conn_pg = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            database=os.getenv("PG_DB"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD")
        )
        self.cursor_pg = self.conn_pg.cursor()
        logging.info(f"Conectado ao PostgreSQL para loja {self.id_loja}.")

    def conectar_sqlite(self):
        self.conn_sqlite = sqlite3.connect('Banco/Produtos.db')
        self.cursor_sqlite = self.conn_sqlite.cursor()
        self.cursor_sqlite.execute("""
            CREATE TABLE IF NOT EXISTS produtoscomprados (
                codigoexterno TEXT,
                codigointerno INTEGER,
                descricao TEXT,
                id_loja INTEGER,
                mes_referencia TEXT,
                data_coleta TEXT,
                PRIMARY KEY (codigoexterno, id_loja, mes_referencia)
            )
        """)
        self.conn_sqlite.commit()
        logging.info("Conectado ao SQLite e tabela verificada/criada.")

    def buscar_notas_pg(self, data_ini, data_fim):
        query_xmls = """
        SELECT NFE.NUMERONOTA, NFE.XML 
        FROM NOTAENTRADANFE NFE
        JOIN NOTAENTRADA NE ON NFE.NUMERONOTA = NE.NUMERONOTA
        WHERE NFE.ID_FORNECEDOR = 2 AND NE.ID_FORNECEDOR = 2
        AND NFE.ID_LOJA = %s
        AND NE.ID_LOJA = %s
        AND NFE.CONFERIDO = TRUE
        AND NFE.CARREGADO = TRUE
        AND NE.ID_TIPOENTRADA != 3
        AND NE.DATAEMISSAO BETWEEN %s AND %s
        """
        self.cursor_pg.execute(query_xmls, (self.id_loja, self.id_loja, data_ini, data_fim))
        notas = self.cursor_pg.fetchall()
        logging.info(f"Buscadas {len(notas)} notas fiscais para loja {self.id_loja} entre {data_ini} e {data_fim}.")
        return notas

    def inserir_codigos_externos_sqlite(self, notas, mes_referencia, data_coleta):
        ns = {'ns': 'http://www.portalfiscal.inf.br/nfe'}
        count_inserts = 0
        count_ignorados = 0

        for numeronota, xml_str in notas:
            try:
                root = ET.fromstring(xml_str)
                cprod_elements = root.findall('.//ns:cProd', ns)
                for cprod in cprod_elements:
                    codigo_externo = cprod.text.strip()
                    # Tenta inserir, se já existir para o mesmo mes e loja, ignora
                    self.cursor_sqlite.execute("""
                    INSERT OR IGNORE INTO produtoscomprados (codigoexterno, codigointerno, descricao, id_loja, mes_referencia, data_coleta)
                    VALUES (?, NULL, NULL, ?, ?, ?)
                    """, (codigo_externo, self.id_loja, mes_referencia, data_coleta))
                    if self.cursor_sqlite.rowcount == 0:
                        count_ignorados += 1
                    else:
                        count_inserts += 1
            except Exception as e:
                logging.error(f"Erro ao processar nota {numeronota}: {e}")

        logging.info(f"Loja {self.id_loja}: Inseridos {count_inserts} novos códigos externos.")
        logging.info(f"Loja {self.id_loja}: Ignorados {count_ignorados} códigos já existentes neste mês.")
        return count_inserts, count_ignorados

    def identificar_codigos_internos(self, mes_referencia, data_ini, data_fim):
        self.cursor_sqlite.execute("""
        SELECT codigoexterno FROM produtoscomprados 
        WHERE codigointerno IS NULL AND id_loja = ? AND mes_referencia = ?
        """, (self.id_loja, mes_referencia))
        codigo_externos = [row[0] for row in self.cursor_sqlite.fetchall()]
        if not codigo_externos:
            logging.info(f"Loja {self.id_loja}: Todos os códigos já foram identificados ou tabela está vazia para o mês {mes_referencia}.")
            return 0, 0

        placeholders = ','.join(['%s'] * len(codigo_externos))
        params = [data_ini, data_fim] + codigo_externos

        query_principal = f"""
        SELECT DISTINCT p.id AS codigointerno, p.descricaoreduzida, pf.codigoexterno
        FROM public.notaentrada ne
        JOIN public.notaentradaitem ni ON ne.id = ni.id_notaentrada
        JOIN public.produto p ON ni.id_produto = p.id
        JOIN public.produtofornecedor pf ON ni.id_produto = pf.id_produto
        WHERE pf.id_fornecedor = 2 
        AND ne.id_fornecedor = 2
        AND ne.id_tipoentrada != 3
        AND ne.dataemissao BETWEEN %s AND %s
        AND ne.id_loja = %s
        AND pf.codigoexterno IN ({placeholders})
        """
        # O id_loja no filtro deve ser depois das datas
        params.insert(2, self.id_loja)

        self.cursor_pg.execute(query_principal, params)
        atualizados = 0
        for codint, descricao, codext in self.cursor_pg.fetchall():
            self.cursor_sqlite.execute("""
            UPDATE produtoscomprados 
            SET codigointerno = ?, descricao = ?
            WHERE codigoexterno = ? AND id_loja = ? AND mes_referencia = ?
            """, (codint, descricao, codext, self.id_loja, mes_referencia))
            atualizados += 1

        query_secundaria = f"""
        SELECT DISTINCT p.id AS codigointerno, p.descricaocompleta, pe.codigoexterno
        FROM public.notaentrada ne
        JOIN public.notaentradaitem ni ON ne.id = ni.id_notaentrada
        JOIN public.produto p ON ni.id_produto = p.id
        JOIN public.produtofornecedor pf ON ni.id_produto = pf.id_produto
        JOIN public.produtofornecedorcodigoexterno pe ON pf.id = pe.id_produtofornecedor
        WHERE pf.id_fornecedor = 2
        AND ne.id_fornecedor = 2
        AND ne.id_tipoentrada != 3
        AND ne.dataemissao BETWEEN %s AND %s
        AND ne.id_loja = %s
        AND pe.codigoexterno IN ({placeholders})
        """
        self.cursor_pg.execute(query_secundaria, params)
        for codint, descricao, codext in self.cursor_pg.fetchall():
            self.cursor_sqlite.execute("""
            UPDATE produtoscomprados 
            SET codigointerno = ?, descricao = ?
            WHERE codigoexterno = ? AND codigointerno IS NULL AND id_loja = ? AND mes_referencia = ?
            """, (codint, descricao, codext, self.id_loja, mes_referencia))
            atualizados += 1

        logging.info(f"Loja {self.id_loja}: Atualizados {atualizados} produtos via queries de identificação.")
        return atualizados

    def remover_mercadologico16(self, mes_referencia):
        self.cursor_pg.execute("""
        SELECT p.id
        FROM public.produto p
        WHERE p.mercadologico1 = 16
        """)
        ids_mercadologico16 = [str(row[0]) for row in self.cursor_pg.fetchall()]
        removidos = 0
        if ids_mercadologico16:
            placeholders_sqlite = ','.join(['?'] * len(ids_mercadologico16))
            self.cursor_sqlite.execute(f"""
            DELETE FROM produtoscomprados
            WHERE codigointerno IN ({placeholders_sqlite}) AND id_loja = ? AND mes_referencia = ?
            """, ids_mercadologico16 + [self.id_loja, mes_referencia])
            removidos = self.cursor_sqlite.rowcount
            logging.info(f"Loja {self.id_loja}: Removidos {removidos} produtos com mercadologico1 = 16 da tabela produtoscomprados.")
        else:
            logging.info(f"Loja {self.id_loja}: Nenhum produto com mercadologico1 = 16 encontrado para exclusão.")
        return removidos

    def listar_nao_identificados(self, mes_referencia):
        self.cursor_sqlite.execute("""
        SELECT codigoexterno FROM produtoscomprados 
        WHERE codigointerno IS NULL AND id_loja = ? AND mes_referencia = ?
        """, (self.id_loja, mes_referencia))
        nao_identificados = self.cursor_sqlite.fetchall()
        if nao_identificados:
            logging.info(f"Loja {self.id_loja} - Códigos não identificados:")
            for cod in nao_identificados:
                logging.info(f" - {cod[0]}")
        else:
            logging.info(f"Loja {self.id_loja} - Todos os códigos foram identificados com sucesso.")

    def fechar_conexoes(self):
        if self.cursor_pg:
            self.cursor_pg.close()
        if self.conn_pg:
            self.conn_pg.close()
        if self.cursor_sqlite:
            self.cursor_sqlite.close()
        if self.conn_sqlite:
            self.conn_sqlite.close()
        logging.info(f"Loja {self.id_loja}: Conexões fechadas.")

    def executar_rotina(self, data_ini=None, data_fim=None):
        # Se não passar datas, define automaticamente:
        if data_ini is None or data_fim is None:
            hoje = date.today()
            data_ini = hoje.replace(day=1).strftime("%Y-%m-%d")
            data_fim = hoje.strftime("%Y-%m-%d")

        mes_referencia = datetime.now().strftime("%Y-%m")
        data_coleta = datetime.now().strftime("%Y-%m-%d")

        try:
            logging.info(f"Iniciando rotina para loja {self.id_loja} entre {data_ini} e {data_fim}...")
            self.conectar_postgres()
            self.conectar_sqlite()

            notas = self.buscar_notas_pg(data_ini, data_fim)

            inserts, ignorados = self.inserir_codigos_externos_sqlite(notas, mes_referencia, data_coleta)
            self.conn_sqlite.commit()

            atualizados = self.identificar_codigos_internos(mes_referencia, data_ini, data_fim)
            self.conn_sqlite.commit()

            removidos = self.remover_mercadologico16(mes_referencia)
            self.conn_sqlite.commit()

            self.listar_nao_identificados(mes_referencia)

            total_registros_mes = self.cursor_sqlite.execute("""
                SELECT COUNT(*) FROM produtoscomprados WHERE id_loja = ? AND mes_referencia = ?
            """, (self.id_loja, mes_referencia)).fetchone()[0]
            logging.info(f"Loja {self.id_loja}: Total de registros no mês {mes_referencia}: {total_registros_mes}")

            logging.info(f"Loja {self.id_loja}: Processo finalizado com sucesso.")
        except Exception as e:
            logging.error(f"Loja {self.id_loja}: Erro inesperado: {e}")
        finally:
            self.fechar_conexoes()

if __name__ == "__main__":
    pc = ProdutosComprados(id_loja=1)
    pc.executar_rotina()
    pc2 = ProdutosComprados(id_loja=2)
    pc2.executar_rotina()
    pc3 = ProdutosComprados(id_loja=3)
    pc3.executar_rotina()