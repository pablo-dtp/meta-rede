import os
import sqlite3
from collections import defaultdict, namedtuple
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape
from weasyprint import HTML
from logger import Logger
import logging
import locale
import subprocess

try:
    locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
except locale.Error:
    locale.setlocale(locale.LC_TIME, 'Portuguese_Brazil.1252')

class ValidaBonificacaoAnual:
    def __init__(self, mes_referencia=None):
        load_dotenv()
        self.db_path = os.getenv("DB_LITE_PATH")
        if not self.db_path:
            raise ValueError("DB_LITE_PATH não configurada no .env")

        self.logger = Logger().get_logger(self.__class__.__name__)
        # Silenciar logs muito verbosos de libs usadas
        for lib in ("fontTools", "weasyprint"):
            lg = logging.getLogger(lib)
            lg.setLevel(logging.CRITICAL)
            lg.propagate = False
            for h in lg.handlers[:]:
                lg.removeHandler(h)

        if mes_referencia:
            try:
                self.mes_ref_dt = datetime.strptime(mes_referencia, "%Y-%m")
            except Exception:
                raise ValueError("mes_referencia deve estar no formato 'YYYY-MM'")
        else:
            self.mes_ref_dt = datetime.now()

        self.periodo_meses = [
            (self.mes_ref_dt - relativedelta(months=i)).strftime("%Y-%m")
            for i in reversed(range(12))
        ]

        self.pasta = os.path.join(os.getcwd(), "Relatorio")
        os.makedirs(self.pasta, exist_ok=True)

        self.env = Environment(
            loader=FileSystemLoader(self.pasta),
            autoescape=select_autoescape(['html'])
        )
        self.template = self.env.get_template("template_bonificacoes.html")

    def conectar(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self.logger.info("Conectado ao SQLite.")

    def fechar(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        self.logger.info("Conexão SQLite fechada.")

    def criar_tabela_cruzada(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS resultado_bonificacao_cruzada (
            id_loja INTEGER,
            mes_referencia TEXT,
            valor_previsto REAL,
            valor_recebido REAL,
            diferenca REAL,
            status TEXT,
            PRIMARY KEY (id_loja, mes_referencia)
        )
        """)
        self.conn.commit()
        self.logger.info("Tabela resultado_bonificacao_cruzada criada/verificada.")

    def buscar_previsto(self):
        placeholders = ','.join('?' for _ in self.periodo_meses)
        query = f"""
            SELECT id_loja, mes_referencia, valor_bonificacao
            FROM resultado_meta_por_mes
            WHERE mes_referencia IN ({placeholders})
        """
        self.cur.execute(query, self.periodo_meses)
        rows = self.cur.fetchall()
        previsto = {}
        for id_loja, mes_ref, valor in rows:
            previsto[(id_loja, mes_ref)] = valor or 0.0
        self.logger.info(f"Valores previstos carregados: {len(previsto)} registros.")
        return previsto

    def buscar_recebido(self):
        placeholders = ','.join('?' for _ in self.periodo_meses)
        query = f"""
            SELECT id_loja, mes_referencia_meta, SUM(valortotal)
            FROM bonificacao_por_mes
            WHERE mes_referencia_meta IN ({placeholders})
            GROUP BY id_loja, mes_referencia_meta
        """
        self.cur.execute(query, self.periodo_meses)
        rows = self.cur.fetchall()
        recebido = {}
        for id_loja, mes_ref, valor in rows:
            recebido[(id_loja, mes_ref)] = valor or 0.0
        self.logger.info(f"Valores recebidos carregados: {len(recebido)} registros.")
        return recebido

    def processar_cruzamento(self):
        self.conectar()
        try:
            self.criar_tabela_cruzada()

            previsto = self.buscar_previsto()
            recebido = self.buscar_recebido()

            chaves = set(previsto.keys()) | set(recebido.keys())

            for chave in chaves:
                id_loja, mes_ref = chave
                if id_loja == 0:
                    continue
                val_prev = previsto.get(chave, 0.0)
                val_receb = recebido.get(chave, 0.0)
                diferenca = val_receb - val_prev
                status = "BATIDO" if val_receb >= val_prev else "NÃO BATIDO"

                self.cur.execute("""
                    INSERT OR REPLACE INTO resultado_bonificacao_cruzada (
                        id_loja, mes_referencia, valor_previsto, valor_recebido, diferenca, status
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (id_loja, mes_ref, val_prev, val_receb, diferenca, status))

            meses_unicos = set(mes for _, mes in chaves)
            for mes_ref in meses_unicos:
                lojas_prev = self._todas_lojas(previsto, mes_ref)
                lojas_receb = self._todas_lojas(recebido, mes_ref)

                val_prev_sum = sum(previsto.get((id_loja, mes_ref), 0.0) for id_loja in lojas_prev)
                val_receb_sum = sum(recebido.get((id_loja, mes_ref), 0.0) for id_loja in lojas_receb)
                diferenca_sum = val_receb_sum - val_prev_sum
                status_sum = "BATIDO" if val_receb_sum >= val_prev_sum else "NÃO BATIDO"

                self.cur.execute("""
                    INSERT OR REPLACE INTO resultado_bonificacao_cruzada (
                        id_loja, mes_referencia, valor_previsto, valor_recebido, diferenca, status
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (0, mes_ref, val_prev_sum, val_receb_sum, diferenca_sum, status_sum))

            self.conn.commit()
            self.logger.info(f"Cruzamento processado e salvo para {len(chaves) + len(meses_unicos)} registros (incluindo consolidado).")
        except Exception as e:
            self.logger.error(f"Erro no processamento do cruzamento: {e}")
        finally:
            self.fechar()

    def _todas_lojas(self, dicionario, mes_ref):
        lojas = set()
        for (id_loja, mes), valor in dicionario.items():
            if mes == mes_ref and id_loja != 0:
                lojas.add(id_loja)
        return lojas

    def gerar_relatorio_pdf(self):
        self.conectar()
        try:
            DadosBonificacao = namedtuple(
                "DadosBonificacao",
                ["valor_a_receber", "valor_recebido", "diferenca", "status"]
            )

            dados = defaultdict(dict)

            placeholders = ','.join('?' for _ in self.periodo_meses)
            query = f"""
                SELECT id_loja, mes_referencia, valor_previsto, valor_recebido, diferenca, status
                FROM resultado_bonificacao_cruzada
                WHERE mes_referencia IN ({placeholders})
                ORDER BY id_loja, mes_referencia
            """
            self.cur.execute(query, self.periodo_meses)

            for id_loja, mes_ref, val_prev, val_rec, diff, status in self.cur.fetchall():
                loja = "Deus Te Pague" if id_loja == 0 else f"Loja {id_loja}"
                dados[loja][mes_ref] = DadosBonificacao(
                    valor_a_receber=val_prev or 0.0,
                    valor_recebido=val_rec or 0.0,
                    diferenca=diff or 0.0,
                    status=status or ""
                )

            dados_formatado = defaultdict(dict)
            for loja, meses in dados.items():
                for mes, info in meses.items():
                    mes_label = datetime.strptime(mes, "%Y-%m").strftime("%b/%y").lower()
                    dados_formatado[loja][mes_label] = info

            self.logger.info(f"Dados para relatório carregados para {len(dados)} lojas.")

            html = self.template.render(
                dados=dados_formatado
            )

            mes_label = self.mes_ref_dt.strftime("%m-%y")
            nome_arquivo = f"RelatorioBonificacoesAnual-{mes_label}.pdf"
            caminho_pdf = os.path.join(self.pasta, nome_arquivo)

            HTML(string=html).write_pdf(caminho_pdf)
            self.logger.info(f"Relatório PDF salvo em {caminho_pdf}")

        except Exception as e:
            self.logger.error(f"Erro ao gerar relatório PDF: {e}")
        finally:
            self.fechar()

if __name__ == "__main__":
    logger = Logger().get_logger("Main")

    # Exemplo fixo para teste:
    #mes_referencia = '2025-06'
    mes_referencia = datetime.now().strftime("%Y-%m")
    processador = ValidaBonificacaoAnual(mes_referencia)

    try:
        processador.processar_cruzamento()
        processador.gerar_relatorio_pdf()

        # calcula período (últimos 12 meses)
        mes_ref_dt = datetime.strptime(mes_referencia, "%Y-%m")
        data_inicio = (mes_ref_dt - relativedelta(months=11))
        data_fim = mes_ref_dt

        inicio_final = data_inicio.strftime("%B/%Y").lower()   # ex.: agosto/2024
        fim_final = data_fim.strftime("%B/%Y").lower()         # ex.: junho/2025

        mes_ref = mes_ref_dt.strftime("%m")                    # ex.: "06"
        ano_ref = mes_ref_dt.strftime("%Y")                    # ex.: "2025"

        logger.info(f"Período do relatório: {inicio_final} até {fim_final}")

        pasta_bot = os.path.join(os.getcwd(), 'BotWhatsapp')
        caminho_script_node = os.path.join(pasta_bot, 'enviarRelatorioBonificacao.js')

        logger.info(f"Executando script Node.js para envio do relatório: {caminho_script_node}")

        resultado = subprocess.run(
            ['node', caminho_script_node, inicio_final, fim_final, mes_ref, ano_ref],
            cwd=pasta_bot,
            capture_output=True,
            text=True,
            check=True
        )

        logger.info(f"Script Node.js executado com sucesso. Output:\n{resultado.stdout}")

    except subprocess.CalledProcessError as e:
        logger.error(f"Erro ao executar script Node.js: {e.stderr}")
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
