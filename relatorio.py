import sqlite3
import os
import base64
from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
import logging
import matplotlib.pyplot as plt
from weasyprint import HTML
from jinja2 import Environment, FileSystemLoader, select_autoescape
from logger import Logger
import numpy as np

class RelatorioMeta:
    def __init__(self, mes_referencia):
        load_dotenv()
        self.mes_referencia = mes_referencia
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

        self.pasta = os.path.join(os.getcwd(), "Relatorio")
        os.makedirs(self.pasta, exist_ok=True)

        self.env = Environment(
            loader=FileSystemLoader(self.pasta),
            autoescape=select_autoescape(['html'])
        )
        self.template = self.env.get_template("template.html")

    def conectar(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cur = self.conn.cursor()
        self.logger.info("Conectado ao SQLite")

    def fechar(self):
        self.cur.close()
        self.conn.close()
        self.logger.info("Conexão SQLite fechada")

    def buscar_dados(self):
        self.cur.execute("""
            SELECT id_loja,
                   metavalor, metavalorbatido, percentual_metavalor,
                   skumetamix, skumetamixcomprado,
                   percentual_metamix, bonificacao_pct, valor_bonificacao, motivo
            FROM resultado_meta_por_mes
            WHERE mes_referencia = ?
            ORDER BY id_loja
        """, (self.mes_referencia,))
        rows = self.cur.fetchall()
        dados = []
        for r in rows:
            loja = "Deus Te Pague" if r[0] == 0 else r[0]
            dados.append({
                "id_loja": loja,
                "metavalor": r[1],
                "metavalorabatido": r[2],
                "percentual_metavalor": r[3],
                "skumetamix": r[4],
                "skumetamixcomprado": r[5],
                "percentual_metamix": r[6],  # já é percentual direto (ex: 23.3)
                "bonificacao_pct": r[7],
                "valor_bonificacao": r[8],
                "motivo": r[9]
            })
        self.logger.info(f"{len(dados)} registros de meta carregados")
        return dados

    def buscar_bonificacoes_mes(self):
        self.logger.info(f"Buscando bonificações para o mês meta {self.mes_referencia}...")
        self.cur.execute("""
            SELECT id_loja, SUM(valortotal)
            FROM bonificacao_por_mes
            WHERE mes_referencia_meta = ?
            GROUP BY id_loja
        """, (self.mes_referencia,))
        rows = self.cur.fetchall()
        dados = {}
        for r in rows:
            loja = "Deus Te Pague" if r[0] == 0 else r[0]
            dados[loja] = r[1] or 0.0
        self.logger.info(f"{len(dados)} registros de bonificação carregados")
        return dados

    def comparar_bonificacoes(self, dados_meta, dados_bonif_chegou):
        bonificacoes = []
        for d in dados_meta:
            loja = d["id_loja"]
            valor_deveria = d["valor_bonificacao"]
            val_chegou = dados_bonif_chegou.get(loja, 0.0)
            status = "OK" if abs(valor_deveria - val_chegou) < 0.01 else "DIVERGENTE"
            bonificacoes.append({
                "id_loja": loja,
                "valor_deveria_chegar": valor_deveria,
                "valor_chegou": val_chegou,
                "status": status
            })
        return bonificacoes

    def calcular_cards(self, dados):
        lojas = [d for d in dados if d["id_loja"] != "Deus Te Pague"]
        total_lojas = len(lojas)
        media_meta = sum(d['percentual_metavalor'] for d in lojas) / total_lojas if total_lojas else 0

        total_sku_catalogo = sum(d['skumetamix'] for d in lojas)
        total_sku_comprado = sum(d['skumetamixcomprado'] for d in lojas)
        perc_mix_total = (total_sku_comprado / total_sku_catalogo * 100) if total_sku_catalogo else 0.0

        total_bonif = sum(d['valor_bonificacao'] for d in lojas)

        return {
            "total_lojas": total_lojas,
            "media_meta_valor": round(media_meta, 2),
            "media_mix": round(perc_mix_total, 2),
            "total_bonificacao": total_bonif,
            "skus_catalogo_total": total_sku_catalogo,
            "skus_comprados_total": total_sku_comprado
        }

    def grafico_colunas_meta_valor(self):
        self.cur.execute("""
            SELECT id_loja, metavalor, metavalorbatido
            FROM resultado_meta_por_mes
            WHERE mes_referencia = ?
            ORDER BY id_loja
        """, (self.mes_referencia,))
        rows = self.cur.fetchall()

        lojas = []
        metas = []
        valores_batidos = []

        for id_loja, meta_val, valor_batido in rows:
            loja_nome = "Deus Te Pague" if id_loja == 0 else f"Loja {id_loja}"
            lojas.append(loja_nome)
            metas.append(meta_val or 0)
            valores_batidos.append(valor_batido or 0)

        x = range(len(lojas))
        largura = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))

        barras_meta = ax.bar([i - largura/2 for i in x], metas, width=largura, label='Meta Valor (R$)', color='lightgreen')
        barras_batido = ax.bar([i + largura/2 for i in x], valores_batidos, width=largura, label='Valor Batido (R$)', color='darkgreen')

        ax.set_xticks(x)
        ax.set_xticklabels(lojas)
        ax.set_ylabel('Valor (R$)')
        ax.set_title(f'Meta Valor e Valor Batido - {self.mes_referencia}')
        ax.legend()

        def rotular_barras(barras):
            for barra in barras:
                altura = barra.get_height()
                ax.annotate(f'R$ {altura:,.0f}',
                            xy=(barra.get_x() + barra.get_width() / 2, altura),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=9)

        rotular_barras(barras_meta)
        rotular_barras(barras_batido)

        plt.tight_layout()

        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.close(fig)
        buf.seek(0)
        self.logger.info("Gráfico de colunas - Meta Valor gerado para o mês atual.")
        return base64.b64encode(buf.read()).decode()

    def grafico_colunas_meta_mix(self):
        self.cur.execute("""
            SELECT id_loja, percentual_metamix
            FROM resultado_meta_por_mes
            WHERE mes_referencia = ?
            ORDER BY id_loja
        """, (self.mes_referencia,))
        rows = self.cur.fetchall()

        lojas = []
        valores_mix = []

        meta_fixa = 50  # percentual meta fixa (%)

        for id_loja, percentual in rows:
            loja_nome = "Deus Te Pague" if id_loja == 0 else f"Loja {id_loja}"
            lojas.append(loja_nome)
            valor_percentual = percentual if percentual is not None else 0.0
            valores_mix.append(valor_percentual)

        x = np.arange(len(lojas))
        largura = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))

        barras_meta = ax.bar(x - largura/2, [meta_fixa]*len(lojas), largura,
                            label='Meta Mix SKUs (50%)', color='lightcoral')

        barras_valor = ax.bar(x + largura/2, valores_mix, largura,
                            label='% Mix SKUs Comprados', color='coral')

        ax.set_xticks(x)
        ax.set_xticklabels(lojas)
        ax.set_ylim(0, 100)
        ax.set_ylabel('Percentual (%)')
        ax.set_title(f'% Mix SKUs Comprados - {self.mes_referencia}')
        ax.legend()

        def rotular_barras(barras):
            for barra in barras:
                altura = barra.get_height()
                ax.annotate(f'{altura:.2f}%',
                            xy=(barra.get_x() + barra.get_width()/2, altura),
                            xytext=(0, 3),
                            textcoords='offset points',
                            ha='center', va='bottom', fontsize=9)

        rotular_barras(barras_meta)
        rotular_barras(barras_valor)

        plt.tight_layout()

        buf = BytesIO()
        plt.savefig(buf, format='png')
        plt.close(fig)
        buf.seek(0)
        self.logger.info("Gráfico de colunas - Mix SKUs gerado para o mês atual.")
        return base64.b64encode(buf.read()).decode()

    def gerar(self):
        self.conectar()
        dados = self.buscar_dados()
        bonifs_chegou = self.buscar_bonificacoes_mes()

        if not dados:
            self.logger.warning("Nenhum dado para relatório")
            self.fechar()
            return

        bonifs = self.comparar_bonificacoes(dados, bonifs_chegou)
        cards = self.calcular_cards(dados)

        grafico_valor = self.grafico_colunas_meta_valor()
        grafico_mix = self.grafico_colunas_meta_mix()

        html = self.template.render(
            mes_referencia=self.mes_referencia,
            cards=cards,
            dados=dados,
            grafico_linha_valor=grafico_valor,
            grafico_linha_mix=grafico_mix,
            bonificacoes=bonifs
        )

        nome = datetime.strptime(self.mes_referencia, "%Y-%m").strftime("RelatorioMetaRede-%m-%y.pdf")
        HTML(string=html).write_pdf(os.path.join(self.pasta, nome))
        self.logger.info(f"Relatório PDF salvo em Relatorio/{nome}")
        self.fechar()


if __name__ == "__main__":
    RelatorioMeta("2025-07").gerar()
