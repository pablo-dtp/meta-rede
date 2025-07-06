from datetime import datetime
from dateutil.relativedelta import relativedelta
from logger import Logger

from compras import ProdutosComprados
from vendas import VendasPorMes
from notabonificacao import BonificacaoPorMes
from comprasvalor import ComprasValorPorMes
from comparamix import ComparadorMixProdutos
from calculodameta import CalculoMeta

class Main:
    def __init__(self, lojas, mes_referencia):
        self.lojas = lojas
        self.mes_referencia = mes_referencia
        
        # Calcula mês anterior para vendas
        ano, mes = map(int, mes_referencia.split("-"))
        dt_mes = datetime(ano, mes, 1)
        dt_mes_vendas = dt_mes - relativedelta(months=1)
        self.mes_vendas = dt_mes_vendas.strftime("%Y-%m")

        # Logger centralizado
        logger_config = Logger()
        self.logger = logger_config.get_logger("Principal")

    def executar_vendas(self):
        self.logger.info(f"Executando Vendas (mês: {self.mes_vendas})")
        for loja in self.lojas:
            self.logger.info(f"Iniciando vendas para loja {loja}")
            vendas = VendasPorMes(id_loja=loja, mes_referencia=self.mes_vendas)
            vendas.consultar_venda()

    def executar_compras(self):
        self.logger.info(f"Executando Compras (mês: {self.mes_referencia})")
        for loja in self.lojas:
            self.logger.info(f"Iniciando compras para loja {loja}")
            compras = ProdutosComprados(id_loja=loja, mes_referencia=self.mes_referencia)
            compras.executar_rotina()

    def executar_bonificacao(self):
        self.logger.info(f"Executando Bonificação (mês: {self.mes_referencia})")
        for loja in self.lojas:
            self.logger.info(f"Iniciando bonificação para loja {loja}")
            bonificacao = BonificacaoPorMes(id_loja=loja, mes_referencia=self.mes_referencia)
            bonificacao.verificar_bonificacao()

    def executar_compras_valor(self):
        self.logger.info(f"Executando Compras Valor (mês: {self.mes_referencia})")
        for loja in self.lojas:
            self.logger.info(f"Iniciando compras valor para loja {loja}")
            compras_valor = ComprasValorPorMes(id_loja=loja, mes_referencia=self.mes_referencia)
            compras_valor.consultar_compras()

    def executar_comparamix(self):
        self.logger.info(f"Executando Comparador Mix Produtos (mês: {self.mes_referencia})")
        comp = ComparadorMixProdutos()
        self.logger.info("Calculando percentual geral")
        comp.calcular_percentual_comprados(mes_referencia=self.mes_referencia, id_loja=None)  # geral
        for loja in self.lojas:
            self.logger.info(f"Calculando percentual para loja {loja}")
            comp.calcular_percentual_comprados(mes_referencia=self.mes_referencia, id_loja=loja)

    def executar_calculodameta(self):
        self.logger.info(f"Executando Cálculo da Meta (mês: {self.mes_referencia})")
        for loja in self.lojas:
            self.logger.info(f"Iniciando cálculo da meta para loja {loja}")
            meta = CalculoMeta(id_loja=loja)
            meta.processar(self.mes_referencia)

        self.logger.info("Iniciando cálculo consolidado da rede (id_loja=0)")
        CalculoMeta.calcular_rede(self.mes_referencia)


    def executar_todas_rotinas(self):
        self.logger.info(f"Executando todas as rotinas para lojas: {self.lojas} - mês referência: {self.mes_referencia}")
        self.logger.info(f"Mês vendas (mês anterior): {self.mes_vendas}")

        self.executar_vendas()
        self.executar_compras()
        self.executar_bonificacao()
        self.executar_compras_valor()
        self.executar_comparamix()
        self.executar_calculodameta()


if __name__ == "__main__":
    from datetime import datetime
    lojas = [1, 2, 3]
    mes_referencia = datetime.now().strftime("%Y-%m")  # pega mês atual

    rotinas = Main(lojas, mes_referencia)
    rotinas.executar_todas_rotinas()
