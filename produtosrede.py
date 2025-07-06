import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from datetime import datetime
import sqlite3
import os
import time


class ProdutosRedeScraper:
    def __init__(self, db_path='Banco/Produtos.db', log_dir='Logs'):
        self.db_path = db_path
        self.log_dir = log_dir
        self._setup_logging()
        load_dotenv()
        self.usuario = os.getenv("USUARIO_VR")
        self.senha = os.getenv("SENHA_VR")
        self.mes_referencia = datetime.now().strftime("%Y-%m")
        self.data_coleta = datetime.now().strftime("%Y-%m-%d")

    def _setup_logging(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        logging.getLogger("webdriver_manager").setLevel(logging.WARNING)
        logging.basicConfig(
            filename=os.path.join(self.log_dir, 'produtosrede.log'),
            filemode='a',
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO
        )

    def _setup_db(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS produtosrede_historico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigoexterno TEXT,
                descricao TEXT,
                mes_referencia TEXT,
                data_coleta TEXT,
                UNIQUE (codigoexterno, mes_referencia)
            )
        ''')
        self.conn.commit()

    def _setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )

    def coletar_produtos(self):
        self._setup_db()
        self._setup_driver()

        try:
            logging.info("Iniciando a coleta de produtos.")
            self.driver.get("http://redeintegrada.ddns.net:38080/php/vrcentralrede/login.php")
            time.sleep(2)

            # Login
            self.driver.find_element(By.ID, "usuario").send_keys(self.usuario)
            self.driver.find_element(By.ID, "senha").send_keys(self.senha)
            self.driver.find_element(By.ID, "btnAutenticar").click()
            time.sleep(3)

            if "Usuário ou senha inválidos" in self.driver.page_source or "Login inválido" in self.driver.page_source:
                logging.error("Falha no login: usuário ou senha incorretos.")
                return False

            self.driver.find_element(By.XPATH, "//a[contains(text(),'Novo')]").click()
            time.sleep(2)

            select_element = Select(self.driver.find_element(By.ID, "tipoExibicao"))
            select_element.select_by_value("1")
            time.sleep(5)

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            tabelas = soup.find_all('table', class_='grid')

            todos_codigos = []
            todos_descricoes = []
            ignorar_proximo_produto = False

            for tabela in tabelas:
                if not tabela.has_attr('id'):
                    titulo = tabela.get_text(strip=True).upper()
                    if "FRUTAS" in titulo or "VERDURAS" in titulo:
                        ignorar_proximo_produto = True
                    else:
                        ignorar_proximo_produto = False

                elif tabela.get('id') == 'tabela_produto':
                    if ignorar_proximo_produto:
                        continue

                    codigos = tabela.select('span[id^="codigo["]')
                    descricoes = tabela.select('span[id^="descricaocompleta["]')

                    for c, d in zip(codigos, descricoes):
                        codigo = c.text.strip()
                        descricao = d.text.strip()
                        todos_codigos.append(codigo)
                        todos_descricoes.append(descricao)

            # Inserção/atualização no banco
            for codigo, descricao in zip(todos_codigos, todos_descricoes):
                self.cursor.execute('''
                    INSERT OR IGNORE INTO produtosrede_historico 
                    (codigoexterno, descricao, mes_referencia, data_coleta)
                    VALUES (?, ?, ?, ?)
                ''', (codigo, descricao, self.mes_referencia, self.data_coleta))

                if self.cursor.rowcount == 0:
                    self.cursor.execute('''
                        UPDATE produtosrede_historico
                        SET descricao = ?, data_coleta = ?
                        WHERE codigoexterno = ? AND mes_referencia = ?
                    ''', (descricao, self.data_coleta, codigo, self.mes_referencia))

            self.conn.commit()
            logging.info(f"{len(todos_codigos)} produtos processados com base em {self.mes_referencia}.")
            return True

        except (WebDriverException, TimeoutException) as e:
            logging.error(f"Erro ao acessar o site ou timeout: {e}")
            return False

        except Exception as e:
            logging.error(f"Erro inesperado: {e}")
            return False

        finally:
            if self.driver:
                self.driver.quit()
            if self.conn:
                self.conn.close()


# Para rodar isolado:
if __name__ == "__main__":
    scraper = ProdutosRedeScraper()
    success = scraper.coletar_produtos()
    if success:
        print("Coleta finalizada com sucesso.")
    else:
        print("Erro na coleta.")