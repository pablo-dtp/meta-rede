from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import sqlite3
import os
import time

# Carrega .env
load_dotenv()
usuario = os.getenv("USUARIO_VR")
senha = os.getenv("SENHA_VR")

# Conecta/cria banco SQLite
conn = sqlite3.connect('Banco/Produtos.db')
cursor = conn.cursor()

# Cria tabela se não existir
cursor.execute('''
CREATE TABLE IF NOT EXISTS produtosrede (
    codigoexterno TEXT PRIMARY KEY,
    descricao TEXT
)
''')
conn.commit()

# Inicializa Selenium
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

try:
    driver.get("http://redeintegrada.ddns.net:38080/php/vrcentralrede/login.php")
    time.sleep(2)

    # Login
    driver.find_element(By.ID, "usuario").send_keys(usuario)
    driver.find_element(By.ID, "senha").send_keys(senha)
    driver.find_element(By.ID, "btnAutenticar").click()
    time.sleep(3)

    # Clica "Novo"
    driver.find_element(By.XPATH, "//a[contains(text(),'Novo')]").click()
    time.sleep(2)

    # Seleciona "Mercadológico"
    select_element = Select(driver.find_element(By.ID, "tipoExibicao"))
    select_element.select_by_value("1")
    time.sleep(5)

    # Coleta o HTML da página
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    tabelas = soup.find_all('table', class_='grid')

    todos_codigos = []
    todos_descricoes = []

    ignorar_proximo_produto = False

    for tabela in tabelas:
        # Verifica se é uma tabela de título (sem id)
        if not tabela.has_attr('id'):
            titulo = tabela.get_text(strip=True).upper()
            if "FRUTAS" in titulo or "VERDURAS" in titulo:
                ignorar_proximo_produto = True
            else:
                ignorar_proximo_produto = False

        # Se for a tabela de produtos
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

    # Insere no banco
    for codigo, descricao in zip(todos_codigos, todos_descricoes):
        cursor.execute('''
        INSERT OR IGNORE INTO produtosrede (codigoexterno, descricao) VALUES (?, ?)
        ''', (codigo, descricao))

    conn.commit()
    print(f"{len(todos_codigos)} produtos inseridos (ou já existentes).")

finally:
    driver.quit()
    conn.close()
