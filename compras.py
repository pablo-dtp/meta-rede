import sqlite3
import xml.etree.ElementTree as ET
import psycopg2
from dotenv import load_dotenv
import os

# Carrega variáveis do .env
load_dotenv()

PG_HOST = os.getenv("PG_HOST")
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

SQLITE_PATH = r"C:\MetaRede\Banco\Produtos.db"

# Conecta ao PostgreSQL
conn_pg = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD
)
cursor_pg = conn_pg.cursor()

# Conecta ao SQLite
conn_sqlite = sqlite3.connect(SQLITE_PATH)
cursor_sqlite = conn_sqlite.cursor()

# Garante que a tabela produtoscomprados exista
cursor_sqlite.execute("""
CREATE TABLE IF NOT EXISTS produtoscomprados (
    codigoexterno TEXT PRIMARY KEY,
    codigointerno INTEGER,
    descricao TEXT
)
""")


# Etapa 1: Coleta dos XMLs e extração dos códigos externos (cProd)
query_xmls = """
SELECT NFE.NUMERONOTA, NFE.XML 
FROM NOTAENTRADANFE NFE
JOIN NOTAENTRADA NE ON NFE.NUMERONOTA = NE.NUMERONOTA
WHERE NFE.ID_FORNECEDOR = 2 AND NE.ID_FORNECEDOR = 2
AND NFE.ID_LOJA = 1
AND NFE.CONFERIDO = TRUE
AND NFE.CARREGADO = TRUE
AND NE.ID_TIPOENTRADA != 3
AND NE.DATAEMISSAO BETWEEN '2025-06-01' AND '2025-06-16'
"""

cursor_pg.execute(query_xmls)
notas = cursor_pg.fetchall()
print(f"[INFO] {len(notas)} notas fiscais carregadas.")

count_inserts = 0

# Namespace do XML NFe
ns = {'ns': 'http://www.portalfiscal.inf.br/nfe'}

for numeronota, xml_str in notas:
    try:
        # Debug: print primeira nota para validar XML
        if count_inserts == 0:
            print(f"[DEBUG] Nota {numeronota} XML preview:\n{xml_str[:500]}")

        root = ET.fromstring(xml_str)

        # Encontra todos os elementos <cProd> considerando o namespace
        cprod_elements = root.findall('.//ns:cProd', ns)
        print(f"[DEBUG] Nota {numeronota} encontrou {len(cprod_elements)} cProd")

        for cprod in cprod_elements:
            codigo_externo = cprod.text.strip()
            cursor_sqlite.execute("""
                INSERT OR IGNORE INTO produtoscomprados (codigoexterno, codigointerno, descricao)
                VALUES (?, NULL, NULL)
            """, (codigo_externo,))
            count_inserts += 1
    except Exception as e:
        print(f"[ERRO] Nota {numeronota}: {e}")

conn_sqlite.commit()
print(f"[INFO] Inseridos {count_inserts} códigos externos no SQLite.")

cursor_sqlite.execute("SELECT COUNT(*) FROM produtoscomprados")
count_sqlite = cursor_sqlite.fetchone()[0]
print(f"[INFO] Total de registros na tabela produtoscomprados: {count_sqlite}")

# Etapa 2: Identificação dos códigos internos e descrições
cursor_sqlite.execute("SELECT codigoexterno FROM produtoscomprados WHERE codigointerno IS NULL")
codigo_externos = [row[0] for row in cursor_sqlite.fetchall()]

if not codigo_externos:
    print("[INFO] Todos os códigos já foram identificados ou tabela está vazia.")
else:
    print(f"[INFO] Identificando {len(codigo_externos)} códigos...")

    placeholders = ','.join(['%s'] * len(codigo_externos))

    # Query principal
    query_principal = f"""
    SELECT DISTINCT p.id AS codigointerno, p.descricaoreduzida, pf.codigoexterno
    FROM public.notaentrada ne
    JOIN public.notaentradaitem ni ON ne.id = ni.id_notaentrada
    JOIN public.produto p ON ni.id_produto = p.id
    JOIN public.produtofornecedor pf ON ni.id_produto = pf.id_produto
    WHERE pf.id_fornecedor = 2 
    AND ne.id_fornecedor = 2
    AND ne.id_tipoentrada != 3
    AND ne.dataemissao BETWEEN '2025-06-01' AND '2025-06-16'
    AND ne.id_loja = 1
    AND pf.codigoexterno IN ({placeholders})
    """

    cursor_pg.execute(query_principal, codigo_externos)
    for codint, descricao, codext in cursor_pg.fetchall():
        cursor_sqlite.execute("""
            UPDATE produtoscomprados 
            SET codigointerno = ?, descricao = ?
            WHERE codigoexterno = ?
        """, (codint, descricao, codext))

    # Query secundária
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
    AND ne.dataemissao BETWEEN '2025-06-01' AND '2025-06-16'
    AND ne.id_loja = 1
    AND pe.codigoexterno IN ({placeholders})
    """

    cursor_pg.execute(query_secundaria, codigo_externos)
    for codint, descricao, codext in cursor_pg.fetchall():
        cursor_sqlite.execute("""
            UPDATE produtoscomprados 
            SET codigointerno = ?, descricao = ?
            WHERE codigoexterno = ? AND codigointerno IS NULL
        """, (codint, descricao, codext))

    conn_sqlite.commit()

    # Etapa 3: Remover produtos com mercadologico1 = 16 da tabela produtoscomprados
    cursor_pg.execute("""
        SELECT p.id
        FROM public.produto p
        WHERE p.mercadologico1 = 16
    """)
    ids_mercadologico16 = [str(row[0]) for row in cursor_pg.fetchall()]

    if ids_mercadologico16:
        placeholders_sqlite = ','.join(['?'] * len(ids_mercadologico16))
        cursor_sqlite.execute(f"""
            DELETE FROM produtoscomprados
            WHERE codigointerno IN ({placeholders_sqlite})
        """, ids_mercadologico16)
        conn_sqlite.commit()
        print(f"[INFO] Removidos {len(ids_mercadologico16)} produtos com mercadologico1 = 16 da tabela produtoscomprados.")
    else:
        print("[INFO] Nenhum produto com mercadologico1 = 16 encontrado para exclusão.")

# Etapa 4: Verificar não identificados
cursor_sqlite.execute("SELECT codigoexterno FROM produtoscomprados WHERE codigointerno IS NULL")
nao_identificados = cursor_sqlite.fetchall()

if nao_identificados:
    print("\n[Códigos não identificados]:")
    for cod in nao_identificados:
        print(f" - {cod[0]}")
else:
    print("\n✅ Todos os códigos foram identificados com sucesso.")

# Finaliza conexões
cursor_pg.close()
conn_pg.close()
cursor_sqlite.close()
conn_sqlite.close()
