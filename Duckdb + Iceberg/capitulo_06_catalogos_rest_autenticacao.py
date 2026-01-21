# -*- coding: utf-8 -*-
"""
capitulo_06_catalogos_rest_autenticacao
"""

# capitulo_06_catalogos_rest_autenticacao
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

# Criar secret
con.execute("""
    CREATE SECRET my_iceberg_secret (
        TYPE iceberg,
        CLIENT_ID 'my_client_id',
        CLIENT_SECRET 'my_client_secret',
        OAUTH2_SERVER_URI 'https://catalog.example.com/v1/oauth/tokens'
    )
""")

# Anexar catálogo
con.execute("""
    ATTACH 'my_warehouse' AS iceberg_cat (
        TYPE iceberg,
        SECRET my_iceberg_secret,
        ENDPOINT 'https://catalog.example.com'
    )
""")

# Consultar tabelas
tables = con.execute("""
    SELECT * FROM iceberg_cat.information_schema.tables
""").df()

print(tables)

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

# Configurar credenciais AWS
con.execute("""
    CREATE SECRET aws_secret (
        TYPE s3,
        PROVIDER credential_chain
    )
""")

# Anexar Glue Catalog
con.execute("""
    CREATE SECRET glue_secret (
        TYPE iceberg,
        CLIENT_ID 'aws_access_key',
        CLIENT_SECRET 'aws_secret_key',
        OAUTH2_SERVER_URI 'https://glue.us-east-1.amazonaws.com'
    )
""")

con.execute("""
    ATTACH 'glue_warehouse' AS glue_catalog (
        TYPE iceberg,
        SECRET glue_secret,
        ENDPOINT 'https://glue.us-east-1.amazonaws.com'
    )
""")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# ... anexar catálogo ...

# Listar todas as tabelas
tables = con.execute("""
    SELECT table_catalog, table_schema, table_name
    FROM iceberg_cat.information_schema.tables
    WHERE table_type = 'BASE TABLE'
""").df()

print("Tabelas disponíveis:")
print(tables)

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()

# ... configurar catálogo ...

# Consultar tabela via catálogo
result = con.execute("""
    SELECT *
    FROM iceberg_catalog.default.sales
    WHERE order_date >= '2024-01-01'
    LIMIT 1000
""").df()

print(result.head())

# Exemplo/Bloco 5
import duckdb
import os

# Ler credenciais de variáveis de ambiente
client_id = os.getenv('ICEBERG_CLIENT_ID')
client_secret = os.getenv('ICEBERG_CLIENT_SECRET')
oauth_uri = os.getenv('ICEBERG_OAUTH_URI')

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# Criar secret com variáveis de ambiente
con.execute(f"""
    CREATE SECRET env_secret (
        TYPE iceberg,
        CLIENT_ID '{client_id}',
        CLIENT_SECRET '{client_secret}',
        OAUTH2_SERVER_URI '{oauth_uri}'
    )
""")

# Exemplo/Bloco 6
import duckdb
import os

class IcebergCatalogManager:
    def __init__(self, endpoint, client_id, client_secret, oauth_uri):
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext
        self.con.execute("LOAD httpfs")

        # Criar secret
        self.con.execute(f"""
            CREATE SECRET catalog_secret (
                TYPE iceberg,
                CLIENT_ID '{client_id}',
                CLIENT_SECRET '{client_secret}',
                OAUTH2_SERVER_URI '{oauth_uri}'
            )
        """)

        # Anexar catálogo
        self.con.execute(f"""
            ATTACH 'warehouse' AS catalog (
                TYPE iceberg,
                SECRET catalog_secret,
                ENDPOINT '{endpoint}'
            )
        """)

    def list_tables(self, schema='default'):
        """Lista tabelas em um schema"""
        return self.con.execute(f"""
            SELECT table_name
            FROM catalog.information_schema.tables
            WHERE table_schema = '{schema}'
        """).df()

    def query_table(self, table_path, limit=100):
        """Consulta tabela do catálogo"""
        return self.con.execute(f"""
            SELECT *
            FROM {table_path}
            LIMIT {limit}
        """).df()

# Usar
catalog = IcebergCatalogManager(
    endpoint='https://catalog.example.com',
    client_id=os.getenv('CLIENT_ID'),
    client_secret=os.getenv('CLIENT_SECRET'),
    oauth_uri='https://catalog.example.com/v1/oauth/tokens'
)

# Listar tabelas
tables = catalog.list_tables('sales')
print("Tabelas:", tables)

# Consultar
data = catalog.query_table('catalog.sales.orders')
print(data.head())

# Exemplo/Bloco 7
import duckdb
import requests

def test_catalog_connection(endpoint, token=None):
    """Testa conectividade com catálogo REST"""
    headers = {}
    if token:
        headers['Authorization'] = f'Bearer {token}'

    try:
        response = requests.get(f"{endpoint}/v1/config", headers=headers)
        print(f"✅ Catálogo acessível: {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ Erro de conectividade: {e}")
        return False

# Testar
test_catalog_connection('https://catalog.example.com')

# Exemplo/Bloco 8
import duckdb

import importlib.util


def has_module(name):
    return importlib.util.find_spec(name) is not None

def safe_install_ext(con, ext_name):
    try:
        con.execute(f"INSTALL {ext_name}")
        con.execute(f"LOAD {ext_name}")
        return True
    except Exception as e:
        print(f"Warning: Failed to install/load {ext_name} extension: {e}")
        return False


con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

try:
    con.execute("""
        CREATE SECRET test_secret (
            TYPE iceberg,
            CLIENT_ID 'test_id',
            CLIENT_SECRET 'test_secret',
            OAUTH2_SERVER_URI 'https://catalog.example.com/oauth/tokens'
        )
    """)
    print("✅ Secret criado com sucesso")

    con.execute("""
        ATTACH 'test_warehouse' AS test_cat (
            TYPE iceberg,
            SECRET test_secret,
            ENDPOINT 'https://catalog.example.com'
        )
    """)
    print("✅ Catálogo anexado com sucesso")

except Exception as e:
    print(f"❌ Erro: {e}")
