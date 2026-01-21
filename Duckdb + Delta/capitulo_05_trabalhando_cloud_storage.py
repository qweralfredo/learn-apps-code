# -*- coding: utf-8 -*-
"""
capitulo-05-trabalhando-cloud-storage
"""

# capitulo-05-trabalhando-cloud-storage
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# Exemplo/Bloco 2
import duckdb

def query_delta_on_s3():
    """
    Consultar tabela Delta no S3 com autenticação
    """
    con = duckdb.connect()

    # Carregar extensões
    con.execute("LOAD httpfs")
    con.execute("LOAD delta")

    # Configurar credenciais S3
    # Opção 1: Credenciais explícitas
    con.execute("""
        CREATE SECRET s3_secret (
            TYPE S3,
            KEY_ID 'YOUR_ACCESS_KEY_ID',
            SECRET 'YOUR_SECRET_ACCESS_KEY',
            REGION 'us-east-1'
        )
    """)

    # Consultar tabela Delta no S3
    result = con.execute("""
        SELECT
            region,
            COUNT(*) as order_count,
            SUM(total_amount) as revenue
        FROM delta_scan('s3://my-data-lake/delta/sales')
        WHERE order_date >= '2024-01-01'
        GROUP BY region
        ORDER BY revenue DESC
    """).fetchdf()

    print(result)
    con.close()

if __name__ == "__main__":
    query_delta_on_s3()

# Exemplo/Bloco 3
import duckdb
import os

# Garantir que credenciais AWS estão no ambiente
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (se aplicável)

con = duckdb.connect()
con.execute("LOAD httpfs")

# Usar credential chain
con.execute("""
    CREATE SECRET (
        TYPE S3,
        PROVIDER credential_chain
    )
""")

# Consultar
df = con.execute("""
    SELECT * FROM delta_scan('s3://my-bucket/delta-table')
    LIMIT 100
""").df()

print(df)

# Exemplo/Bloco 4
import duckdb
import os

def query_delta_on_azure():
    """
    Consultar tabela Delta no Azure Blob Storage
    """
    con = duckdb.connect()

    # Carregar extensões
    con.execute("LOAD httpfs")
    con.execute("LOAD delta")

    # Configurar autenticação Azure
    con.execute("""
        CREATE SECRET azure_secret (
            TYPE AZURE,
            ACCOUNT_NAME 'mystorageaccount',
            ACCOUNT_KEY 'myaccountkey'
        )
    """)

    # Ou usar credential chain
    # con.execute("CREATE SECRET (TYPE AZURE, PROVIDER credential_chain)")

    # Consultar tabela Delta
    result = con.execute("""
        SELECT
            DATE_TRUNC('month', order_date) as month,
            COUNT(*) as orders,
            SUM(total_amount) as revenue
        FROM delta_scan('az://datalake/delta/sales')
        WHERE order_date >= '2024-01-01'
        GROUP BY 1
        ORDER BY 1
    """).fetchdf()

    print(result)
    con.close()

if __name__ == "__main__":
    query_delta_on_azure()

# Exemplo/Bloco 5
import duckdb
import os

# Definir variável de ambiente com caminho para service account JSON
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/service-account.json'

con = duckdb.connect()
con.execute("LOAD httpfs")

con.execute("""
    CREATE SECRET (
        TYPE GCS,
        PROVIDER credential_chain
    )
""")

df = con.execute("""
    SELECT * FROM delta_scan('gs://my-bucket/sales')
    LIMIT 100
""").df()

# Exemplo/Bloco 6
import duckdb
from typing import Dict, List
import pandas as pd

class MultiCloudDeltaReader:
    """
    Classe para ler tabelas Delta de múltiplas clouds
    """

    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("LOAD httpfs")
        self.con.execute("LOAD delta")

    def setup_aws(self, access_key: str, secret_key: str, region: str = 'us-east-1'):
        """Configurar credenciais AWS"""
        self.con.execute(f"""
            CREATE OR REPLACE SECRET aws_secret (
                TYPE S3,
                KEY_ID '{access_key}',
                SECRET '{secret_key}',
                REGION '{region}'
            )
        """)

    def setup_azure(self, account_name: str, account_key: str):
        """Configurar credenciais Azure"""
        self.con.execute(f"""
            CREATE OR REPLACE SECRET azure_secret (
                TYPE AZURE,
                ACCOUNT_NAME '{account_name}',
                ACCOUNT_KEY '{account_key}'
            )
        """)

    def setup_gcs_credential_chain(self):
        """Configurar GCS via credential chain"""
        self.con.execute("""
            CREATE OR REPLACE SECRET gcs_secret (
                TYPE GCS,
                PROVIDER credential_chain
            )
        """)

    def query_delta_table(self, path: str, query: str = None) -> pd.DataFrame:
        """
        Consultar tabela Delta de qualquer cloud

        Args:
            path: Caminho completo (s3://, az://, gs://, ou local)
            query: Query SQL adicional (WHERE, ORDER BY, etc.)

        Returns:
            DataFrame com resultados
        """
        base_query = f"SELECT * FROM delta_scan('{path}')"

        if query:
            full_query = f"{base_query} {query}"
        else:
            full_query = base_query

        return self.con.execute(full_query).df()

    def aggregate_across_clouds(
        self,
        tables: Dict[str, str],
        metric_col: str = 'amount'
    ) -> pd.DataFrame:
        """
        Agregar métricas de tabelas em múltiplas clouds

        Args:
            tables: Dict com {nome: caminho}
            metric_col: Coluna para agregar

        Returns:
            DataFrame com agregações por fonte
        """
        union_queries = []

        for name, path in tables.items():
            union_queries.append(f"""
                SELECT
                    '{name}' as source,
                    COUNT(*) as record_count,
                    SUM({metric_col}) as total_{metric_col},
                    AVG({metric_col}) as avg_{metric_col}
                FROM delta_scan('{path}')
            """)

        full_query = " UNION ALL ".join(union_queries)
        return self.con.execute(full_query).df()

    def join_cross_cloud(
        self,
        left_table: str,
        right_table: str,
        join_key: str
    ) -> pd.DataFrame:
        """
        JOIN entre tabelas em diferentes clouds
        """
        query = f"""
            SELECT
                l.*,
                r.*
            FROM delta_scan('{left_table}') l
            JOIN delta_scan('{right_table}') r
                ON l.{join_key} = r.{join_key}
        """
        return self.con.execute(query).df()

    def close(self):
        """Fechar conexão"""
        self.con.close()


# Exemplo de uso
if __name__ == "__main__":
    reader = MultiCloudDeltaReader()

    # Configurar credenciais
    reader.setup_aws(
        access_key='YOUR_AWS_KEY',
        secret_key='YOUR_AWS_SECRET',
        region='us-east-1'
    )

    reader.setup_azure(
        account_name='yourazureaccount',
        account_key='yourazurekey'
    )

    # Query em S3
    aws_data = reader.query_delta_table(
        's3://my-bucket/sales',
        "WHERE order_date >= '2024-01-01' LIMIT 100"
    )
    print("AWS Data:")
    print(aws_data.head())

    # Query em Azure
    azure_data = reader.query_delta_table(
        'az://my-container/sales',
        "WHERE region = 'EMEA' LIMIT 100"
    )
    print("\nAzure Data:")
    print(azure_data.head())

    # Agregação cross-cloud
    aggregated = reader.aggregate_across_clouds({
        'AWS_US': 's3://my-bucket/sales',
        'Azure_EU': 'az://my-container/sales'
    })
    print("\nCross-cloud Aggregation:")
    print(aggregated)

    reader.close()

# Exemplo/Bloco 7
# Verificar configurações
con = duckdb.connect()
con.execute("LOAD httpfs")

# Listar secrets configurados
result = con.execute("SELECT * FROM duckdb_secrets()").df()
print(result)

# Testar conectividade básica
try:
    con.execute("SELECT * FROM delta_scan('s3://bucket/table') LIMIT 1")
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")

# Exemplo/Bloco 8
# Habilitar logging para debug
import duckdb

con = duckdb.connect()
con.execute("SET enable_progress_bar=true")
con.execute("SET enable_profiling=true")
con.execute("SET profiling_mode='detailed'")

# Executar query
result = con.execute("SELECT * FROM delta_scan('s3://bucket/table')").df()

# Ver profile
profile = con.execute("SELECT * FROM pragma_last_profile_results()").df()
print(profile)

