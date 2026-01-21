# -*- coding: utf-8 -*-
"""
capitulo-06-secrets-autenticacao
"""

# capitulo-06-secrets-autenticacao
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
import os

# Definir credenciais via variáveis de ambiente
os.environ['AWS_ACCESS_KEY_ID'] = 'your_access_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'your_secret_key'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
con.execute("LOAD httpfs")

# Usar credential chain
con.execute("""
    CREATE SECRET (
        TYPE S3,
        PROVIDER credential_chain
    )
""")

# Acessar S3
df = con.execute("SELECT * FROM delta_scan('s3://bucket/table')").df()

# Exemplo/Bloco 2
import duckdb
import os

# Definir caminho para service account
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/key.json'

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
con.execute("LOAD httpfs")

con.execute("""
    CREATE SECRET (
        TYPE GCS,
        PROVIDER credential_chain
    )
""")

df = con.execute("SELECT * FROM delta_scan('gs://bucket/table')").df()

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Listar secrets
secrets = con.execute("SELECT * FROM duckdb_secrets()").df()
print(secrets)

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
con.execute("CREATE SECRET (TYPE S3, PROVIDER credential_chain)")

# Secret disponível
con.execute("SELECT * FROM delta_scan('s3://bucket/table')")

con.close()

# Nova conexão - secret não existe mais
con2 = duckdb.connect()
# Precisa recriar o secret

# Exemplo/Bloco 5
import duckdb

# Conectar a arquivo
con = duckdb.connect('mydb.duckdb')

# Criar secret persistente
con.execute("""
    CREATE PERSISTENT SECRET aws_prod (
        TYPE S3,
        KEY_ID 'key',
        SECRET 'secret'
    )
""")

con.close()

# Nova sessão - secret ainda existe
con2 = duckdb.connect('mydb.duckdb')
result = con2.execute("SELECT * FROM duckdb_secrets()").df()
print(result)  # aws_prod está lá

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

try:
    # Criar secret temporário
    con.execute("""
        CREATE TEMPORARY SECRET temp_s3 (
            TYPE S3,
            KEY_ID 'temp_key',
            SECRET 'temp_secret'
        )
    """)

    # Usar secret
    df = con.execute("SELECT * FROM delta_scan('s3://bucket/table')").df()

finally:
    # Secret é automaticamente removido ao fechar conexão
    con.close()

# Exemplo/Bloco 7
import duckdb
from datetime import datetime, timedelta

def rotate_secret_if_needed(con, secret_name, last_rotation):
    """Rotacionar secret se passou X dias"""
    days_since_rotation = (datetime.now() - last_rotation).days

    if days_since_rotation > 90:  # Rotacionar a cada 90 dias
        # Buscar novas credenciais de um cofre seguro
        new_key, new_secret = fetch_new_credentials()

        # Atualizar secret
        con.execute(f"""
            CREATE OR REPLACE SECRET {secret_name} (
                TYPE S3,
                KEY_ID '{new_key}',
                SECRET '{new_secret}'
            )
        """)

        print(f"Secret {secret_name} rotacionado com sucesso")
        return datetime.now()

    return last_rotation

# Exemplo/Bloco 8
import duckdb
import os
from typing import Optional

def create_s3_secret_from_env(
    con: duckdb.DuckDBPyConnection,
    secret_name: Optional[str] = None,
    region: str = 'us-east-1'
):
    """
    Criar secret S3 a partir de variáveis de ambiente
    """
    key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    session_token = os.environ.get('AWS_SESSION_TOKEN')

    if not key_id or not secret_key:
        raise ValueError("AWS credentials not found in environment")

    secret_clause = f"CREATE SECRET {secret_name}" if secret_name else "CREATE SECRET"

    params = f"""
        TYPE S3,
        KEY_ID '{key_id}',
        SECRET '{secret_key}',
        REGION '{region}'
    """

    if session_token:
        params += f",\n        SESSION_TOKEN '{session_token}'"

    con.execute(f"{secret_clause} ({params})")
    print(f"[OK] S3 secret created from environment variables")

# Uso
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
con.execute("LOAD httpfs")
create_s3_secret_from_env(con, 'aws_prod')

# Exemplo/Bloco 9
# [X] Ruim - Hardcoded
con.execute("""
    CREATE SECRET (
        TYPE S3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',  # Nunca faça isso!
        SECRET 'wJalrXUtnFEMI/K7MDENG'  # Nunca faça isso!
    )
""")

# [OK] Bom - Usa variáveis de ambiente
import os
con.execute(f"""
    CREATE SECRET (
        TYPE S3,
        KEY_ID '{os.environ['AWS_ACCESS_KEY_ID']}',
        SECRET '{os.environ['AWS_SECRET_ACCESS_KEY']}'
    )
""")

# [OK] Melhor - Usa credential chain
con.execute("CREATE SECRET (TYPE S3, PROVIDER credential_chain)")

# Exemplo/Bloco 10
import duckdb
import os
from typing import Dict, Optional, List
from dataclasses import dataclass

@dataclass
class SecretConfig:
    """Configuração de um secret"""
    name: str
    type: str
    provider: Optional[str] = None
    params: Optional[Dict] = None
    scope: Optional[str] = None

class SecretManager:
    """Gerenciador de secrets do DuckDB"""

    def __init__(self, db_path: str = ':memory:'):
        self.con = duckdb.connect(db_path)
        self.con.execute("LOAD httpfs")

    def create_s3_secret(
        self,
        name: Optional[str] = None,
        use_credential_chain: bool = True,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = 'us-east-1',
        scope: Optional[str] = None
    ):
        """Criar secret S3"""
        secret_clause = f"CREATE SECRET {name}" if name else "CREATE SECRET"

        if use_credential_chain:
            params = "TYPE S3, PROVIDER credential_chain"
        else:
            if not access_key or not secret_key:
                raise ValueError("Access key and secret key required")
            params = f"TYPE S3, KEY_ID '{access_key}', SECRET '{secret_key}', REGION '{region}'"

        if scope:
            params += f", SCOPE '{scope}'"

        self.con.execute(f"{secret_clause} ({params})")
        print(f"[OK] S3 secret created: {name or 'anonymous'}")

    def create_azure_secret(
        self,
        name: Optional[str] = None,
        account_name: Optional[str] = None,
        account_key: Optional[str] = None,
        connection_string: Optional[str] = None,
        use_credential_chain: bool = False
    ):
        """Criar secret Azure"""
        secret_clause = f"CREATE SECRET {name}" if name else "CREATE SECRET"

        if use_credential_chain:
            params = "TYPE AZURE, PROVIDER credential_chain"
        elif connection_string:
            params = f"TYPE AZURE, CONNECTION_STRING '{connection_string}'"
        elif account_name and account_key:
            params = f"TYPE AZURE, ACCOUNT_NAME '{account_name}', ACCOUNT_KEY '{account_key}'"
        else:
            raise ValueError("Must provide credentials or use credential chain")

        self.con.execute(f"{secret_clause} ({params})")
        print(f"[OK] Azure secret created: {name or 'anonymous'}")

    def list_secrets(self) -> List[Dict]:
        """Listar todos os secrets"""
        return self.con.execute("""
            SELECT name, type, provider, scope
            FROM duckdb_secrets()
        """).df().to_dict('records')

    def drop_secret(self, name: str):
        """Remover secret por nome"""
        self.con.execute(f"DROP SECRET IF EXISTS {name}")
        print(f"[OK] Secret removed: {name}")

    def test_s3_connection(self, bucket: str, path: str = '') -> bool:
        """Testar conexão S3"""
        try:
            test_path = f"s3://{bucket}/{path}" if path else f"s3://{bucket}"
            self.con.execute(f"SELECT * FROM delta_scan('{test_path}') LIMIT 1")
            print(f"[OK] S3 connection successful: {test_path}")
            return True
        except Exception as e:
            print(f"✗ S3 connection failed: {e}")
            return False

    def close(self):
        """Fechar conexão"""
        self.con.close()


# Exemplo de uso
if __name__ == "__main__":
    # Criar manager
    manager = SecretManager()

    # Criar secrets
    manager.create_s3_secret(
        name='prod_aws',
        use_credential_chain=True,
        region='us-east-1',
        scope='s3://production-*'
    )

    manager.create_s3_secret(
        name='dev_aws',
        use_credential_chain=True,
        region='us-west-2',
        scope='s3://development-*'
    )

    # Listar secrets
    secrets = manager.list_secrets()
    print("\nConfigured secrets:")
    for secret in secrets:
        print(f"  - {secret['name']}: {secret['type']} ({secret.get('scope', 'global')})")

    # Testar conexão
    manager.test_s3_connection('my-bucket', 'delta-tables/sales')

    manager.close()

