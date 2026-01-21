# -*- coding: utf-8 -*-
"""
capitulo_06_secrets_autenticacao
"""

import duckdb
import os
import boto3
import shutil
import pandas as pd
from datetime import datetime

# Helper injected
def safe_install_ext(con, ext_name):
    try:
        con.install_extension(ext_name)
        con.load_extension(ext_name)
        print(f"Extension '{ext_name}' loaded.")
        return True
    except Exception as e:
        print(f"Could not load extension '{ext_name}': {e}")
        return False

print("--- Iniciando Capítulo 06: Secrets e Autenticação ---")

# ==============================================================================
# SETUP Simulation (MinIO)
# ==============================================================================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "secure-bucket"

# Mock ENV vars for credential chain test
os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
os.environ['AWS_REGION'] = 'us-east-1'
os.environ['AWS_ENDPOINT_URL'] = MINIO_ENDPOINT # Botocore/DuckDB might respect this
os.environ['DUCKDB_NO_SSL'] = "true" # Custom hint, not standard duckdb env var, but good for context

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
except:
    pass

# Create dummy data in bucket
df = pd.DataFrame({'id': [1,2,3], 'secret_data': ['A', 'B', 'C']})
df.to_parquet("secret.parquet")
s3_client.upload_file("secret.parquet", BUCKET_NAME, "table/data.parquet")
os.remove("secret.parquet")

# ==============================================================================
# DUCKDB
# ==============================================================================
con = duckdb.connect()
safe_install_ext(con, "httpfs")
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet")

print("\n--- 1. Auth via CREATE SECRET (Config explicit) ---")
# Explicitamente definindo credenciais no segredo. 
# Funciona para MinIO se passarmos ENDPOINT.
con.execute(f"""
    CREATE OR REPLACE SECRET minio_explicit (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        REGION 'us-east-1',
        ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
        USE_SSL 'false',
        URL_STYLE 'path'
    )
""")
print("Secret 'minio_explicit' criado.")

try:
    # Testing read
    count = con.execute(f"SELECT count(*) FROM 's3://{BUCKET_NAME}/table/data.parquet'").fetchone()
    print(f"Leitura com secret explícito: {count[0]} linhas.")
except Exception as e:
    print(f"Erro leitura explícita: {e}")


print("\n--- 2. Auth via Credential Chain (Env Vars) ---")
# Credential Chain lê do ambiente (AWS_ACCESS_KEY_ID etc).
# Porem, para MinIO, precisamos injectar o ENDPOINT.
# CREATE SECRET ... PROVIDER credential_chain ... ENDPOINT '...'
# Nota: Algumas versões do DuckDB permitem misturar chain + params estáticos.

try:
    con.execute(f"""
        CREATE OR REPLACE SECRET minio_chain (
            TYPE S3,
            PROVIDER credential_chain,
            ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
            USE_SSL 'false',
            URL_STYLE 'path'
        )
    """)
    print("Secret 'minio_chain' criado.")
    
    count = con.execute(f"SELECT count(*) FROM 's3://{BUCKET_NAME}/table/data.parquet'").fetchone()
    print(f"Leitura com credential chain + endpoint: {count[0]} linhas.")
except Exception as e:
    print(f"Erro leitura chain: {e}")

print("\n--- 3. GCS / Azure (Exemplos Conceituais) ---")
# Apenas mostrando sintaxe, pois não temos emulador GCS/Azure rodando e DuckDB valida conexao as vezes.
print("Exemplo GCS (não executado):")
print("""
    CREATE SECRET gcs_secret (
        TYPE GCS,
        KEY_ID '...',
        SECRET '...'
    );
""")

print("--- Fim do Capítulo 06 ---")
