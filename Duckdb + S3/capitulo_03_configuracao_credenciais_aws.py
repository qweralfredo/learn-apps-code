# -*- coding: utf-8 -*-
"""
capitulo_03_configuracao_credenciais_aws
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError

# ==============================================================================
# SETUP MINIO
# ==============================================================================
print(f"--- Iniciando Capítulo 03: Configuração de Credenciais ---")

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "learn-duckdb-s3"

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)
try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
except ClientError:
    pass

# Helper to write verify file
with open("creds.csv", "w") as f:
    f.write("id,msg\n1,success")
s3_client.upload_file("creds.csv", BUCKET_NAME, "creds.csv")
os.remove("creds.csv")

# ==============================================================================
# EXEMPLOS
# ==============================================================================
con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

print("\n--- Método 1: CREATE SECRET (Explícito) ---")
con.execute(f"""
CREATE OR REPLACE SECRET secret_explicit (
    TYPE S3,
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    REGION 'us-east-1',
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    URL_STYLE 'path',
    USE_SSL 'false'
);
""")
res = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/creds.csv'").fetchall()
print(f"Leitura com Secret Explícito: {res}")

print("\n--- Método 2: Variáveis de Ambiente + Credential Chain ---")
# Limpar secrets
con.execute("DROP SECRET IF EXISTS secret_explicit")

# Definir Env Vars
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
# Para MinIO precisamos especificar o endpoint, que normalmente não é pego auto pela env var aws padrão pelo duckdb S3 a menos que usemos S3_ENDPOINT, mas o DuckDB usa DUCKDB_... ou settings.
# O provider CREDENTIAL_CHAIN lê AWS_... mas não endpoint customizado facilmente sem configurar secret de qualquer forma.
# Entao vamos criar um secret que usa o PROVIDER 'env' ou 'credential_chain' mas subscreve o endpoint.

con.execute(f"""
CREATE OR REPLACE SECRET secret_from_env (
    TYPE S3,
    PROVIDER CREDENTIAL_CHAIN,
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    URL_STYLE 'path',
    USE_SSL 'false'
);
""")

res = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/creds.csv'").fetchall()
print(f"Leitura com Env Vars: {res}")

print("\n--- Método 3: Profile (Simulado via boto3 config logic) ---")
# O provider 'config' tentaria ler ~/.aws/config. Não vamos modificar o home do usuário.
# Mas a sintaxe seria:
print("Exemplo de sintaxe (não executado):")
print("""
CREATE SECRET secret_profile (
    TYPE S3,
    PROVIDER CONFIG,
    PROFILE 'my-minio-profile'
);
""")


