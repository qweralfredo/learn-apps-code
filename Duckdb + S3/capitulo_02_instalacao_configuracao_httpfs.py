# -*- coding: utf-8 -*-
"""
capitulo_02_instalacao_configuracao_httpfs
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError

# ==============================================================================
# SETUP MINIO (SIMULATING S3)
# ==============================================================================
print(f"--- Iniciando Capítulo 02: Instalação e Configuração HTTPFS ---")

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "learn-duckdb-s3"

# Setup Boto3 client
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

# Create a sample file
with open("test.csv", "w") as f:
    f.write("col1,col2\n10,20")
s3_client.upload_file("test.csv", BUCKET_NAME, "test.csv")
os.remove("test.csv")

# ==============================================================================
# EXEMPLOS DUCKDB
# ==============================================================================

con = duckdb.connect(database=':memory:')

print("\n--- Instalando HTTPFS ---")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
print("Extensão httpfs carregada.")

print("\n--- Verificando Extensões Instaladas ---")
extensions = con.execute("SELECT extension_name, installed, loaded FROM duckdb_extensions() WHERE extension_name='httpfs'").df()
print(extensions)

print("\n--- Configurando Credenciais S3 (MinIO) via SECRET ---")
con.execute(f"""
CREATE OR REPLACE SECRET secret_minio_config (
    TYPE S3,
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    REGION 'us-east-1',
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    URL_STYLE 'path',
    USE_SSL 'false'
);
""")
# Nota: SET s3_... é a forma legada/global. Secrets são escopados e preferidos.

print("\n--- Testando Leitura ---")
result = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/test.csv'").df()
print(result)

print("\n--- Teste de Autoload (Simulado) ---")
# DuckDB tenta carregar httpfs automaticamente se detectar s3:// ou https://
# Como já carregamos, apenas validamos que funciona
try:
    count = con.execute(f"SELECT count(*) FROM 's3://{BUCKET_NAME}/test.csv'").fetchone()
    print(f"Contagem de linhas: {count[0]}")
except Exception as e:
    print(f"Erro: {e}")


