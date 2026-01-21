# -*- coding: utf-8 -*-
"""
capitulo_01_introducao_duckdb_s3
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd

# ==============================================================================
# SETUP MINIO (SIMULATING S3)
# ==============================================================================
print(f"--- Iniciando Capítulo 01: Introdução DuckDB e S3 ---")

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "learn-duckdb-s3"

# Setup Boto3 client to manage MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

# Create Bucket if not exists
try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
    print(f"Bucket '{BUCKET_NAME}' criado/verificado no MinIO.")
except ClientError as e:
    print(f"Erro ao criar bucket: {e}")

# Upload sample data
sample_csv = "vendas_s3.csv"
with open(sample_csv, "w") as f:
    f.write("id,produto,valor\n1,Notebook,3500\n2,Mouse,50")

try:
    s3_client.upload_file(sample_csv, BUCKET_NAME, "vendas.csv")
    print(f"Arquivo 'vendas.csv' enviado para o bucket '{BUCKET_NAME}'.")
finally:
    if os.path.exists(sample_csv):
        os.remove(sample_csv)

# ==============================================================================
# EXEMPLOS DUCKDB
# ==============================================================================

con = duckdb.connect(database=':memory:')

# Instalar e carregar extensão HTTPFS para acesso ao S3
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configurar credenciais (Segredo)
con.execute(f"""
CREATE SECRET secret_minio (
    TYPE S3,
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    REGION 'us-east-1',
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    URL_STYLE 'path',
    USE_SSL 'false'
);
""")

print("\n--- Consultando arquivo no S3 (MinIO) ---")
# Query direta no arquivo S3
result = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/vendas.csv'").df()
print(result)

print("\n--- Copiando tabela para S3 (Parquet) ---")
# Criar tabela local
con.execute("CREATE TABLE produtos AS SELECT * FROM 's3://{}/vendas.csv'".format(BUCKET_NAME))

# Exportar para Parquet no S3
con.execute(f"""
COPY produtos TO 's3://{BUCKET_NAME}/produtos_backup.parquet' (FORMAT PARQUET);
""")
print("Exportação concluída.")

print("\n--- Lendo Parquet do S3 ---")
result_parquet = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/produtos_backup.parquet'").df()
print(result_parquet)

print("\n--- Globbing (Listando arquivos) ---")
# Listar arquivos no bucket
files = con.execute(f"SELECT * FROM glob('s3://{BUCKET_NAME}/*')").df()
print(files)


