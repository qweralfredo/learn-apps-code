# -*- coding: utf-8 -*-
"""
Capítulo 01: Introdução DuckDB e S3
"""

import duckdb
import pandas as pd
import boto3
import os
from botocore.exceptions import ClientError

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

s3_client.upload_file(sample_csv, BUCKET_NAME, "data/vendas.csv")
print(f"Arquivo '{sample_csv}' enviado para 's3://{BUCKET_NAME}/data/vendas.csv'.")
os.remove(sample_csv)

# ==============================================================================
# DUCKDB S3 CONFIGURATION
# ==============================================================================

con = duckdb.connect(database=':memory:')

# Install and Load httpfs
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configure Secrets/Access
con.execute(f"""
    CREATE SECRET secret1 (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        REGION 'us-east-1',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")
print("DuckDB configurado com credenciais S3 (MinIO).")

# ==============================================================================
# QUERIES
# ==============================================================================

print("\n>>> Executando: Leitura do S3")
try:
    con.sql(f"SELECT * FROM 's3://{BUCKET_NAME}/data/vendas.csv'").show()
except Exception as e:
    print(f"Erro ao ler do S3: {e}")

print("\n--- Capítulo concluído com sucesso ---")













