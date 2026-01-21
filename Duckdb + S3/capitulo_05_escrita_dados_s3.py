# -*- coding: utf-8 -*-
"""
capitulo_05_escrita_dados_s3
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError

# ==============================================================================
# SETUP MINIO
# ==============================================================================
print(f"--- Iniciando Capítulo 05: Escrita de Dados S3 ---")

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

# ==============================================================================
# EXEMPLOS DUCKDB
# ==============================================================================
con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")
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

# 1. Criar dados locais
print("\n--- Gerando dados locais ---")
con.execute("""
CREATE TABLE vendas AS
SELECT
    range as id,
    'Produto_' || (range % 5) as produto,
    (random() * 100)::DECIMAL(10,2) as valor
FROM range(10);
""")
print(con.execute("SELECT * FROM vendas LIMIT 5").df())

# 2. Escrever Parquet
print("\n--- Escrevendo Parquet no S3 ---")
con.execute(f"COPY vendas TO 's3://{BUCKET_NAME}/output_vendas.parquet' (FORMAT PARQUET)")
print("Arquivo parquet escrito com sucesso.")

# 3. Escrever CSV (com opções)
print("\n--- Escrevendo CSV no S3 ---")
con.execute(f"""
COPY vendas TO 's3://{BUCKET_NAME}/output_vendas.csv' 
(FORMAT CSV, DELIMITER ';', HEADER TRUE)
""")
print("Arquivo CSV escrito com sucesso.")

# 4. Escrever JSON
print("\n--- Escrevendo JSON no S3 ---")
con.execute(f"""
COPY vendas TO 's3://{BUCKET_NAME}/output_vendas.json' 
(FORMAT JSON)
""")
print("Arquivo JSON escrito com sucesso.")

# 5. Escrever particionado (Hive Partitioning)
print("\n--- Escrevendo Particionado (Hive) ---")
con.execute(f"""
COPY vendas TO 's3://{BUCKET_NAME}/vendas_partitioned' 
(FORMAT PARQUET, PARTITION_BY (produto), OVERWRITE_OR_IGNORE)
""")
print("Dados particionados escritos.")

print("\n--- Verificando Arquivos Criados ---")
files = con.execute(f"SELECT file FROM glob('s3://{BUCKET_NAME}/**')").df()
print(files)


