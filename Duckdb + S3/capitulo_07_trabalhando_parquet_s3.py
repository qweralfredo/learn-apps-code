# -*- coding: utf-8 -*-
"""
capitulo_07_trabalhando_parquet_s3
"""

import duckdb
import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ==============================================================================
# SETUP MINIO
# ==============================================================================
print(f"--- Iniciando Capítulo 07: Trabalhando com Parquet no S3 ---")

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

# Helper
def upload_df(df, key):
    local_name = "temp.parquet"
    df.to_parquet(local_name)
    s3_client.upload_file(local_name, BUCKET_NAME, key)
    os.remove(local_name)

# Create Partitioned Data
# year=2023/month=01/file.parquet
df1 = pd.DataFrame({'id': [1, 2], 'val': [100, 200]})
upload_df(df1, "data_ch07/year=2023/month=01/part1.parquet")

# year=2023/month=02/file.parquet
df2 = pd.DataFrame({'id': [3], 'val': [300]})
upload_df(df2, "data_ch07/year=2023/month=02/part1.parquet")

# year=2024/month=01/file.parquet
df3 = pd.DataFrame({'id': [4, 5], 'val': [400, 500]})
upload_df(df3, "data_ch07/year=2024/month=01/part1.parquet")


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

print("\n--- 1. Globbing de Arquivos ---")
# Query All
res = con.execute(f"SELECT count(*) FROM 's3://{BUCKET_NAME}/data_ch07/**/*.parquet'").fetchall()
print(f"Total rows: {res[0][0]}")

print("\n--- 2. Hive Partitioning (Auto Discovery) ---")
# DuckDB consegue inferir year e month como colunas
res = con.execute(f"""
    SELECT id, val, year, month 
    FROM read_parquet('s3://{BUCKET_NAME}/data_ch07/**/*.parquet', hive_partitioning=1)
    ORDER BY id
""").df()
print(res)

print("\n--- 3. Filename Metadata ---")
# Saber qual arquivo originou o dado
res = con.execute(f"""
    SELECT id, filename 
    FROM read_parquet('s3://{BUCKET_NAME}/data_ch07/**/*.parquet', filename=true)
    LIMIT 2
""").df()
print(res)

print("\n--- 4. Filter Pushdown (Explicado) ---")
print("DuckDB usa os filtros na query para pular partições inteiras.")
query = f"SELECT count(*) FROM read_parquet('s3://{BUCKET_NAME}/data_ch07/**/*.parquet', hive_partitioning=1) WHERE year='2024'"
explain = con.execute(f"EXPLAIN {query}").fetchall()
# Printar explain é muito verborrágico, mas podemos confirmar que executa
print("Query com filtro executada.")
count_2024 = con.execute(query).fetchone()[0]
print(f"Linhas em 2024: {count_2024}")


