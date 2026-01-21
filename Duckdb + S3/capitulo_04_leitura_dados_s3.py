# -*- coding: utf-8 -*-
"""
capitulo_04_leitura_dados_s3
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import json

# ==============================================================================
# SETUP MINIO
# ==============================================================================
print(f"--- Iniciando Capítulo 04: Leitura de Dados S3 ---")

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

# Helper to upload data
def upload_data(filename, content, is_json=False):
    with open(filename, "w") as f:
        f.write(content)
    s3_client.upload_file(filename, BUCKET_NAME, filename)
    os.remove(filename)

# 1. Parquet Data (creating locally with pandas then uploading)
df = pd.DataFrame({
    'product_id': [1, 2, 3],
    'quantity': [10, 5, 2],
    'price': [100.0, 50.0, 1000.0],
    'date': ['2023-01-01', '2024-01-02', '2023-12-31']
})
df.to_parquet("sales.parquet")
s3_client.upload_file("sales.parquet", BUCKET_NAME, "sales.parquet")
os.remove("sales.parquet")

# 2. CSV Data
upload_data("data.csv", "id,name\n1,Alice\n2,Bob")

# 3. JSON Data
upload_data("data.json", '[{"id":1, "data":"info"}, {"id":2, "data":"more"}]')

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

print("\n--- Leitura Simples (Parquet) ---")
res = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/sales.parquet'").df()
print(res)

print("\n--- Leitura com Filtro ---")
res = con.execute(f"""
    SELECT product_id, price 
    FROM 's3://{BUCKET_NAME}/sales.parquet' 
    WHERE price > 50
""").df()
print(res)

print("\n--- Leitura CSV ---")
res = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/data.csv'").df()
print(res)

print("\n--- Leitura JSON ---")
res = con.execute(f"SELECT * FROM read_json_auto('s3://{BUCKET_NAME}/data.json')").df()
print(res)

print("\n--- Verificação de Schema ---")
con.execute(f"DESCRIBE SELECT * FROM 's3://{BUCKET_NAME}/sales.parquet'")
print(con.fetchall())


