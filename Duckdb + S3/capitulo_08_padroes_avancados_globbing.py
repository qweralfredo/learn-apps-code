# -*- coding: utf-8 -*-
"""
capitulo_08_padroes_avancados_globbing
"""

import duckdb
import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ==============================================================================
# SETUP MINIO
# ==============================================================================
print(f"--- Iniciando Capítulo 08: Padrões Avançados de Globbing ---")

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

def upload_df(key):
    df = pd.DataFrame({'id': [1], 'source': [key]})
    local_name = "temp_glob.parquet"
    df.to_parquet(local_name)
    s3_client.upload_file(local_name, BUCKET_NAME, key)
    os.remove(local_name)

# Create complex structure
PREFIX = "glob_data"
upload_df(f"{PREFIX}/server1/2024/01/log.parquet")
upload_df(f"{PREFIX}/server1/2024/02/log.parquet")
upload_df(f"{PREFIX}/server2/2024/01/log.parquet")
upload_df(f"{PREFIX}/server3/archive/old.parquet")
upload_df(f"{PREFIX}/misc/config.json")

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

print("\n--- 1. Recursive Globbing (**) ---")
res = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/{PREFIX}/**/*.parquet'").df()
print(res)

print("\n--- 2. Single Wildcard (*) ---")
# Server1, any year/month? (assuming deeper structure matches if we aim correctly)
# 'server1/*/*/log.parquet'
res = con.execute(f"SELECT * FROM 's3://{BUCKET_NAME}/{PREFIX}/server1/*/*/log.parquet'").df()
print(res)

print("\n--- 3. Using glob() function ---")
# List files instead of reading content
files = con.execute(f"SELECT file FROM glob('s3://{BUCKET_NAME}/{PREFIX}/**')").df()
print(files)

print("\n--- 4. List Check ---")
# Check filtering logic
count = con.execute(f"SELECT count(*) FROM 's3://{BUCKET_NAME}/{PREFIX}/**/log.parquet'").fetchone()[0]
print(f"Total 'log.parquet' files found: {count}")


