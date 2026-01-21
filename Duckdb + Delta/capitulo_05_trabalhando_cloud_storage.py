# -*- coding: utf-8 -*-
"""
capitulo_05_trabalhando_cloud_storage
"""

import duckdb
import os
import boto3
import shutil
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError

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

print("--- Iniciando Capítulo 05: Trabalhando com Cloud Storage (S3) ---")

# ==============================================================================
# SETUP Simulation (MinIO)
# ==============================================================================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "my-data-lake"

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

print("Configurando MinIO...")
try:
    s3_client.create_bucket(Bucket=BUCKET_NAME)
except ClientError:
    pass

# Generate Local Delta/Parquet and Upload
LOCAL_PATH = "./sales_temp"
if os.path.exists(LOCAL_PATH):
    shutil.rmtree(LOCAL_PATH)

try:
    from deltalake import write_deltalake
    df_sales = pd.DataFrame({
        'order_id': range(50),
        'region': ['US', 'EU'] * 25,
        'total_amount': [100.0] * 50,
        'order_date': [datetime.now()] * 50
    })
    write_deltalake(LOCAL_PATH, df_sales)
    print("Local Delta table created.")
except ImportError:
    print("Deltalake not installed. Creating Parquet structure.")
    os.makedirs(LOCAL_PATH, exist_ok=True)
    df_sales = pd.DataFrame({
        'order_id': range(50),
        'region': ['US', 'EU'] * 25,
        'total_amount': [100.0] * 50,
        'order_date': [datetime.now()] * 50
    })
    df_sales.to_parquet(os.path.join(LOCAL_PATH, "part-001.parquet"))

# Upload to MinIO
print("Fazendo upload para MinIO...")
for root, dirs, files in os.walk(LOCAL_PATH):
    for file in files:
        local_file = os.path.join(root, file)
        # s3://my-data-lake/delta/sales/...
        rel_path = os.path.relpath(local_file, LOCAL_PATH)
        s3_key = f"delta/sales/{rel_path}".replace("\\", "/") # Windows fix
        s3_client.upload_file(local_file, BUCKET_NAME, s3_key)

if os.path.exists(LOCAL_PATH):
    shutil.rmtree(LOCAL_PATH)

# ==============================================================================
# DUCKDB
# ==============================================================================
con = duckdb.connect()
safe_install_ext(con, "httpfs")
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet")

# Configuring Secret for MinIO (pretending to be S3)
con.execute(f"""
    CREATE OR REPLACE SECRET s3_secret (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
        USE_SSL 'false',
        URL_STYLE 'path'
    )
""")
print("Secret S3 configurado.")

print("\n--- Consultando Tabela no S3 (MinIO) ---")
scan_func = "delta_scan" if delta_loaded else "read_parquet"
scan_path = f"s3://{BUCKET_NAME}/delta/sales"
if not delta_loaded:
    scan_path += "/**/*.parquet"

print(f"Strategy: {scan_func} on {scan_path}")

try:
    result = con.execute(f"""
        SELECT
            region,
            COUNT(*) as order_count,
            SUM(total_amount) as revenue
        FROM {scan_func}('{scan_path}')
        GROUP BY region
        ORDER BY revenue DESC
    """).df()
    print("Resultado:")
    print(result)
except Exception as e:
    print(f"Erro na query: {e}")

print("--- Fim do Capítulo 05 ---")
