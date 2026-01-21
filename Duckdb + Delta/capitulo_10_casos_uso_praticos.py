# -*- coding: utf-8 -*-
"""
capitulo_10_casos_uso_praticos
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import numpy as np
from datetime import datetime
import shutil

# Helper Helper
def safe_install_ext(con, ext_name):
    try:
        con.install_extension(ext_name)
        con.load_extension(ext_name)
        print(f"Extension '{ext_name}' loaded.")
        return True
    except Exception:
        return False

print("--- Iniciando Capítulo 10: Casos de Uso Práticos (Pipeline) ---")

# ==============================================================================
# INFRASTRUCTURE MOCK
# ==============================================================================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET = "ecommerce-lake"

s3 = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, 
                  aws_access_key_id=MINIO_ACCESS_KEY, 
                  aws_secret_access_key=MINIO_SECRET_KEY)
try:
    s3.create_bucket(Bucket=BUCKET)
except ClientError:
    pass

# Generate Local Data -> Upload -> Delete Local
LOCAL_TEMP = "./temp_sales_data"
if os.path.exists(LOCAL_TEMP): shutil.rmtree(LOCAL_TEMP)

print("Gerando dados no MinIO...")
try:
    from deltalake import write_deltalake
    df = pd.DataFrame({
        'order_id': range(100),
        'customer_id': np.random.randint(0, 20, 100),
        'order_date': [datetime(2024, 1, 15)] * 100,
        'total_amount': np.random.rand(100) * 100
    })
    write_deltalake(LOCAL_TEMP, df)
    
    # Upload
    for root, _, files in os.walk(LOCAL_TEMP):
        for f in files:
            full_path = os.path.join(root, f)
            rel_path = os.path.relpath(full_path, LOCAL_TEMP).replace("\\", "/")
            s3.upload_file(full_path, BUCKET, f"sales/{rel_path}")
            
except ImportError:
    print("Deltalake library missing. Using Parquet fallback upload.")
    os.makedirs(LOCAL_TEMP, exist_ok=True)
    df = pd.DataFrame({
        'order_id': range(100),
        'customer_id': np.random.randint(0, 20, 100),
        'order_date': [datetime(2024, 1, 15)] * 100,
        'total_amount': np.random.rand(100) * 100
    })
    df.to_parquet(os.path.join(LOCAL_TEMP, "data.parquet"))
    s3.upload_file(os.path.join(LOCAL_TEMP, "data.parquet"), BUCKET, "sales/data.parquet")

if os.path.exists(LOCAL_TEMP): shutil.rmtree(LOCAL_TEMP)


# ==============================================================================
# PIPELINE CLASS
# ==============================================================================
class EcommerceAnalyticsPipeline:
    def __init__(self, s3_base_path):
        self.s3_base_path = s3_base_path
        self.con = duckdb.connect()
        self._setup_extensions()
        self._setup_secrets()

    def _setup_extensions(self):
        safe_install_ext(self.con, "httpfs")
        self.delta_available = safe_install_ext(self.con, "delta")
        safe_install_ext(self.con, "parquet")

    def _setup_secrets(self):
        self.con.execute(f"""
            CREATE OR REPLACE SECRET minio_pipe (
                TYPE S3,
                KEY_ID '{MINIO_ACCESS_KEY}',
                SECRET '{MINIO_SECRET_KEY}',
                ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
                USE_SSL 'false',
                URL_STYLE 'path'
            )
        """)

    def _get_scan_sql(self, subpath):
        full_path = f"{self.s3_base_path}/{subpath}"
        if self.delta_available:
            return f"delta_scan('{full_path}')"
        else:
            return f"read_parquet('{full_path}/**/*.parquet')"

    def get_sales_metrics(self, start_date, end_date):
        scan_clause = self._get_scan_sql("sales")
        query = f"""
        SELECT
            CAST(order_date AS DATE) as date,
            COUNT(DISTINCT order_id) as orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value
        FROM {scan_clause}
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1
        """
        try:
            return self.con.execute(query).df()
        except Exception as e:
            print(f"Query failed: {e}")
            return pd.DataFrame()

# ==============================================================================
# EXECUTION
# ==============================================================================
print("\n--- Running Pipeline ---")
pipeline = EcommerceAnalyticsPipeline(f"s3://{BUCKET}")
df_metrics = pipeline.get_sales_metrics('2024-01-01', '2024-01-31')

print("Metrics Report:")
print(df_metrics)

print("--- Fim do Capítulo 10 ---")
