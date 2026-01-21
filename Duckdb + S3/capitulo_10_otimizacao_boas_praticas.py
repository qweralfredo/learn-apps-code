# -*- coding: utf-8 -*-
"""
capitulo_10_otimizacao_boas_praticas
"""

import duckdb
import os
import boto3
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError
from datetime import datetime

print(f"--- Iniciando Capítulo 10: Otimização e Boas Práticas ---")

# ==============================================================================
# SETUP Simulation (MinIO)
# ==============================================================================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"

# Buckets needed for examples
BUCKETS = [
    "my-bucket", "huge-bucket", "bucket", "raw-bucket", 
    "processed-bucket", "source-bucket", "target-bucket", 
    "bronze-bucket", "silver-bucket", "gold-bucket",
    "finance-bucket", "public-bucket"
]

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

print("Configurando Buckets no MinIO...")
for b in BUCKETS:
    try:
        s3_client.create_bucket(Bucket=b)
    except ClientError:
        pass # Bucket Exists

# ------------------------------------------------------------------------------
# Create Dummy Data
# ------------------------------------------------------------------------------
def upload_parquet_df(df, bucket, key):
    filename = "temp_data.parquet"
    df.to_parquet(filename)
    s3_client.upload_file(filename, bucket, key)
    os.remove(filename)

print("Gerando dados de teste...")

# 1. Wide Table (Many Columns)
df_wide = pd.DataFrame(np.random.randint(0,100,size=(100, 50)), columns=[f'col_{i}' for i in range(50)])
df_wide['id'] = range(100)
df_wide['name'] = [f"Name_{i}" for i in range(100)]
df_wide['email'] = [f"user_{i}@example.com" for i in range(100)]
df_wide['created_at'] = datetime.now()
upload_parquet_df(df_wide, "my-bucket", "wide_table.parquet")

# 2. Large Parquet (Just simulating name)
upload_parquet_df(df_wide, "my-bucket", "large.parquet")

# 3. Partitioned Data (Simulated)
# s3://huge-bucket/2024/01/15/data.parquet
df_part = pd.DataFrame({'id': range(10), 'date': '2024-01-15'})
upload_parquet_df(df_part, "huge-bucket", "2024/01/15/data.parquet")

# 4. Data for Copy/Compression Tests
df_hot = pd.DataFrame({'id': range(1000), 'val': np.random.randn(1000)})
upload_parquet_df(df_hot, "bucket", "input_data.parquet")

# 5. Pipeline Data
# s3://raw-bucket/input/2024-01-10.parquet
df_pipeline = pd.DataFrame({
    'id': range(50), 
    'name': [f"prod_{i}" for i in range(50)], 
    'amount': np.random.rand(50)*100,
    'date': '2024-01-10',
    'status': 'active',
    'region': 'us-east-1'
})
upload_parquet_df(df_pipeline, "raw-bucket", "input/2024-01-10.parquet")

# ==============================================================================
# DUCKDB CONFIG
# ==============================================================================
con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Setup Default Secret for MinIO
con.execute(f"""
CREATE OR REPLACE SECRET minio_default (
    TYPE S3,
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    USE_SSL 'false',
    URL_STYLE 'path'
);
""")

# ==============================================================================
# EXAMPLES
# ==============================================================================

print("\n--- 1. Projection Pushdown (Selecionar poucas colunas) ---")
# Baixa apenas colunas necessárias
res = con.execute("""
SELECT id, name, email
FROM 's3://my-bucket/wide_table.parquet'
LIMIT 5;
""").df()
print(res)

print("\n--- 2. Metadata Reading (Count rápido) ---")
res = con.execute("SELECT count(*) FROM 's3://my-bucket/large.parquet'").fetchone()
print(f"Count(*): {res[0]}")

print("\n--- 3. Partition Pruning ---")
# Query eficiente com partições (DuckDB entende estrutura de diretórios data=... se usar hive partitioning,
# aqui estamos usando globbing manual ou estrutura de data).
# O exemplo original: 's3://huge-bucket/2024/01/15/*.parquet'
try:
    res = con.execute("""
    SELECT count(*) FROM 's3://huge-bucket/2024/01/15/*.parquet';
    """).fetchone()
    print(f"Rows in partition: {res[0]}")
except Exception as e:
    print(f"Erro partition: {e}")

print("\n--- 4. Compression Formats (Simulação de Escrita) ---")
# Nota: Compressão real depende do conteúdo
try:
    con.execute("""
    COPY (SELECT * FROM 's3://bucket/input_data.parquet') 
    TO 's3://bucket/hot.parquet' (FORMAT parquet, COMPRESSION 'snappy');
    """)
    print("Escrita com Snappy: OK")

    con.execute("""
    COPY (SELECT * FROM 's3://bucket/input_data.parquet') 
    TO 's3://bucket/warm.parquet' (FORMAT parquet, COMPRESSION 'zstd');
    """)
    print("Escrita com Zstd: OK")
except Exception as e:
    print(f"Erro compressão: {e}")

print("\n--- 5. Security & Secrets Management (Conceitual) ---")
# Exemplo de criação de segredo (Bad Practice vs Good Practice)
# Aqui só criamos para validar sintaxe, apontando para MinIO para não falhar
con.execute(f"""
CREATE OR REPLACE SECRET finance_data (
    TYPE S3,
    SCOPE 's3://finance-bucket',
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    USE_SSL 'false',
    URL_STYLE 'path'
);
""")
print("Secret 'finance_data' criado com SCOPE.")

print("\n--- 6. Settings Tuning ---")
con.execute("SET threads = 4;")
con.execute("SET s3_uploader_thread_limit = 4;")
con.execute("SET http_timeout = 30000;") # ms
print("Settings ajustadas.")

print("\n--- 7. ETL Pipeline Example ---")
# Leitura e Transformação
try:
    con.execute("""
    COPY (
        SELECT
            id,
            upper(name) as name,
            amount * 1.1 as amount_with_tax,
            current_timestamp as processed_at
        FROM 's3://raw-bucket/input/**/*.parquet'
        WHERE status = 'active'
    ) TO 's3://processed-bucket/output_etl.parquet' (
        FORMAT parquet,
        COMPRESSION zstd
    );
    """)
    print("ETL concluído com sucesso.")
except Exception as e:
    print(f"Erro ETL: {e}")

print("\n--- 8. Data Layering (Consumo camadas) ---")
# Simples query para validar acesso
try:
    con.execute("CREATE TABLE IF NOT EXISTS watermark (last_processed_date DATE);")
    con.execute("INSERT INTO watermark VALUES ('2023-01-01');")
    
    # Simulação de Incremental
    # Vamos apenas listar o que existiria
    print("Simulação de carga incremental (Query preparada).")
except Exception as e:
    print(f"Erro Data Layering: {e}")

print("\n--- Fim do Capítulo 10 ---")


