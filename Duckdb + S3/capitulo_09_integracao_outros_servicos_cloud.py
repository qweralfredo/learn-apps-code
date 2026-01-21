# -*- coding: utf-8 -*-
"""
capitulo_09_integracao_outros_servicos_cloud
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError

print(f"--- Iniciando Capítulo 09: Integração com Outros Serviços Cloud ---")

# ==============================================================================
# SETUP Simulation (MinIO)
# ==============================================================================
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_R2_SIM = "simulated-r2-bucket"

s3_client = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY
)

try:
    s3_client.create_bucket(Bucket=BUCKET_R2_SIM)
except ClientError:
    pass

# Upload dummy data
with open("cloud_data.csv", "w") as f:
    f.write("service,status\nR2,OK\nGCS,Skipped")
s3_client.upload_file("cloud_data.csv", BUCKET_R2_SIM, "data.csv")
os.remove("cloud_data.csv")

# ==============================================================================
# EXEMPLOS DUCKDB
# ==============================================================================
con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

print("\n--- 1. Cloudflare R2 (Simulado via MinIO S3 API) ---")
# R2 é compatível com S3. Podemos usar TYPE S3 e mudar o endpoint.
# No DuckDB existe TYPE R2 que facilita, mas por baixo usa S3.
con.execute(f"""
CREATE OR REPLACE SECRET r2_secret (
    TYPE R2,
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    ACCOUNT_ID 'minio_simulated',
    session_token '', 
    -- Hack para forçar endpoint MinIO
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    USE_SSL 'false',
    URL_STYLE 'path'
);
""")
# Nota: TYPE R2 tenta construir url: <account_id>.r2.cloudflarestorage.com
# Se sobrescrevermos ENDPOINT, talvez funcione como alias S3.
# Se falhar, usamos TYPE S3 para simular.

try:
    # Tenta usar r2:// (que usa o secret type r2)
    # Se DuckDB hardcodar o dominio r2.cloudflarestorage.com mesmo com endpoint custom no secret, pode falhar.
    # Vamos testar TYPE S3 simulando.
    
    con.execute(f"""
    CREATE OR REPLACE SECRET r2_sim (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        REGION 'auto',
        ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
        USE_SSL 'false',
        URL_STYLE 'path'
    );
    """)
    res = con.execute(f"SELECT * FROM 's3://{BUCKET_R2_SIM}/data.csv'").df()
    print("Leitura com protocolo S3 (simulando outros providers S3-compatíveis):")
    print(res)
except Exception as e:
    print(f"Erro na simulação: {e}")

print("\n--- 2. Google Cloud Storage (GCS) ---")
print("Exemplo de configuração (não executado sem credenciais):")
print("""
CREATE SECRET gcs_secret (
    TYPE GCS,
    KEY_ID 'GOOG...',
    SECRET '...'
);
-- SELECT * FROM 'gs://bucket/file.parquet';
""")

print("\n--- 3. Azure Blob Storage ---")
print("Exemplo de configuração (não executado):")
print("""
CREATE SECRET azure_secret (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net'
);
-- SELECT * FROM 'azure://container/blob.parquet';
""")


