# -*- coding: utf-8 -*-
"""
Capítulo 03: Configuração Credenciais AWS (MinIO)
"""

import duckdb

print(f"--- Iniciando Capítulo 03: Configuração Credenciais AWS ---")
con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Constants for MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"

# 1. Configuração Legacy (SET Variables)
print("\n>>> Executando: Configuração Legacy (SET)")
con.execute(f"SET s3_region='us-east-1'")
con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}'")
con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}'")
con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}'")
con.execute("SET s3_use_ssl='false'")
con.execute("SET s3_url_style='path'")

# Verify by listing buckets (assuming bucket 'learn-duckdb-s3' exists from ch1)
# Note: globbing requires full path with bucket
try:
    print("Teste Legacy: Listando arquivos...")
    con.sql("SELECT * FROM glob('s3://learn-duckdb-s3/data/*.csv')").show()
except Exception as e:
    print(f"Erro no teste Legacy: {e}")

# 2. Configuração Moderna (SECRETS)
print("\n>>> Executando: Configuração Moderna (SECRETS)")

# Clear variables first to ensure we use secret
con.execute("RESET s3_access_key_id; RESET s3_secret_access_key;")

# Create Secret
con.execute(f"""
    CREATE OR REPLACE SECRET my_minio_secret (
        TYPE S3,
        KEY_ID '{MINIO_ACCESS_KEY}',
        SECRET '{MINIO_SECRET_KEY}',
        REGION 'us-east-1',
        ENDPOINT '{MINIO_ENDPOINT}',
        URL_STYLE 'path',
        USE_SSL false
    );
""")
print("Secret 'my_minio_secret' criado.")

# Verify again
try:
    print("Teste Secret: Listando arquivos...")
    con.sql("SELECT * FROM glob('s3://learn-duckdb-s3/data/*.csv')").show()
except Exception as e:
    print(f"Erro no teste Secret: {e}")

print("\n--- Capítulo concluído com sucesso ---")
