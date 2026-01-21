# -*- coding: utf-8 -*-
"""
Capítulo 07: Trabalhando com Parquet no S3
"""

import duckdb

print(f"--- Iniciando Capítulo 07: Trabalhando com Parquet no S3 ---")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Setup Credentials
con.execute("""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3,
        KEY_ID 'admin',
        SECRET 'password',
        REGION 'us-east-1',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")

# 1. Inspecting Parquet Metadata directly from S3
print("\n>>> Executando: Inspecting Metadata (S3)")
try:
    # Requires HTTP range requests to be efficient
    con.sql("SELECT * FROM parquet_metadata('s3://learn-duckdb-s3/data/users_001.parquet')").show()
except Exception as e:
    print(f"Erro: {e}")

# 2. Reading specific columns (Projection Pushdown)
print("\n>>> Executando: Projection Pushdown")
# DuckDB only fetches headers and the 'name' column chunks
con.sql("SELECT name FROM 's3://learn-duckdb-s3/data/users_001.parquet'").show()

# 3. Filter Pushdown (Predicate Pushdown)
print("\n>>> Executando: Filter Pushdown")
# DuckDB downloads metadata, checks statistics (min/max), and only downloads relevant row groups
con.sql("SELECT * FROM 's3://learn-duckdb-s3/data/users_001.parquet' WHERE id = 2").show()

print("\n--- Capítulo concluído com sucesso ---")
