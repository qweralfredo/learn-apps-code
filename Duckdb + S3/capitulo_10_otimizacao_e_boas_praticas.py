# -*- coding: utf-8 -*-
"""
Capítulo 10: Otimização e Boas Práticas
"""

import duckdb
import time

print(f"--- Iniciando Capítulo 10: Otimização e Boas Práticas ---")

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

query = "SELECT count(*) FROM 's3://learn-duckdb-s3/data/users_*.parquet'"

# 1. Without Metadata Cache (Default)
print("\n>>> Executando: Query Sem Cache Explícito")
start = time.time()
con.sql(query).show()
print(f"Tempo: {time.time() - start:.4f}s")

# 2. Enable Metadata Cache
print("\n>>> Executando: Query COM HTTP Metadata Cache")
# DuckDB versions might vary on parameter names, but enable_http_metadata_cache is standard in recent ones
try:
    con.execute("SET enable_http_metadata_cache=true")
except:
    pass

start = time.time()
con.sql(query).show()
print(f"Tempo: {time.time() - start:.4f}s")

# 3. Object Cache
print("\n>>> Executando: Query COM Object Cache")
# Only relevant for persistent connections typically, but good for demo
# con.execute("SET enable_object_cache=true") 
# print("Object cache enabled.")

print("\n--- Capítulo concluído com sucesso ---")
