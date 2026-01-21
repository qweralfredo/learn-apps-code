# -*- coding: utf-8 -*-
"""
Capítulo 08: Padrões Avançados e Globbing
"""

import duckdb

print(f"--- Iniciando Capítulo 08: Padrões Avançados e Globbing ---")

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

# 1. Recursive Globbing
print("\n>>> Executando: Recursive Globbing")
# Might need to ensure structure exists. assuming data/*.parquet from previous chapters
try:
    con.sql("SELECT count(*) FROM 's3://learn-duckdb-s3/**/*.parquet'").show()
except Exception as e:
    print(e)
    
# 2. List of Files
print("\n>>> Executando: Leitura de Lista de Arquivos")
try:
    con.sql("""
        SELECT * FROM read_parquet([
            's3://learn-duckdb-s3/data/users_001.parquet',
            's3://learn-duckdb-s3/data/users_002.parquet'
        ])
    """).show()
except Exception as e:
    print(e)

# 3. Filename expansion
print("\n>>> Executando: Filename Expansion")
try:
    con.sql("SELECT filename, count(*) FROM read_parquet('s3://learn-duckdb-s3/data/*.parquet', filename=true) GROUP BY filename").show()
except Exception as e:
    print(e)

print("\n--- Capítulo concluído com sucesso ---")
