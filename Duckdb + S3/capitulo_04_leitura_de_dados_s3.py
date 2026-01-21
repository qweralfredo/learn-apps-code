# -*- coding: utf-8 -*-
"""
Capítulo 04: Leitura de Dados S3
"""

import duckdb
import pandas as pd

print(f"--- Iniciando Capítulo 04: Leitura de Dados S3 ---")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Setup Credentials (Using Secret as established in Ch3)
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

# ==============================================================================
# PREPARAÇÃO DE DADOS (Vamos criar alguns arquivos Parquet no S3)
# ==============================================================================
print(">>> Preparando dados no S3...")
con.execute("CREATE TABLE users (id INT, name VARCHAR, age INT)")
con.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)")

# Upload parquet files
con.execute("COPY users TO 's3://learn-duckdb-s3/data/users_001.parquet' (FORMAT PARQUET)")
con.execute("INSERT INTO users VALUES (4, 'David', 40), (5, 'Eve', 22)")
con.execute("COPY users TO 's3://learn-duckdb-s3/data/users_002.parquet' (FORMAT PARQUET)")
print("Arquivos users_001.parquet e users_002.parquet criados no S3.")

# ==============================================================================
# 1. Read Parquet S3
# ==============================================================================
print(f"\n>>> Executando: Read Parquet S3")
print("Lendo arquivo unico:")
con.sql("SELECT * FROM 's3://learn-duckdb-s3/data/users_001.parquet'").show()

# ==============================================================================
# 2. Read CSV S3
# ==============================================================================
print(f"\n>>> Executando: Read CSV S3")
# Assumindo que vendas.csv foi criado no Cap 01
try:
    con.sql("SELECT * FROM 's3://learn-duckdb-s3/data/vendas.csv'").show()
except:
    print("Arquivo vendas.csv não encontrado (talvez pule o Cap 1).")

# ==============================================================================
# 3. Glob Patterns (Leitura Múltipla)
# ==============================================================================
print(f"\n>>> Executando: Glob Patterns")
print("Lendo todos os usuarios (*.parquet):")
con.sql("SELECT * FROM 's3://learn-duckdb-s3/data/users_*.parquet'").show()

# Filename column (DuckDB feature)
print("Com coluna filename:")
# Nota: Necessario habilitar filename=true em algumas versoes ou usar glob diretamente
con.sql("SELECT * FROM read_parquet('s3://learn-duckdb-s3/data/users_*.parquet', filename=true)").show()

print("\n--- Capítulo concluído com sucesso ---")













