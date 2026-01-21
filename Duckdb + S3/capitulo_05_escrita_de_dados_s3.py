# -*- coding: utf-8 -*-
"""
Capítulo 05: Escrita de Dados S3
"""

import duckdb
import pandas as pd

print(f"--- Iniciando Capítulo 05: Escrita de Dados S3 ---")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Setup Credentials (MinIO)
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

# Create sample data
con.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INT,
        date DATE,
        amount DECIMAL(10,2),
        region VARCHAR
    );
    INSERT INTO transactions VALUES 
    (1, '2023-01-01', 100.50, 'North'),
    (2, '2023-01-01', 200.00, 'South'),
    (3, '2023-01-02', 150.00, 'North'),
    (4, '2023-01-02', 300.50, 'East');
""")

# ==============================================================================
# 1. Escrita Simples (Parquet)
# ==============================================================================
print(f"\n>>> Executando: Escrita Simples (Parquet)")
con.execute("COPY transactions TO 's3://learn-duckdb-s3/data/transactions.parquet' (FORMAT PARQUET)")
print("Arquivo transactions.parquet escrito.")

# Verify
con.sql("SELECT count(*) FROM 's3://learn-duckdb-s3/data/transactions.parquet'").show()

# ==============================================================================
# 2. Escrita com Compressão (ZSTD)
# ==============================================================================
print(f"\n>>> Executando: Escrita com Compressão (ZSTD)")
con.execute("""
    COPY transactions TO 's3://learn-duckdb-s3/data/transactions_zstd.parquet' 
    (FORMAT PARQUET, COMPRESSION ZSTD)
""")
print("Arquivo transactions_zstd.parquet escrito.")

# ==============================================================================
# 3. Escrita Particionada (Hive Partitioning)
# ==============================================================================
print(f"\n>>> Executando: Escrita Particionada (Hive)")
# Partition by Region
con.execute("""
    COPY transactions TO 's3://learn-duckdb-s3/data/transactions_partitioned' 
    (FORMAT PARQUET, PARTITION_BY (region), OVERWRITE_OR_IGNORE)
""")
print("Dados particionados escritos em data/transactions_partitioned/")

# Verify partition structure
print("Lendo dados particionados:")
con.sql("""
    SELECT * 
    FROM read_parquet('s3://learn-duckdb-s3/data/transactions_partitioned/**/*.parquet', hive_partitioning=true)
""").show()

print("\n--- Capítulo concluído com sucesso ---")
