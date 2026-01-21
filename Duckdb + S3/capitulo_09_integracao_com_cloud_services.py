# -*- coding: utf-8 -*-
"""
Capítulo 09: Integração com Cloud Services (Pandas/Python Integration)
"""

import duckdb
import pandas as pd

print(f"--- Iniciando Capítulo 09: Integração com Python/Pandas ---")

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

# 1. DuckDB Result to Pandas
print("\n>>> Executando: DuckDB -> Pandas")
df = con.sql("SELECT * FROM 's3://learn-duckdb-s3/data/users_001.parquet'").df()
print("DataFrame Pandas:")
print(df.head())

# 2. Pandas to S3 (via DuckDB)
print("\n>>> Executando: Pandas -> DuckDB -> S3")
df['new_col'] = df['age'] * 2

# Register DataFrame as Update
con.register('df_view', df)
con.execute("COPY df_view TO 's3://learn-duckdb-s3/data/from_pandas.parquet' (FORMAT PARQUET)")
print("Arquivo s3://learn-duckdb-s3/data/from_pandas.parquet criado.")

# Verify
con.sql("SELECT * FROM 's3://learn-duckdb-s3/data/from_pandas.parquet'").show()

print("\n--- Capítulo concluído com sucesso ---")
