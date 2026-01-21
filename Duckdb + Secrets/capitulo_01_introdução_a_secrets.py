# -*- coding: utf-8 -*-
"""
Capítulo 01: Introdução a Secrets
"""

# Capítulo 1: Introdução a Secrets
import duckdb

# Exemplo 1: Criar secret S3
con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute("""
    CREATE SECRET my_s3 (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1'
    )
""")

# Listar secrets
secrets = con.execute("SELECT * FROM duckdb_secrets()").df()
print(secrets[['name', 'type', 'provider']])

# Usar which_secret()
result = con.execute("""
    SELECT * FROM which_secret('s3://bucket/file.parquet', 's3')
""").df()
print(result[['name', 'scope']])

con.close()
