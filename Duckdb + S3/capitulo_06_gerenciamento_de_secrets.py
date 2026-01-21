# -*- coding: utf-8 -*-
"""
Capítulo 06: Gerenciamento de Secrets
"""

import duckdb
import pandas as pd

print(f"--- Iniciando Capítulo 06: Gerenciamento de Secrets ---")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# 1. Create Secrets using MinIO credentials
print("\n>>> Executando: Criando Secrets")

# Secret Default (No scope implies generic S3)
con.execute("""
    CREATE OR REPLACE SECRET s3_default (
        TYPE S3,
        KEY_ID 'admin',
        SECRET 'password',
        REGION 'us-east-1',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")
print("Secret 's3_default' criado.")

# Secret Scoped (Hypothetical for a specific bucket 'other-bucket')
# DuckDB supports SCOPE option to apply secret only to specific paths
con.execute("""
    CREATE OR REPLACE SECRET s3_restricted (
        TYPE S3,
        SCOPE 's3://other-bucket',
        KEY_ID 'user2',
        SECRET 'pass2',
        ENDPOINT 'localhost:9000',
        USE_SSL false
    );
""")
print("Secret 's3_restricted' (Scoped) criado.")

# 2. Inspect Secrets (Note: Secrets values are redacted)
print("\n>>> Executando: Inspecionando Secrets")
con.sql("SELECT name, type, provider, scope FROM duckdb_secrets()").show()

# 3. Test Access
print("\n>>> Executando: Teste de Acesso (Default Secret)")
try:
    # Should use s3_default because path matches default behavior (or lack of specific scope)
    con.sql("SELECT count(*) FROM 's3://learn-duckdb-s3/data/vendas.csv'").show()
except Exception as e:
    print(f"Erro: {e}")

# 4. Drop Secret
print("\n>>> Executando: Drop Secret")
con.execute("DROP SECRET s3_restricted")
print("Secret 's3_restricted' removido.")

con.sql("SELECT name, type, provider, scope FROM duckdb_secrets()").show()

print("\n--- Capítulo concluído com sucesso ---")
