# -*- coding: utf-8 -*-
"""
Capítulo 01: Introdução a Secrets
"""

import duckdb

print(f"--- Iniciando Capítulo 01: Introdução a Secrets ---")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# 1. Inspect Initial State
print("\n>>> Antes (Sem Secrets):")
con.sql("SELECT name, type, provider FROM duckdb_secrets()").show()

# 2. Creating a Secret
print("\n>>> Criando Secret (MinIO)...")
con.execute("""
    CREATE SECRET my_secret (
        TYPE S3,
        KEY_ID 'admin',
        SECRET 'password',
        ENDPOINT 'localhost:9000',
        USE_SSL false
    )
""")

# 3. Inspect After
print("\n>>> Depois (Com Secret):")
con.sql("SELECT name, type, provider FROM duckdb_secrets()").show()

print("\n--- Capítulo concluído com sucesso ---")



























