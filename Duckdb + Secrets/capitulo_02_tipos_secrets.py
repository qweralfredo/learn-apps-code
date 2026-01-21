# -*- coding: utf-8 -*-
"""
Capítulo 02: Tipos de Secrets
"""

import duckdb

print(f"--- Iniciando Capítulo 02: Tipos de Secrets ---")

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# 1. TYPE S3
print("\n>>> TYPE S3")
con.execute("""
    CREATE OR REPLACE SECRET s3_secret (
        TYPE S3,
        KEY_ID 'access_key',
        SECRET 'secret_key'
    )
""")
print("Secret S3 criado.")

# 2. TYPE GCS (Google Cloud)
print("\n>>> TYPE GCS")
con.execute("""
    CREATE OR REPLACE SECRET gcs_secret (
        TYPE GCS,
        KEY_ID 'gcp_key',
        SECRET 'gcp_secret'
    )
""")
print("Secret GCS criado.")

# 3. TYPE AZURE (Azure)
print("\n>>> TYPE AZURE")
try:
    con.execute("INSTALL azure; LOAD azure;")
    con.execute("""
        CREATE OR REPLACE SECRET azure_secret (
            TYPE AZURE,
            CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;'
        )
    """)
    print("Secret Azure criado.")
except Exception as e:
    print(f"Info: Azure setup skipped ({e})")

# List all
con.sql("SELECT name, type FROM duckdb_secrets()").show()

print("\n--- Capítulo concluído com sucesso ---")
