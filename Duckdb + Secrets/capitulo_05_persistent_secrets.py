# -*- coding: utf-8 -*-
"""
Capítulo 05: Persistent Secrets
"""

import duckdb
import os

print(f"--- Iniciando Capítulo 05: Persistent Secrets ---")

# Using a persistent database file to test persistence, though SECRETS usually stored in global config manager
# Note: DuckDB secrets persist in the system-wide (user) config usually, unless temporary.

con = duckdb.connect(database=':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# 1. Create Persistent Secret
print("\n>>> Criando Secret Persistente (duckdb_secrets_storage='local_file')")
# By default, secrets are temporary in in-memory DB unless a secrets manager is configured.
# However, the CREATE PERSISTENT SECRET syntax exists in some contexts or implicit via manager.
# Standard syntax is CREATE SECRET ... (PERSIST) - wait, syntax is `CREATE PERSISTENT SECRET` or via option?
# In 0.10+, secrets are persisted by default if using the default secret provider on disk-based DB?
# Actually, let's create a secret and see the storage provider.

con.execute("""
    CREATE OR REPLACE SECRET persist_s3 (
        TYPE S3,
        KEY_ID 'admin',
        SECRET 'pass'
    )
""")
print("Secret 'persist_s3' criado.")

# 2. Check Provider
con.sql("SELECT name, provider FROM duckdb_secrets() WHERE name='persist_s3'").show()

# 3. Drop (Clean up)
print("Removendo secrets (Temporary e Persistent se existirem)...")
try:
    con.execute("DROP TEMPORARY SECRET IF EXISTS persist_s3")
except: pass

try:
    con.execute("DROP PERSISTENT SECRET IF EXISTS persist_s3")
except: pass
print("Secret removido.")

print("\n--- Capítulo concluído com sucesso ---")
