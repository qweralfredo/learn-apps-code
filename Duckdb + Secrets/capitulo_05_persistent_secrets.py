# -*- coding: utf-8 -*-
"""
Capítulo 05: Persistent Secrets
"""

# Capítulo 5: Persistent Secrets
import duckdb
import os

# Criar database file-based
db_path = 'test_persistent.duckdb'
con = duckdb.connect(db_path)
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar PERSISTENT secret
con.execute("""
    CREATE PERSISTENT SECRET my_persistent_s3 (
        TYPE s3,
        KEY_ID 'persistent_key',
        SECRET 'persistent_secret',
        REGION 'us-east-1'
    )
""")

# Verificar persistência
secrets = con.execute("SELECT name, persistent, storage FROM duckdb_secrets()").df()
print(secrets)

con.close()

# Cleanup
if os.path.exists(db_path):
    os.remove(db_path)
