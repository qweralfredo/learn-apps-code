# -*- coding: utf-8 -*-
"""
Capítulo 02: Tipos de Secrets
"""

# Capítulo 2: Tipos de Secrets
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# S3 Secret
con.execute("""
    CREATE SECRET s3_secret (TYPE s3, KEY_ID 'key', SECRET 'secret', REGION 'us-east-1')
""")

# HTTP Secret
con.execute("""
    CREATE SECRET http_secret (TYPE http, BEARER_TOKEN 'token123')
""")

# Listar por tipo
secrets = con.execute("SELECT name, type, provider FROM duckdb_secrets()").df()
print(secrets)

con.close()
