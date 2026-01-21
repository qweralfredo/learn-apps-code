# -*- coding: utf-8 -*-
"""
Capítulo 08: Secrets com Extensions
"""

# Capítulo 8: Secrets com Extensions
import duckdb

con = duckdb.connect(':memory:')

# Setup httpfs
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

# Secret S3
con.execute("""
    CREATE SECRET s3_main (
        TYPE s3, KEY_ID 'key', SECRET 'secret',
        REGION 'us-east-1', SCOPE 's3://main-bucket/'
    )
""")

# Secret HTTP
con.execute("""
    CREATE SECRET api_bearer (
        TYPE http, BEARER_TOKEN 'token123',
        SCOPE 'https://api.example.com/'
    )
""")

# Listar extensions e secrets
secrets = con.execute("SELECT name, type, scope FROM duckdb_secrets()").df()
print(secrets)

con.close()
