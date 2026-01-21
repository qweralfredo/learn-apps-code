# -*- coding: utf-8 -*-
"""
Capítulo 03: Cloud Storage Secrets
"""

# Capítulo 3: Cloud Storage Secrets
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# S3 com configurações avançadas
con.execute("""
    CREATE SECRET s3_advanced (
        TYPE s3,
        KEY_ID 'AKIAEXAMPLE',
        SECRET 'secretEXAMPLE',
        REGION 'us-east-1',
        ENDPOINT 's3.amazonaws.com',
        URL_STYLE 'vhost',
        USE_SSL true
    )
""")

# GCS com credential chain
con.execute("""
    CREATE SECRET gcs_chain (
        TYPE gcs,
        PROVIDER credential_chain
    )
""")

secrets = con.execute("SELECT name, type, scope FROM duckdb_secrets()").df()
print(secrets)

con.close()
