# -*- coding: utf-8 -*-
"""
Capítulo 06: Secret Providers
"""

# Capítulo 6: Secret Providers
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Config provider (explícito)
con.execute("""
    CREATE SECRET s3_config (
        TYPE s3,
        PROVIDER config,
        KEY_ID 'key',
        SECRET 'secret'
    )
""")

# Credential chain provider
con.execute("""
    CREATE SECRET s3_chain (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'env;config'
    )
""")

# Listar providers
secrets = con.execute("SELECT name, type, provider FROM duckdb_secrets()").df()
print(secrets)

con.close()
