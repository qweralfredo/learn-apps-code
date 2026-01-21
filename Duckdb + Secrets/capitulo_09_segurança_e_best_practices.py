# -*- coding: utf-8 -*-
"""
Capítulo 09: Segurança e Best Practices
"""

# Capítulo 9: Segurança e Best Practices
import duckdb
import os
from datetime import datetime

# NUNCA hardcode credenciais - use variáveis de ambiente
os.environ['AWS_ACCESS_KEY_ID'] = 'EXAMPLE_KEY'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'EXAMPLE_SECRET'

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Usar credenciais de variáveis de ambiente
s3_key = os.getenv('AWS_ACCESS_KEY_ID')
s3_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

con.execute(f"""
    CREATE SECRET s3_from_env (
        TYPE s3,
        KEY_ID '{s3_key}',
        SECRET '{s3_secret}',
        REGION 'us-east-1',
        USE_SSL true
    )
""")

print("✓ Secret criado usando variáveis de ambiente")
print("✓ SSL habilitado para segurança")

# Listar secrets (sem expor credenciais)
secrets = con.execute("SELECT name, type, provider FROM duckdb_secrets()").df()
print(secrets)

con.close()
