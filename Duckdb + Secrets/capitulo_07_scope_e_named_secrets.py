# -*- coding: utf-8 -*-
"""
Capítulo 07: SCOPE e Named Secrets
"""

# Capítulo 7: SCOPE e Named Secrets
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secrets com SCOPEs diferentes
con.execute("""
    CREATE SECRET s3_bucket1 (
        TYPE s3, KEY_ID 'k1', SECRET 's1',
        SCOPE 's3://bucket1/'
    )
""")

con.execute("""
    CREATE SECRET s3_bucket2 (
        TYPE s3, KEY_ID 'k2', SECRET 's2',
        SCOPE 's3://bucket2/'
    )
""")

# Usar which_secret() para verificar matching
test_urls = ['s3://bucket1/file.parquet', 's3://bucket2/data.csv', 's3://bucket3/other.parquet']

for url in test_urls:
    try:
        result = con.execute(f"SELECT name FROM which_secret('{url}', 's3')").fetchone()
        print(f"{url:40} → {result[0] if result else 'None'}")
    except Exception as e:
        print(f"{url:40} → Error")

con.close()
