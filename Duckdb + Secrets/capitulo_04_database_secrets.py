# -*- coding: utf-8 -*-
"""
Capítulo 04: Database Secrets
"""

# Capítulo 4: Database Secrets
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

# PostgreSQL Secret
con.execute("""
    CREATE SECRET pg_db (
        TYPE postgres,
        HOST 'localhost',
        PORT 5432,
        DATABASE 'mydb',
        USER 'postgres',
        PASSWORD 'password',
        SSLMODE 'require'
    )
""")

# Listar secrets de database
secrets = con.execute("""
    SELECT name, type FROM duckdb_secrets() WHERE type IN ('postgres', 'mysql')
""").df()
print(secrets)

con.close()
