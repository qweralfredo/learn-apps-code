# -*- coding: utf-8 -*-
"""
capitulo_08_consultas_arquivos_remotos
"""

# capitulo_08_consultas_arquivos_remotos
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()

# Configurar credenciais
con.sql("""
    CREATE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID 'YOUR_KEY',
        SECRET 'YOUR_SECRET',
        REGION 'us-east-1'
    )
""")

# Combinar arquivo local com S3
result = con.sql("""
    SELECT
        l.user_id,
        l.name,
        s.total_purchases
    FROM 'local_users.csv' l
    JOIN 's3://my-bucket/user_stats.parquet' s
        ON l.user_id = s.user_id
    WHERE s.total_purchases > 1000
""").df()

print(result)

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()

# Testar com arquivo público
try:
    result = con.sql("""
        SELECT count(*)
        FROM 'https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet'
    """).fetchone()
    print(f"Conectividade OK: {result[0]} linhas")
except Exception as e:
    print(f"Erro: {e}")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()

# Tentar criar secret
try:
    con.sql("""
        CREATE SECRET secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID 'test_key',
            SECRET 'test_secret',
            REGION 'us-east-1'
        )
    """)
    print("Secret criado com sucesso")
except Exception as e:
    print(f"Erro ao criar secret: {e}")

