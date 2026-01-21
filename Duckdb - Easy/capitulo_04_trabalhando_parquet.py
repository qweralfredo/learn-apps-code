# -*- coding: utf-8 -*-
"""
capitulo_04_trabalhando_parquet
"""

# capitulo_04_trabalhando_parquet
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

# Ler Parquet
rel = duckdb.read_parquet("example.parquet")
print(rel)

# Query direta
result = duckdb.sql("SELECT * FROM 'example.parquet'")
result.show()

# Converter para DataFrame
df = duckdb.read_parquet("example.parquet").df()

# Exemplo/Bloco 2
import duckdb

# Salvar query como Parquet
duckdb.sql("SELECT * FROM tbl").write_parquet("out.parquet")

# Com compressão específica
duckdb.sql("""
    COPY (SELECT * FROM tbl)
    TO 'out.parquet'
    (FORMAT parquet, COMPRESSION zstd)
""")

