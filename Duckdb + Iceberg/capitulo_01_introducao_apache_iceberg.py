# -*- coding: utf-8 -*-
"""
Iceberg-01-introducao-apache-iceberg
"""

# Iceberg-01-introducao-apache-iceberg
import duckdb
import os

# Exemplo/Bloco 1
# Análise rápida com DuckDB em tabela Iceberg gerenciada
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg")
con.execute("LOAD iceberg")

# Query em tabela Iceberg como se fosse Parquet normal
result = con.execute("""
    SELECT category, sum(revenue)
    FROM iceberg_scan('s3://bucket/sales')
    GROUP BY category
""").df()

