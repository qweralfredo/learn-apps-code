# -*- coding: utf-8 -*-
"""
capitulo_09_meta_queries
"""

# capitulo_09_meta_queries
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()

# Carregar dados
con.sql("CREATE TABLE sales AS SELECT * FROM 'sales.csv'")

# Ver estrutura
print("=== ESTRUTURA ===")
con.sql("DESCRIBE sales").show()

# Ver estatísticas
print("\n=== ESTATÍSTICAS ===")
con.sql("SUMMARIZE sales").show()

# Análise customizada
print("\n=== COLUNAS COM ALTA CARDINALIDADE ===")
result = con.sql("""
    SELECT column_name, approx_unique, count
    FROM (SUMMARIZE sales)
    WHERE approx_unique > 100
    ORDER BY approx_unique DESC
""").df()

print(result)

