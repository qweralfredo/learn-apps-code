# -*- coding: utf-8 -*-
"""
capitulo_05_importacao_exportacao_json
"""

# capitulo_05_importacao_exportacao_json
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

# Ler JSON
rel = duckdb.read_json("example.json")
print(rel)

# Query direta
result = duckdb.sql("SELECT * FROM 'example.json'")
result.show()

# Exemplo/Bloco 2
import duckdb

# Salvar resultado como JSON
duckdb.sql("SELECT * FROM table").write_json("output.json")

# Ou usando COPY
duckdb.sql("""
    COPY (SELECT * FROM table)
    TO 'output.json'
""")

# Exemplo/Bloco 3
import duckdb

# DuckDB pode trabalhar diretamente com dicts Python
data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25}
]

result = duckdb.sql("SELECT * FROM data")
result.show()

