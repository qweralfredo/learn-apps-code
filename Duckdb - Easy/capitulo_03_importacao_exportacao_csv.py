# -*- coding: utf-8 -*-
"""
capitulo_03_importacao_exportacao_csv
"""

# capitulo_03_importacao_exportacao_csv
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

# Ler CSV como Relation
rel = duckdb.read_csv("example.csv")
print(rel)

# Query direta
result = duckdb.sql("SELECT * FROM 'example.csv'")
result.show()

# Exemplo/Bloco 2
import duckdb

# Ler e converter para DataFrame
df = duckdb.read_csv("example.csv").df()

# Ou diretamente
df = duckdb.sql("SELECT * FROM 'example.csv'").df()

# Exemplo/Bloco 3
import duckdb

# Executar query e salvar como CSV
result = duckdb.sql("SELECT * FROM tbl")
result.write_csv("out.csv")

# Com método direto
duckdb.sql("SELECT * FROM tbl").write_csv("out.csv")

