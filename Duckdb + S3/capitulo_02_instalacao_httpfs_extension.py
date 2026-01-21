# -*- coding: utf-8 -*-
"""
Capítulo 02: Instalação HTTPFS Extension
"""

import duckdb
import pandas as pd

print(f"--- Iniciando Capítulo 02: Instalação HTTPFS Extension ---")

con = duckdb.connect(database=':memory:')

# 1. Install Extension
print("\n>>> Executando: Install Extension")
try:
    con.execute("INSTALL httpfs")
    print("Extension 'httpfs' instalada.")
except Exception as e:
    print(f"Info: {e} (Geralmente já vem instalada em builds Python)")

# 2. Load Extension
print("\n>>> Executando: Load Extension")
con.execute("LOAD httpfs")
print("Extension 'httpfs' carregada.")

# 3. Verify Extension
print("\n>>> Verificando Extensões Carregadas")
con.sql("SELECT * FROM duckdb_extensions() WHERE loaded=true AND extension_name='httpfs'").show()

# 4. Configuração Básica (Placeholder, será detalhado depois)
print("\n>>> Executando: Configuração Básica")
con.execute("SET s3_region='us-east-1'")
con.execute("SET s3_url_style='path'")

print("\n--- Capítulo concluído com sucesso ---")













