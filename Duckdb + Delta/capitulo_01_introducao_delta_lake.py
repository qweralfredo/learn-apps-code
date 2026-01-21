# -*- coding: utf-8 -*-
"""
Capítulo 01: Introdução Delta Lake
"""

import duckdb

print(f"--- Iniciando Capítulo 01: Introdução Delta Lake ---")

con = duckdb.connect(database=':memory:')

# 1. Install Delta Extension
print("\n>>> Instalando Extension Delta")
try:
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")
    print("Extension Delta carregada.")
except Exception as e:
    print(f"Aviso: {e} (Delta extension pode não estar disponível nesta build)")

# 2. Check Version/Availability
try:
    con.sql("SELECT * FROM duckdb_extensions() WHERE extension_name='delta'").show()
except:
    pass

print("\n--- Capítulo concluído com sucesso ---")
