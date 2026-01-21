# -*- coding: utf-8 -*-
"""
capitulo_07_otimizacoes_performance
"""

import duckdb
import os
import shutil
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Helper injected
def safe_install_ext(con, ext_name):
    try:
        con.install_extension(ext_name)
        con.load_extension(ext_name)
        print(f"Extension '{ext_name}' loaded.")
        return True
    except Exception as e:
        print(f"Could not load extension '{ext_name}': {e}")
        return False

print("--- Iniciando Capítulo 07: Otimizações de Performance (Delta) ---")

# ==============================================================================
# DATA GENERATION
# ==============================================================================
TABLE_PATH = "./large_delta_table"
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)

print("Gerando dados de teste...")
# Create a moderately sized dataset
dates = [datetime(2024, 1, 15) + timedelta(days=i%3) for i in range(1000)]
regions = ['US', 'EU', 'AS', 'SA']
df = pd.DataFrame({
    'id': range(1000),
    'date': dates,
    'region': [regions[i%4] for i in range(1000)],
    'amount': np.random.rand(1000) * 100,
    'description': ['A'*100] * 1000 # padding to make it wider
})

try:
    from deltalake import write_deltalake
    write_deltalake(TABLE_PATH, df, partition_by=['date', 'region'])
    print("Tabela Delta criada.")
except ImportError:
    print("Deltalake missing. Creating Parquet structure.")
    # Manually mimic partitioning for DuckDB hive discovery
    for r in regions:
        # Simplificando: vamos salvar tudo num arquivo flat para teste básico se partição falhar na criação manual
        # Mas DuckDB gosta de hive: year=...
        # Vamos salvar sem partição física complexa para o fallback simples
        os.makedirs(TABLE_PATH, exist_ok=True)
        df.to_parquet(f"{TABLE_PATH}/data.parquet")

# ==============================================================================
# DUCKDB PROFILING
# ==============================================================================
con = duckdb.connect()
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet")

scan_func = "delta_scan" if delta_loaded else "read_parquet"
scan_arg = f"'{TABLE_PATH}'" if delta_loaded else f"'{TABLE_PATH}/**/*.parquet'"

print(f"\n--- 1. Filter Pushdown Analysis ({scan_func}) ---")
con.execute("SET enable_profiling='json'")
con.execute("SET profiling_mode='detailed'")
con.execute("PRAGMA disable_print_progress_bar") 

start = time.time()
try:
    result = con.execute(f"""
        SELECT COUNT(*), SUM(amount)
        FROM {scan_func}({scan_arg})
        WHERE date = '2024-01-15'
            AND region = 'US'
    """).fetchone()
    elapsed = time.time() - start
    print(f"Results: {result}")
    print(f"Time: {elapsed:.4f}s")
    
    # Check Profile
    # Note: duckdb profile output is a file path usually, or we query system_profile_info
    # PRAGMA last_profiling_output returns the filename of the JSON
    # We won't print the whole JSON content here.
    print("Query executada (Profile generated).")
except Exception as e:
    print(f"Erro na query 1: {e}")

print(f"\n--- 2. Projection Pushdown (SELECT * vs specific) ---")
con.execute("PRAGMA disable_profiling")

try:
    start = time.time()
    # Select All
    r1 = con.execute(f"SELECT * FROM {scan_func}({scan_arg}) LIMIT 1000").df()
    t1 = time.time() - start
    print(f"SELECT *: {t1:.4f}s")

    start = time.time()
    # Select Specific
    r2 = con.execute(f"SELECT id, amount FROM {scan_func}({scan_arg}) LIMIT 1000").df()
    t2 = time.time() - start
    print(f"SELECT specific: {t2:.4f}s")
    
except Exception as e:
    print(f"Erro teste 2: {e}")

# Clean up
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)

print("--- Fim do Capítulo 07 ---")
