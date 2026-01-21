# -*- coding: utf-8 -*-
"""
capitulo_04_leitura_tabelas_delta
"""

import duckdb
import os
import shutil
import pandas as pd
from datetime import datetime

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

# Setup Environment
if os.path.exists("./sales"):
    shutil.rmtree("./sales")

# Create Dummy Delta Table
try:
    from deltalake import write_deltalake
    df_sales = pd.DataFrame({
        'order_id': range(100),
        'region': ['US' if i % 2 == 0 else 'EU' for i in range(100)],
        'total_amount': [float(i)*10 for i in range(100)],
        'order_date': [datetime.now() for i in range(100)]
    })
    write_deltalake("./sales", df_sales)
    print("Mock Delta table './sales' created.")
except ImportError:
    print("Deltalake library missing. Creating Parquet fallback for simulation.")
    os.makedirs("./sales", exist_ok=True)
    df_sales = pd.DataFrame({
        'order_id': range(100),
        'region': ['US' if i % 2 == 0 else 'EU' for i in range(100)],
        'total_amount': [float(i)*10 for i in range(100)],
        'order_date': [datetime.now() for i in range(100)]
    })
    df_sales.to_parquet("./sales/part-001.parquet")

print("--- Iniciando Capítulo 04: Leitura de Tabelas Delta ---")

con = duckdb.connect()
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet") # Ensure parquet is loaded for fallback

# Determine scan method
scan_method = "delta_scan('./sales')" if delta_loaded else "read_parquet('./sales/**/*.parquet')"
print(f"Using scan method: {scan_method}")

# Exemplo/Bloco 1 - Pandas
print("\n--- 1. Pandas Integration ---")
try:
    df = con.execute(f"""
        SELECT *
        FROM {scan_method}
        -- WHERE order_date >= '2024-01-01' -- Commented out to ensure data return with dynamic dates
    """).df()

    print(df.head())
    print(f"Total rows: {len(df)}")
    print("Análise Pandas:")
    print(df.groupby('region')['total_amount'].sum())
except Exception as e:
    print(f"Erro Pandas: {e}")

# Exemplo/Bloco 2 - Polars
print("\n--- 2. Polars Integration ---")
try:
    import polars as pl
    
    # DuckDB can output to Polars directly -> .pl()
    df_polars = con.execute(f"""
        SELECT *
        FROM {scan_method}
    """).pl()

    # Análise com Polars
    result = (
        df_polars
        .group_by('region')
        .agg([
            pl.col('total_amount').sum().alias('total_revenue'),
            pl.col('order_id').count().alias('order_count')
        ])
        .sort('total_revenue', descending=True)
    )
    print("Análise Polars:")
    print(result)

except ImportError:
    print("Polars library not installed. Skipping Polars example.")
except Exception as e:
    print(f"Erro Polars: {e}")

# Cleanup
if os.path.exists("./sales"):
    shutil.rmtree("./sales")

print("--- Fim do Capítulo 04 ---")
