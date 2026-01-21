# -*- coding: utf-8 -*-
"""
capitulo_09_integracao_python_spark
"""

import duckdb
import os
import shutil
import pandas as pd
import numpy as np
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

print("--- Iniciando Capítulo 09: Integração Python e Spark ---")

# ==============================================================================
# DATA SETUP
# ==============================================================================
SALES_PATH = "./sales"
if os.path.exists(SALES_PATH):
    shutil.rmtree(SALES_PATH)

print("Setup: Creating Mock './sales' table...")
df_sales = pd.DataFrame({
    'customer_id': range(100),
    'order_date': [datetime(2024, 1, 1)] * 100,
    'amount': np.random.rand(100) * 200,
    'region': 'US'
})

try:
    from deltalake import write_deltalake
    write_deltalake(SALES_PATH, df_sales)
    print("Mock Delta table created.")
except ImportError:
    print("Deltalake library not installed. Using Parquet fallback.")
    os.makedirs(SALES_PATH, exist_ok=True)
    df_sales.to_parquet(os.path.join(SALES_PATH, "part-001.parquet"))

# ==============================================================================
# DUCKDB & PANDAS
# ==============================================================================
con = duckdb.connect()
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet")

scan_func = "delta_scan" if delta_loaded else "read_parquet"
scan_arg = f"'{SALES_PATH}'" if delta_loaded else f"'{SALES_PATH}/**/*.parquet'"

print("\n--- 1. Pandas DataFrame Integration ---")
try:
    df = con.execute(f"""
        SELECT * FROM {scan_func}({scan_arg}) LIMIT 5
    """).df()
    print(df.head())
except Exception as e:
    print(f"Erro Pandas 1: {e}")

print("\n--- 2. Filtering Pushdown to Pandas ---")
try:
    df_filtered = con.execute(f"""
        SELECT customer_id, order_date, amount
        FROM {scan_func}({scan_arg})
        WHERE amount > 100
    """).df()
    print(f"Rows loaded (amount > 100): {len(df_filtered)}")
except Exception as e:
    print(f"Erro Pandas 2: {e}")

print("\n--- 3. Writing Delta from Pandas (Mock) ---")
try:
    from deltalake import write_deltalake
    df_prod = pd.DataFrame({'id': range(5), 'name': ['Prod']*5, 'price': [10.0]*5})
    write_deltalake("./products_mock", df_prod, mode='overwrite')
    print("Write OK.")
    if os.path.exists("./products_mock"):
        shutil.rmtree("./products_mock")
except ImportError:
    print("Skipped (deltalake missing)")

# ==============================================================================
# MOCK SPARK
# ==============================================================================
print("\n--- 4. Spark Integration (Mock) ---")
try:
    import pyspark
    from pyspark.sql import SparkSession
    print("PySpark installed. Starting Mock Session...")
    # spark = SparkSession.builder.getOrCreate()
    # df_spark = spark.read.format("delta").load(SALES_PATH)
    # df_spark.createOrReplaceTempView("sales_view")
    # ...
    # spark.stop()
except ImportError:
    print("PySpark not installed. Conceptual examples skipped.")

# Clean up
con.close()
if os.path.exists(SALES_PATH):
    shutil.rmtree(SALES_PATH)

print("--- Fim do Capítulo 09 ---")
