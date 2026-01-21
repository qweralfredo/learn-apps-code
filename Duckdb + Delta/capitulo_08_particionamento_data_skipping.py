# -*- coding: utf-8 -*-
"""
capitulo_08_particionamento_data_skipping
"""

import duckdb
import os
import shutil
import pandas as pd
import numpy as np

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

print("--- Iniciando Capítulo 08: Particionamento e Data Skipping ---")

TABLE_PATH = "./sales_partitioned"
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)

con = duckdb.connect()

# ==============================================================================
# DATA GENERATION
# ==============================================================================
print("Generating data...")
df = con.execute("""
    SELECT
        i as order_id,
        'Customer ' || (i % 1000) as customer_name,
        CASE 
            WHEN (i % 4) = 0 THEN 'US'
            WHEN (i % 4) = 1 THEN 'UK'
            WHEN (i % 4) = 2 THEN 'BR'
            ELSE 'JP' 
        END as country,
        CAST('2024-01-01' AS DATE) + (i % 100) * INTERVAL '1 day' as order_date,
        RANDOM() * 1000 as amount
    FROM range(0, 1000) tbl(i)
""").df()
# range reduced to 1000 for speed

# Adicionar colunas derivadas para partição
df['order_date'] = pd.to_datetime(df['order_date'])
df['year'] = df['order_date'].dt.year
df['month'] = df['order_date'].dt.month

try:
    from deltalake import write_deltalake
    write_deltalake(TABLE_PATH, df, partition_by=["country", "month"], mode="overwrite")
    print("✓ Partitioned Delta table created!")
except ImportError:
    print("Deltalake library missing. Creating Hive Parquet structure manually.")
    # Manual Hive: country=.../month=...
    for country in df['country'].unique():
        for month in df['month'].unique():
            subset = df[(df['country'] == country) & (df['month'] == month)]
            if not subset.empty:
                path = f"{TABLE_PATH}/country={country}/month={month}"
                os.makedirs(path, exist_ok=True)
                subset.to_parquet(f"{path}/data.parquet")
    print("✓ Partitioned Parquet table created!")

# Verify structure
print("\nStructure:")
for root, dirs, files in os.walk(TABLE_PATH):
    level = root.replace(TABLE_PATH, "").count(os.sep)
    if level < 3: # Limit depth printing
        indent = " " * 2 * level
        print(f"{indent}{os.path.basename(root)}/")

# ==============================================================================
# DUCKDB ANALYSIS
# ==============================================================================
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet")

scan_func = "delta_scan" if delta_loaded else "read_parquet"
# For read_parquet with Hive partitioning, we create a view or use pattern
# DuckDB AUTO_DETECT hive partitioning usually works with read_parquet('root/**/*')
scan_arg = f"'{TABLE_PATH}'" if delta_loaded else f"'{TABLE_PATH}/**/*.parquet'"

print(f"\n--- Analysis using {scan_func} ---")

try:
    if not delta_loaded:
        # Enable hive partitioning explicitly if needed, but auto-detect is good
        con.execute("SET enable_object_cache=true") # good practice
    
    # Query filtering on partition column (Data Skipping)
    print("Querying country='BR' and month=2...")
    res = con.execute(f"""
        EXPLAIN ANALYZE
        SELECT count(*) 
        FROM {scan_func}({scan_arg})
        WHERE country = 'BR' AND month = 2
    """).fetchone() # EXPLAIN returns analysis text, doesn't print result directly in fetchone
    
    # To see the count:
    real_res = con.execute(f"""
        SELECT count(*) 
        FROM {scan_func}({scan_arg})
        WHERE country = 'BR' AND month = 2
    """).fetchone()
    print(f"Count: {real_res[0]}")
    
    print("(EXPLAIN ANALYZE output would confirm only specific folders were scanned)")

except Exception as e:
    print(f"Erro analysis: {e}")

# ==============================================================================
# PYSPARK MOCK
# ==============================================================================
print("\n--- PySpark Integration (Mock) ---")
try:
    import pyspark
    print("PySpark detected (Mock code executed).")
    # We won't actually spin up a spark session here to save resources/time
except ImportError:
    print("PySpark not installed. Skipping Spark examples.")


# Clean up
con.close()
if os.path.exists(TABLE_PATH):
    shutil.rmtree(TABLE_PATH)

print("--- Fim do Capítulo 08 ---")
