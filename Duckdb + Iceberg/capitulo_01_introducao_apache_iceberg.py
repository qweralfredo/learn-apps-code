# -*- coding: utf-8 -*-
"""
capitulo_01_introducao_apache_iceberg
"""

import duckdb
import os
import shutil
import pandas as pd
from pyiceberg.catalog import load_catalog

# Helper
def safe_install_ext(con, ext_name):
    try:
        con.install_extension(ext_name)
        con.load_extension(ext_name)
        print(f"Extension '{ext_name}' loaded.")
        return True
    except:
        return False

print("--- Iniciando Capítulo 01: Introdução Apache Iceberg ---")

# ==============================================================================
# DATA SETUP (PyIceberg)
# ==============================================================================
WAREHOUSE_PATH = "./iceberg_warehouse"
if os.path.exists(WAREHOUSE_PATH):
    shutil.rmtree(WAREHOUSE_PATH)
os.makedirs(WAREHOUSE_PATH, exist_ok=True)

print("Creating Iceberg Table via PyIceberg...")
try:
    # SQL-based Catalog using SQLite or FileSystem?
    # PyIceberg defaults to Rest, Hive, Glue, Sql.
    # We use "sql" catalog with sqlite for local testing.
    
    catalog = load_catalog(
        "local", 
        **{
            "type": "sql", 
            "uri": f"sqlite:///{WAREHOUSE_PATH}/catalog.db",
            "warehouse": f"file://{os.path.abspath(WAREHOUSE_PATH)}"
        }
    )
    
    df_pandas = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
    import pyarrow as pa
    df_arrow = pa.Table.from_pandas(df_pandas)
    
    try:
        ns = catalog.create_namespace("default")
    except:
        pass # exists

    table = catalog.create_table(
        "default.my_table",
        schema=df_arrow.schema,
    )
    table.append(df_arrow)
    print("Table 'default.my_table' created successfully using PyIceberg.")
    
    # Path to metadata location for DuckDB
    # PyIceberg stores it. We need to find the metadata.json
    # Default file layout: warehouse/default/my_table/metadata/...
    table_loc = table.location.replace("file://", "")
    # Usually: WAREHOUSE_PATH + /default/my_table
    
except Exception as e:
    print(f"PyIceberg setup failed: {e}")
    # Fallback handled below

# ==============================================================================
# DUCKDB SCAN
# ==============================================================================
con = duckdb.connect()
iceberg_loaded = safe_install_ext(con, "iceberg")
safe_install_ext(con, "parquet")

print("\n--- Reading with DuckDB ---")
try:
    if iceberg_loaded:
        # Search for metadata file
        # Check standard path
        search_path = os.path.join(WAREHOUSE_PATH, "default", "my_table", "metadata")
        metadata_file = None
        if os.path.exists(search_path):
            files = [f for f in os.listdir(search_path) if f.endswith(".metadata.json")]
            if files:
                # Get latest - sort by name usually works as they have versions v1, v2
                files.sort()
                metadata_file = os.path.join(search_path, files[-1])
        
        if metadata_file:
            print(f"Found metadata: {metadata_file}")
            res = con.execute(f"SELECT * FROM iceberg_scan('{metadata_file}')").df()
            print(res)
        else:
            print("Could not find metadata file to scan.")
    else:
        print("Iceberg extension not loaded. Skipping scan.")

except Exception as e:
    print(f"DuckDB Query Error: {e}")

print("--- Fim do Capítulo 01 ---")
