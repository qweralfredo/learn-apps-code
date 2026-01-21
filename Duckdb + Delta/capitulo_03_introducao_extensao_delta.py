# -*- coding: utf-8 -*-
"""
capitulo_03_introducao_extensao_delta
"""

import duckdb
import os
import shutil

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

print("--- Iniciando Capítulo 03: Introdução à Extensão Delta ---")

con = duckdb.connect()
delta_loaded = safe_install_ext(con, "delta")
safe_install_ext(con, "parquet")

# Exemplo/Bloco 1
print("\n--- Criando Tabela Delta (via python-deltalake) ---")
try:
    from deltalake import write_deltalake
    import pandas as pd
    
    # Criar dados de exemplo
    df = con.execute("""
        SELECT
            i as id,
            i % 10 as category,
            i % 2 as partition_col,
            'value-' || i as description,
            CURRENT_DATE - (i % 100) * INTERVAL '1 day' as created_date,
            RANDOM() * 1000 as amount
        FROM range(0, 1000) tbl(i)
    """).df()

    # Clean up previous run
    if os.path.exists("./my_delta_table"):
        shutil.rmtree("./my_delta_table")

    # Escrever como tabela Delta (particionada)
    write_deltalake(
        "./my_delta_table",
        df,
        partition_by=["partition_col"],
        mode="overwrite"
    )
    print("Delta table created successfully at ./my_delta_table")

    print("\n--- Lendo Tabela Delta ---")
    if delta_loaded:
        result = con.execute("""
            SELECT
                partition_col,
                COUNT(*) as total_rows,
                AVG(amount) as avg_amount
            FROM delta_scan('./my_delta_table')
            GROUP BY partition_col
            ORDER BY partition_col
        """).fetchdf()
        print("Resultado via 'delta_scan':")
        print(result)
    else:
        print("Extensão 'delta' não disponível. Tentando ler como Parquet puro (fallback)...")
        # Fallback: Read underlying parquet files
        # Note: This is an approximation, ignores transaction log
        result = con.execute("""
            SELECT
                partition_col,
                COUNT(*) as total_rows,
                AVG(amount) as avg_amount
            FROM read_parquet('./my_delta_table/**/*.parquet')
            GROUP BY partition_col
            ORDER BY partition_col
        """).fetchdf()
        print("Resultado via 'read_parquet' (Aproximação):")
        print(result)

except ImportError:
    print("Biblioteca 'deltalake' não instalada. Exemplo ignorado.")
except Exception as e:
    print(f"Erro no exemplo 1: {e}")

# Clean up
if os.path.exists("./my_delta_table"):
    shutil.rmtree("./my_delta_table")

print("--- Fim do Capítulo 03 ---")
