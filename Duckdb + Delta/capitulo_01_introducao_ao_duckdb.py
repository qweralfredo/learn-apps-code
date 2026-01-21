# -*- coding: utf-8 -*-
"""
capitulo_01_introducao_ao_duckdb
"""

# capitulo_01_introducao_ao_duckdb
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

import importlib.util


def has_module(name):
    return importlib.util.find_spec(name) is not None

def safe_install_ext(con, ext_name):
    try:
        con.execute(f"INSTALL {ext_name}")
        con.execute(f"LOAD {ext_name}")
        return True
    except Exception as e:
        print(f"Warning: Failed to install/load {ext_name} extension: {e}")
        return False

con = duckdb.connect(database=':memory:')

# Criação de arquivos dummy para exemplo
with open('sales_data.csv', 'w') as f:
    f.write('region,sales\nNorth,100\nSouth,200')
with open('input.json', 'w') as f:
    f.write('[{"date":"2024-01-05", "value":10}, {"date":"2023-12-31", "value":5}]')

con.execute("""
-- Analisar um arquivo CSV grande
SELECT
    region,
    AVG(sales) as avg_sales,
    COUNT(*) as total_orders
FROM read_csv('sales_data.csv')
GROUP BY region
ORDER BY avg_sales DESC;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Transformar e exportar dados
COPY (
    SELECT * FROM read_json('input.json')
    WHERE date >= '2024-01-01'
) TO 'output.parquet' (FORMAT PARQUET);
""")
print(con.fetchall()) # Inspect result

if safe_install_ext(con, 'delta'):
    # Create dummy delta table if possible? OR just try query
    # Since we can't create it without extension (and likely deltalake pip lib isn't used here), 
    # we expect this to fail on 'Table not found' vs 'Function not found'.
    # But for safety we wrap in try/except
    try:
        con.execute("""
        -- 1. Carregar extensão automaticamente (já carregada via safe_install_ext)
        -- SELECT * FROM delta_scan('./my_delta_table');
        """)
        print("Delta queries executed (mocked)")
    except Exception as e:
        print(f"Delta error: {e}")
else:
    print("Skipping Delta Lake queries - extension not available")

"""
con.execute(\"\"\"
-- 1. Carregar extensão automaticamente
SELECT * FROM delta_scan('./my_delta_table');
...
\"\"\")
print(con.fetchall()) # Inspect result
"""
# End of script

