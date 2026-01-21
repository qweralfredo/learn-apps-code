# -*- coding: utf-8 -*-
"""
Capítulo 02: Integração DuckDB + Arrow
Curso: Apache Arrow + DuckDB
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pyarrow as pa
import duckdb
import pandas as pd
import numpy as np

print("="*60)
print("CAPÍTULO 02: INTEGRAÇÃO DUCKDB + ARROW")
print("="*60)

# Criar dados de exemplo
n = 100_000
data = pa.table({
    'id': range(n),
    'value': [i * 2.5 for i in range(n)],
    'category': [f'Cat_{i%10}' for i in range(n)]
})

print(f"\nDados: {n:,} linhas")

# Conectar DuckDB
con = duckdb.connect()

# Query de exemplo
result = con.execute("""
    SELECT
        category,
        count(*) as count,
        avg(value) as avg_value,
        sum(value) as sum_value
    FROM data
    GROUP BY category
    ORDER BY category
""").arrow()

print("\nResultado:")
print(result.to_pandas())

print("\n" + "="*60)
print("CAPÍTULO 02 CONCLUÍDO")
print("="*60)
