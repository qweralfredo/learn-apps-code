# -*- coding: utf-8 -*-
"""
Capítulo 1: Introdução ao Apache Arrow
Curso: Apache Arrow + DuckDB

Configuração para Windows (UTF-8)
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Instalar dependências
print("Instalando dependências...")
print("pip install pyarrow duckdb pandas")

import pyarrow as pa
import duckdb
import pandas as pd

print("\n" + "="*60)
print("CAPÍTULO 1: INTRODUÇÃO AO APACHE ARROW")
print("="*60)

# 1.1 O que é Apache Arrow?
print("\n1.1 O QUE É APACHE ARROW?")
print("-"*60)

# Criar Arrow table
arrow_table = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Carol'],
    'age': pa.array([30, 25, 35], type=pa.int32())
})

print("\nArrow Table criada:")
print(arrow_table)
print(f"Tipo: {type(arrow_table)}")

# 1.2 Integração com DuckDB
print("\n1.2 INTEGRAÇÃO COM DUCKDB (ZERO-COPY)")
print("-"*60)

# DuckDB pode ler/escrever Arrow diretamente
con = duckdb.connect()

# Query diretamente na Arrow table (ZERO-COPY!)
result = con.execute("SELECT * FROM arrow_table WHERE age > 25").arrow()
print("\nResultado da query (zero-copy):")
print(result)
print(f"Tipo: {type(result)}")

# 1.3 Conversões Pandas <-> Arrow
print("\n1.3 CONVERSÕES PANDAS <-> ARROW")
print("-"*60)

# Criar DataFrame Pandas
df = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=5),
    'sales': [100, 150, 200, 175, 225],
    'region': ['North', 'South', 'North', 'East', 'South']
})

print("\nPandas DataFrame:")
print(df)

# Converter para Arrow (zero-copy quando possível)
arrow_from_pandas = pa.Table.from_pandas(df)
print("\nArrow Table convertida de Pandas:")
print(arrow_from_pandas)

# Query com DuckDB
result_df = con.execute("""
    SELECT
        region,
        sum(sales) as total_sales,
        avg(sales) as avg_sales
    FROM arrow_from_pandas
    GROUP BY region
    ORDER BY total_sales DESC
""").df()

print("\nResultado agregado (voltou para Pandas):")
print(result_df)

# 1.4 Performance Comparison
print("\n1.4 PERFORMANCE COMPARISON")
print("-"*60)

import time

# Criar dados grandes
n = 1_000_000
data = {
    'id': range(n),
    'value': [i * 2.5 for i in range(n)],
    'category': ['A' if i % 3 == 0 else 'B' if i % 3 == 1 else 'C' for i in range(n)]
}

# Teste 1: Pandas DataFrame
df_large = pd.DataFrame(data)
start = time.time()
result_pandas = duckdb.execute("SELECT category, avg(value) FROM df_large GROUP BY category").df()
time_pandas = time.time() - start

# Teste 2: Arrow Table
arrow_large = pa.table(data)
start = time.time()
result_arrow = duckdb.execute("SELECT category, avg(value) FROM arrow_large GROUP BY category").arrow()
time_arrow = time.time() - start

print(f"\nTempo com Pandas: {time_pandas:.4f}s")
print(f"Tempo com Arrow:  {time_arrow:.4f}s")
print(f"Speedup: {time_pandas/time_arrow:.2f}x mais rápido com Arrow!")

# 1.5 Tipos de Dados Arrow
print("\n1.5 TIPOS DE DADOS ARROW")
print("-"*60)

from datetime import datetime, date

# Criar tabela com tipos variados
complex_table = pa.table({
    'id': pa.array([1, 2, 3], type=pa.int32()),
    'name': pa.array(['Alice', 'Bob', 'Carol'], type=pa.string()),
    'balance': pa.array([1234.56, 7890.12, 3456.78], type=pa.decimal128(10, 2)),
    'birth_date': pa.array([date(1990, 1, 15), date(1985, 6, 20), date(1992, 3, 10)], type=pa.date32()),
    'is_active': pa.array([True, False, True], type=pa.bool_())
})

print("\nArrow Table com tipos variados:")
print(complex_table)
print(f"\nSchema completo:\n{complex_table.schema}")

# Query no DuckDB
result_types = con.execute("""
    SELECT
        name,
        balance,
        is_active
    FROM complex_table
    WHERE is_active = true
""").df()

print("\nResultado da query:")
print(result_types)

print("\n" + "="*60)
print("CAPÍTULO 1 CONCLUÍDO!")
print("="*60)
print("\nVocê aprendeu:")
print("[OK] O que é Apache Arrow e por que é importante")
print("[OK] Integração nativa DuckDB + Arrow")
print("[OK] Zero-copy reads e vantagens de performance")
print("[OK] Como criar Arrow tables")
print("[OK] Conversão entre Pandas e Arrow")
print("[OK] Tipos de dados suportados")
