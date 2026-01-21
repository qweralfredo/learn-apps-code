# -*- coding: utf-8 -*-
"""
Capítulo 03: Arrow Tables e Datasets
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- Criação de Tables
- Schemas e tipos
- Datasets particionados
- Leitura de Parquet
- Filtros e projections
"""

import sys
import io
import pyarrow as pa
import duckdb
import pandas as pd
import numpy as np

# Configuração para saída correta no Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("="*60)
print(f"CAPÍTULO 03: ARROW TABLES E DATASETS")
print("="*60)

# Dados de exemplo globais
try:
    print("\nGerando dados de exemplo...")
    data = pa.table({
        'id': range(1000),
        'valor': np.random.randn(1000),
        'categoria': np.random.choice(['A', 'B', 'C'], 1000)
    })
    print(f"Tabela PyArrow criada: {data.num_rows} linhas")
except Exception as e:
    print(f"Erro ao criar dados: {e}")

# Conexão DuckDB
con = duckdb.connect()


# ==========================================
# Tópico 1: Criação de Tables
# ==========================================
print(f"\n--- {'Criação de Tables'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Criação de Tables
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: Schemas e tipos
# ==========================================
print(f"\n--- {'Schemas e tipos'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Schemas e tipos
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Datasets particionados
# ==========================================
print(f"\n--- {'Datasets particionados'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Datasets particionados
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Leitura de Parquet
# ==========================================
print(f"\n--- {'Leitura de Parquet'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Leitura de Parquet
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Filtros e projections
# ==========================================
print(f"\n--- {'Filtros e projections'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Filtros e projections
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 03")
print("="*60)
