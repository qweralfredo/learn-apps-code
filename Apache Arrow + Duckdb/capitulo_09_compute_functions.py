# -*- coding: utf-8 -*-
"""
Capítulo 09: Arrow Compute Functions
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- Operações vetorizadas
- String operations
- Filtros e máscaras
- Agregações
- Joins
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
print(f"CAPÍTULO 09: ARROW COMPUTE FUNCTIONS")
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
# Tópico 1: Operações vetorizadas
# ==========================================
print(f"\n--- {'Operações vetorizadas'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Operações vetorizadas
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: String operations
# ==========================================
print(f"\n--- {'String operations'.upper()} ---")

# TODO: Implementar exemplos práticos sobre String operations
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Filtros e máscaras
# ==========================================
print(f"\n--- {'Filtros e máscaras'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Filtros e máscaras
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Agregações
# ==========================================
print(f"\n--- {'Agregações'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Agregações
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Joins
# ==========================================
print(f"\n--- {'Joins'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Joins
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 09")
print("="*60)
