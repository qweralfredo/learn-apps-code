# -*- coding: utf-8 -*-
"""
Capítulo 08: Integração Pandas e Polars
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- Conversões Pandas ↔ Arrow
- Conversões Polars ↔ Arrow
- Interoperabilidade DuckDB
- Otimizações de tipos
- Pipelines híbridos
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
print(f"CAPÍTULO 08: INTEGRAÇÃO PANDAS E POLARS")
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
# Tópico 1: Conversões Pandas ↔ Arrow
# ==========================================
print(f"\n--- {'Conversões Pandas ↔ Arrow'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Conversões Pandas ↔ Arrow
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: Conversões Polars ↔ Arrow
# ==========================================
print(f"\n--- {'Conversões Polars ↔ Arrow'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Conversões Polars ↔ Arrow
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Interoperabilidade DuckDB
# ==========================================
print(f"\n--- {'Interoperabilidade DuckDB'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Interoperabilidade DuckDB
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Otimizações de tipos
# ==========================================
print(f"\n--- {'Otimizações de tipos'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Otimizações de tipos
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Pipelines híbridos
# ==========================================
print(f"\n--- {'Pipelines híbridos'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Pipelines híbridos
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 08")
print("="*60)
