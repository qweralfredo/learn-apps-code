# -*- coding: utf-8 -*-
"""
Capítulo 06: Streaming e Batches
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- Record Batches
- RecordBatchReader
- Processamento incremental
- Iteradores e geradores
- Controle de memória
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
print(f"CAPÍTULO 06: STREAMING E BATCHES")
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
# Tópico 1: Record Batches
# ==========================================
print(f"\n--- {'Record Batches'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Record Batches
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: RecordBatchReader
# ==========================================
print(f"\n--- {'RecordBatchReader'.upper()} ---")

# TODO: Implementar exemplos práticos sobre RecordBatchReader
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Processamento incremental
# ==========================================
print(f"\n--- {'Processamento incremental'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Processamento incremental
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Iteradores e geradores
# ==========================================
print(f"\n--- {'Iteradores e geradores'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Iteradores e geradores
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Controle de memória
# ==========================================
print(f"\n--- {'Controle de memória'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Controle de memória
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 06")
print("="*60)
