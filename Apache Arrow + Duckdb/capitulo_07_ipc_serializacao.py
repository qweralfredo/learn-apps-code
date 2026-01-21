# -*- coding: utf-8 -*-
"""
Capítulo 07: Arrow IPC e Serialização
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- IPC Format
- Feather format
- Serialização para disco
- Shared memory
- Compressão
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
print(f"CAPÍTULO 07: ARROW IPC E SERIALIZAÇÃO")
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
# Tópico 1: IPC Format
# ==========================================
print(f"\n--- {'IPC Format'.upper()} ---")

# TODO: Implementar exemplos práticos sobre IPC Format
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: Feather format
# ==========================================
print(f"\n--- {'Feather format'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Feather format
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Serialização para disco
# ==========================================
print(f"\n--- {'Serialização para disco'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Serialização para disco
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Shared memory
# ==========================================
print(f"\n--- {'Shared memory'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Shared memory
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Compressão
# ==========================================
print(f"\n--- {'Compressão'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Compressão
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 07")
print("="*60)
