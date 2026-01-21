# -*- coding: utf-8 -*-
"""
Capítulo 05: Arrow Flight SQL
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- Arquitetura Flight SQL
- Servidor e cliente
- Streaming via gRPC
- Prepared statements
- Autenticação
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
print(f"CAPÍTULO 05: ARROW FLIGHT SQL")
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
# Tópico 1: Arquitetura Flight SQL
# ==========================================
print(f"\n--- {'Arquitetura Flight SQL'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Arquitetura Flight SQL
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: Servidor e cliente
# ==========================================
print(f"\n--- {'Servidor e cliente'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Servidor e cliente
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Streaming via gRPC
# ==========================================
print(f"\n--- {'Streaming via gRPC'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Streaming via gRPC
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Prepared statements
# ==========================================
print(f"\n--- {'Prepared statements'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Prepared statements
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Autenticação
# ==========================================
print(f"\n--- {'Autenticação'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Autenticação
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 05")
print("="*60)
