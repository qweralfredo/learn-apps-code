# -*- coding: utf-8 -*-
"""
Capítulo 10: Casos de Uso e Otimizações
Curso: Apache Arrow + DuckDB

Tópicos abordados:
- ETL pipelines
- Data lake architecture
- Incremental loading
- Query optimization
- Best practices
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
print(f"CAPÍTULO 10: CASOS DE USO E OTIMIZAÇÕES")
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
# Tópico 1: ETL pipelines
# ==========================================
print(f"\n--- {'ETL pipelines'.upper()} ---")

# TODO: Implementar exemplos práticos sobre ETL pipelines
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 2: Data lake architecture
# ==========================================
print(f"\n--- {'Data lake architecture'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Data lake architecture
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 3: Incremental loading
# ==========================================
print(f"\n--- {'Incremental loading'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Incremental loading
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 4: Query optimization
# ==========================================
print(f"\n--- {'Query optimization'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Query optimization
# Dica: Use duckdb.query() ou funcoes do pyarrow

# ==========================================
# Tópico 5: Best practices
# ==========================================
print(f"\n--- {'Best practices'.upper()} ---")

# TODO: Implementar exemplos práticos sobre Best practices
# Dica: Use duckdb.query() ou funcoes do pyarrow


print("\n" + "="*60)
print(f"Fim do Capítulo 10")
print("="*60)
