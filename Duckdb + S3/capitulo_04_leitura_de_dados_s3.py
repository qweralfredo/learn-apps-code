# -*- coding: utf-8 -*-
"""
Capítulo 04: Leitura de Dados S3
"""

import duckdb
import pandas as pd
import pathlib

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo 04: Leitura de Dados S3 ---")

# Conexão em memória para testes
con = duckdb.connect(database=':memory:')

# Criação de dados mock para exemplos
con.execute("""
    CREATE TABLE IF NOT EXISTS vendas (
        id INTEGER,
        data DATE,
        produto VARCHAR,
        categoria VARCHAR,
        valor DECIMAL(10,2),
        quantidade INTEGER
    );
    
    INSERT INTO vendas VALUES
    (1, '2023-01-01', 'Notebook', 'Eletronicos', 3500.00, 2),
    (2, '2023-01-02', 'Mouse', 'Perifericos', 50.00, 10),
    (3, '2023-01-03', 'Teclado', 'Perifericos', 120.00, 5),
    (4, '2023-01-04', 'Monitor', 'Eletronicos', 1200.00, 3);
""")
print("Dados de exemplo 'vendas' criados com sucesso.")

# ==============================================================================
# CONTEÚDO DO CAPÍTULO
# ==============================================================================

# -----------------------------------------------------------------------------
# Tópico: Read Parquet S3
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Read Parquet S3")

# TODO: Implementar exemplos práticos para Read Parquet S3
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: Read CSV S3
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Read CSV S3")

# TODO: Implementar exemplos práticos para Read CSV S3
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: Glob Patterns
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Glob Patterns")

# TODO: Implementar exemplos práticos para Glob Patterns
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

print("\n--- Capítulo concluído com sucesso ---")
