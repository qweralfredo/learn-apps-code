# -*- coding: utf-8 -*-
"""
Capítulo 01: Introdução ao SQL no DuckDB
"""

import duckdb
import pandas as pd
import pathlib

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo 01: Introdução ao SQL no DuckDB ---")

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
# Tópico: Conceitos Básicos
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Conceitos Básicos")

# TODO: Implementar exemplos práticos para Conceitos Básicos
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: SELECT Simples
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: SELECT Simples")

# TODO: Implementar exemplos práticos para SELECT Simples
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: Filtragem
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Filtragem")

# TODO: Implementar exemplos práticos para Filtragem
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

print("\n--- Capítulo concluído com sucesso ---")
