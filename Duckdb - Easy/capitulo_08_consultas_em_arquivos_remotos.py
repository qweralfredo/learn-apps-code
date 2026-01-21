# -*- coding: utf-8 -*-
"""
Capítulo 08: Consultas em Arquivos Remotos
"""

import duckdb
import pandas as pd
import pathlib

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo 08: Consultas em Arquivos Remotos ---")

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
# Tópico: HTTPFS
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: HTTPFS")

# TODO: Implementar exemplos práticos para HTTPFS
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: Reading from URL
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Reading from URL")

# TODO: Implementar exemplos práticos para Reading from URL
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: S3 Basics
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: S3 Basics")

# TODO: Implementar exemplos práticos para S3 Basics
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

print("\n--- Capítulo concluído com sucesso ---")
