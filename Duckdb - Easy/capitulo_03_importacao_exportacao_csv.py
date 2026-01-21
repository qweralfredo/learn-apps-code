# -*- coding: utf-8 -*-
"""
Capítulo 03: Importação Exportação CSV
"""

import duckdb
import pandas as pd
import pathlib

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo 03: Importação Exportação CSV ---")

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
# Tópico: Read CSV
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Read CSV")

# TODO: Implementar exemplos práticos para Read CSV
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: Read CSV Auto
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Read CSV Auto")

# TODO: Implementar exemplos práticos para Read CSV Auto
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

# -----------------------------------------------------------------------------
# Tópico: Copy To CSV
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Copy To CSV")

# TODO: Implementar exemplos práticos para Copy To CSV
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()

print("\n--- Capítulo concluído com sucesso ---")
