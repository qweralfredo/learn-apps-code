# -*- coding: utf-8 -*-
"""
Capítulo 05: Importação e Exportação JSON
"""

import duckdb
import pandas as pd
import pathlib
import os
import json

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo 05: Importação e Exportação JSON ---")

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
        quantidade INTEGER,
        completed BOOLEAN
    );
    
    INSERT INTO vendas VALUES
    (1, '2023-01-01', 'Notebook', 'Eletronicos', 3500.00, 2, true),
    (2, '2023-01-02', 'Mouse', 'Perifericos', 50.00, 10, false),
    (3, '2023-01-03', 'Teclado', 'Perifericos', 120.00, 5, true),
    (4, '2023-01-04', 'Monitor', 'Eletronicos', 1200.00, 3, false);
""")
print("Dados de exemplo 'vendas' criados com sucesso.")

os.makedirs("data_output", exist_ok=True)

# Vamos criar um arquivo JSON 'todos.json' para o exercício
# Usando a própria facilidade do DuckDB para exportar
print("Gerando data_output/todos.json...")
con.execute("COPY vendas TO 'data_output/todos.json' (FORMAT JSON, ARRAY true)")

# E um arquivo NDJSON (Newline Delimited)
print("Gerando data_output/todos.ndjson...")
con.execute("COPY vendas TO 'data_output/todos.ndjson' (FORMAT JSON)")

# ==============================================================================
# CONTEÚDO DO CAPÍTULO
# ==============================================================================

# -----------------------------------------------------------------------------
# Tópico: Leitura Básica de JSON
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Leitura Básica de JSON")

# Leitura Automática
print("Leitura Automática (todos.json):")
con.sql("SELECT * FROM 'data_output/todos.json'").show()

# Usando read_json explicitly
print("Usando read_json:")
con.sql("SELECT * FROM read_json('data_output/todos.json')").show()

# Especificando Schema
print("Especificando Schema:")
con.sql("""
    SELECT * FROM read_json('data_output/todos.json',
        format = 'array',
        columns = {
            id: 'INTEGER',
            produto: 'VARCHAR',
            valor: 'DECIMAL(10,2)'
        }
    )
""").show()

# -----------------------------------------------------------------------------
# Tópico: Leitura de NDJSON
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Leitura de NDJSON")
print("Lendo todos.ndjson:")
con.sql("SELECT * FROM read_json_auto('data_output/todos.ndjson')").show()

# -----------------------------------------------------------------------------
# Tópico: Tipo de Dados JSON Nativo
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Tipo de Dados JSON Nativo")

# Criando tabela com coluna JSON
con.execute("""
    CREATE TABLE IF NOT EXISTS produtos_json (
        id INTEGER,
        detalhes JSON
    );
    INSERT INTO produtos_json VALUES 
    (1, '{"cor": "preto", "dimensoes": {"x": 10, "y": 20}}'),
    (2, '{"cor": "branco", "wireless": true}');
""")

print("Tabela com coluna JSON criada.")
con.sql("SELECT * FROM produtos_json").show()

# Extraindo valores do JSON
print("Extraindo valores (detalhes->'cor'):")
con.sql("SELECT id, detalhes->>'cor' as cor FROM produtos_json").show()

print("\n--- Capítulo concluído com sucesso ---")
