# -*- coding: utf-8 -*-
"""
Capítulo 04: Trabalhando com Parquet
"""

import duckdb
import pandas as pd
import pathlib
import os

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo 04: Trabalhando com Parquet ---")

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

# Create output directory for parquet files
os.makedirs("data_output", exist_ok=True)

# ==============================================================================
# CONTEÚDO DO CAPÍTULO
# ==============================================================================

# -----------------------------------------------------------------------------
# Tópico: Escrita de Arquivos (Preparação)
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Escrita de Parquet (Prepared)")

# Exportar tabela vendas para Parquet (Snappy default)
con.execute("COPY vendas TO 'data_output/vendas.parquet' (FORMAT parquet)")
print("Arquivo data_output/vendas.parquet criado.")

# Exportar com compressão ZSTD
con.execute("COPY vendas TO 'data_output/vendas_zstd.parquet' (FORMAT parquet, COMPRESSION zstd)")
print("Arquivo data_output/vendas_zstd.parquet criado.")

# -----------------------------------------------------------------------------
# Tópico: Read Parquet
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Read Parquet")

# Leitura Simples
print("Leitura Simples:")
con.sql("SELECT * FROM 'data_output/vendas.parquet'").show()

# Usando read_parquet explicitamente
print("Usando read_parquet:")
con.sql("SELECT * FROM read_parquet('data_output/vendas.parquet')").show()

# Descobrindo o Schema
print("Schema do arquivo:")
con.sql("DESCRIBE SELECT * FROM 'data_output/vendas.parquet'").show()

# -----------------------------------------------------------------------------
# Tópico: Performance e Metadados
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Metadados Parquet")

# Parquet Metadata
try:
    print("Metadata Geral:")
    con.sql("SELECT * FROM parquet_metadata('data_output/vendas.parquet')").show()
except Exception as e:
    print(f"Erro ao ler metadata (pode depender da versão do DuckDB): {e}")

# Parquet Schema
try:
    print("Parquet Schema:")
    con.sql("SELECT * FROM parquet_schema('data_output/vendas.parquet')").show()
except Exception as e:
    print(f"Erro ao ler schema: {e}")

# -----------------------------------------------------------------------------
# Tópico: Partitioned Writes (Simulação)
# -----------------------------------------------------------------------------
print(f"\n>>> Executando: Partitioned Writes")

# Exportar particionado por categoria
con.execute("""
    COPY vendas TO 'data_output/vendas_particionadas' 
    (FORMAT parquet, PARTITION_BY (categoria), OVERWRITE_OR_IGNORE)
""")
print("Dados particionados em data_output/vendas_particionadas/")

# Ler dados particionados (Hive partitioning)
print("Lendo dados particionados (hive partitioning):")
con.sql("SELECT * FROM read_parquet('data_output/vendas_particionadas/**/*.parquet', hive_partitioning=true)").show()


print("\n--- Capítulo concluído com sucesso ---")
