# -*- coding: utf-8 -*-
"""
Capítulo 1: Introdução ao DuckDB e S3
=====================================

Este script demonstra os conceitos básicos do DuckDB e sua integração com S3.

Autor: Curso DuckDB + S3
Data: 2024
"""

import duckdb
import os
from pathlib import Path

# Configuração para Windows (UTF-8)
if os.name == 'nt':
    os.system('chcp 65001 > nul')

print("=" * 70)
print("CAPÍTULO 1: INTRODUÇÃO AO DUCKDB E S3")
print("=" * 70)

# =============================================================================
# 1. O QUE É DUCKDB?
# =============================================================================
print("\n1. O que é DuckDB?")
print("-" * 70)
print("""
DuckDB é um sistema de gerenciamento de banco de dados analítico:
- In-process (sem servidor dedicado)
- SQL OLAP (Online Analytical Processing)
- Projetado para consultas analíticas complexas
- Executa diretamente sobre arquivos de dados
""")

# Criar conexão com DuckDB (em memória)
conn = duckdb.connect(':memory:')

# Verificar versão do DuckDB
version = conn.execute("SELECT version()").fetchone()[0]
print(f"✓ DuckDB Version: {version}\n")

# =============================================================================
# 2. CRIANDO DADOS LOCAIS DE EXEMPLO
# =============================================================================
print("\n2. Criando dados de exemplo localmente")
print("-" * 70)

# Criar diretório para dados de exemplo
data_dir = Path("./sample_data")
data_dir.mkdir(exist_ok=True)

# Criar tabela de vendas de exemplo
print("Criando tabela de vendas...")
conn.execute("""
    CREATE TABLE sales AS
    SELECT
        range as id,
        'Product_' || (range % 10) as product,
        (random() * 1000)::INTEGER as amount,
        DATE '2024-01-01' + INTERVAL (range % 90) DAY as date
    FROM range(1000)
""")

# Verificar dados criados
print("\nPrimeiras 10 linhas:")
result = conn.execute("SELECT * FROM sales LIMIT 10").fetchdf()
print(result)

print(f"\n✓ Total de registros: {conn.execute('SELECT count(*) FROM sales').fetchone()[0]}")

# =============================================================================
# 3. DEMONSTRAÇÃO: ESCRITA LOCAL (SIMULANDO S3)
# =============================================================================
print("\n3. Demonstração: Escrita de arquivo Parquet local")
print("-" * 70)

# Escrever para arquivo Parquet local
parquet_file = data_dir / "sales.parquet"
conn.execute(f"""
    COPY sales TO '{parquet_file}' (FORMAT parquet)
""")
print(f"✓ Arquivo criado: {parquet_file}")
print(f"✓ Tamanho: {parquet_file.stat().st_size:,} bytes")

# =============================================================================
# 4. DEMONSTRAÇÃO: LEITURA DIRETA DE PARQUET
# =============================================================================
print("\n4. Demonstração: Leitura direta de arquivo Parquet")
print("-" * 70)

# Ler o arquivo Parquet
print("Lendo arquivo Parquet...")
result = conn.execute(f"SELECT * FROM '{parquet_file}' LIMIT 5").fetchdf()
print(result)

# Análise agregada
print("\nAnálise agregada - Total de vendas por produto:")
result = conn.execute(f"""
    SELECT
        product,
        count(*) as transactions,
        sum(amount) as total_sales,
        avg(amount)::INTEGER as avg_amount
    FROM '{parquet_file}'
    GROUP BY product
    ORDER BY total_sales DESC
    LIMIT 5
""").fetchdf()
print(result)

# =============================================================================
# 5. FORMATOS SUPORTADOS
# =============================================================================
print("\n5. Formatos suportados pelo DuckDB")
print("-" * 70)

# CSV
csv_file = data_dir / "sales.csv"
conn.execute(f"""
    COPY (SELECT * FROM sales LIMIT 100)
    TO '{csv_file}' (FORMAT csv, HEADER true)
""")
print(f"✓ CSV criado: {csv_file}")

# JSON
json_file = data_dir / "sales.json"
conn.execute(f"""
    COPY (SELECT * FROM sales LIMIT 100)
    TO '{json_file}' (FORMAT json)
""")
print(f"✓ JSON criado: {json_file}")

# Ler CSV
print("\nLendo CSV:")
result = conn.execute(f"""
    SELECT count(*) as total
    FROM read_csv_auto('{csv_file}')
""").fetchdf()
print(result)

# =============================================================================
# 6. GLOBBING: LEITURA DE MÚLTIPLOS ARQUIVOS
# =============================================================================
print("\n6. Demonstração: Globbing (padrões de arquivo)")
print("-" * 70)

# Criar múltiplos arquivos Parquet
for i in range(3):
    file_path = data_dir / f"sales_part_{i}.parquet"
    conn.execute(f"""
        COPY (
            SELECT * FROM sales
            WHERE id BETWEEN {i * 300} AND {(i + 1) * 300 - 1}
        ) TO '{file_path}' (FORMAT parquet)
    """)
    print(f"✓ Criado: sales_part_{i}.parquet")

# Ler todos os arquivos usando glob pattern
print("\nLendo múltiplos arquivos com globbing:")
glob_pattern = str(data_dir / "sales_part_*.parquet").replace('\\', '/')
result = conn.execute(f"""
    SELECT count(*) as total_records
    FROM '{glob_pattern}'
""").fetchdf()
print(result)

# =============================================================================
# 7. METADADOS DE PARQUET
# =============================================================================
print("\n7. Explorar metadados de arquivo Parquet")
print("-" * 70)

# Ver schema
print("Schema do arquivo:")
result = conn.execute(f"""
    SELECT * FROM parquet_schema('{parquet_file}')
""").fetchdf()
print(result)

# Ver metadados
print("\nMetadados do arquivo (primeiras linhas):")
result = conn.execute(f"""
    SELECT
        file_name,
        row_group_id,
        num_values,
        total_compressed_size,
        total_uncompressed_size
    FROM parquet_metadata('{parquet_file}')
    LIMIT 5
""").fetchdf()
print(result)

# =============================================================================
# 8. EXEMPLO PRÁTICO: ANÁLISE DE VENDAS
# =============================================================================
print("\n8. Exemplo prático: Análise de vendas")
print("-" * 70)

# Análise temporal
print("Vendas por semana:")
result = conn.execute(f"""
    SELECT
        date_trunc('week', date) as week,
        count(*) as transactions,
        sum(amount) as total_sales,
        avg(amount)::INTEGER as avg_order_value
    FROM '{parquet_file}'
    GROUP BY week
    ORDER BY week
    LIMIT 10
""").fetchdf()
print(result)

# =============================================================================
# 9. SIMULAÇÃO DE ACESSO S3 (COM ARQUIVOS LOCAIS)
# =============================================================================
print("\n9. Simulação: Estrutura similar ao S3")
print("-" * 70)

# Criar estrutura hierárquica (simulando S3)
s3_sim_dir = data_dir / "s3-simulation" / "bucket" / "data" / "2024"
s3_sim_dir.mkdir(parents=True, exist_ok=True)

# Criar arquivos em estrutura hierárquica
for month in range(1, 4):
    month_dir = s3_sim_dir / f"{month:02d}"
    month_dir.mkdir(exist_ok=True)

    file_path = month_dir / "sales.parquet"
    conn.execute(f"""
        COPY (
            SELECT * FROM sales
            WHERE EXTRACT(month FROM date) = {month}
        ) TO '{file_path}' (FORMAT parquet)
    """)
    print(f"✓ Criado: {file_path}")

# Ler todos usando glob recursivo
glob_pattern = str(s3_sim_dir.parent / "**" / "*.parquet").replace('\\', '/')
print(f"\nLendo estrutura hierárquica: {glob_pattern}")
result = conn.execute(f"""
    SELECT
        count(*) as total_records,
        count(DISTINCT product) as unique_products,
        sum(amount) as total_sales
    FROM '{glob_pattern}'
""").fetchdf()
print(result)

# =============================================================================
# 10. EXERCÍCIOS PRÁTICOS
# =============================================================================
print("\n10. Exercícios práticos")
print("-" * 70)

print("""
Exercícios para praticar:

1. Criar uma tabela com 10.000 registros e salvar em Parquet
   HINT: Use range(10000) e random()

2. Ler o arquivo e calcular estatísticas (min, max, avg)
   HINT: Use funções agregadas

3. Criar 5 arquivos particionados por categoria
   HINT: Use WHERE para filtrar e múltiplos COPY

4. Ler todos os arquivos e fazer análise agregada
   HINT: Use glob pattern com *

5. Explorar metadados: quantos row groups existem?
   HINT: Use parquet_metadata()
""")

# Exercício 1 - Solução
print("\nExercício 1 - Solução:")
conn.execute("""
    CREATE TABLE large_dataset AS
    SELECT
        range as id,
        'Category_' || (range % 5) as category,
        (random() * 10000)::INTEGER as value,
        current_timestamp() as created_at
    FROM range(10000)
""")

exercise_file = data_dir / "exercise_01.parquet"
conn.execute(f"""
    COPY large_dataset TO '{exercise_file}' (FORMAT parquet)
""")
print(f"✓ Criado arquivo com 10.000 registros: {exercise_file}")

# Exercício 2 - Solução
print("\nExercício 2 - Solução:")
result = conn.execute(f"""
    SELECT
        min(value) as min_value,
        max(value) as max_value,
        avg(value)::INTEGER as avg_value,
        count(*) as total_records
    FROM '{exercise_file}'
""").fetchdf()
print(result)

# =============================================================================
# LIMPEZA E FECHAMENTO
# =============================================================================
print("\n" + "=" * 70)
print("DEMONSTRAÇÃO CONCLUÍDA!")
print("=" * 70)
print(f"""
Arquivos criados em: {data_dir.absolute()}

Próximos passos:
1. No Capítulo 2: Instalar e configurar extensão httpfs
2. No Capítulo 3: Configurar credenciais AWS
3. No Capítulo 4: Ler dados reais do S3

Para limpar os arquivos de exemplo:
- Remova manualmente a pasta: {data_dir.absolute()}
""")

# Fechar conexão
conn.close()
print("\n✓ Conexão fechada com sucesso!")
