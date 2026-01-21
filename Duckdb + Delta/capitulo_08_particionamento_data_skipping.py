# -*- coding: utf-8 -*-
"""
capitulo-08-particionamento-data-skipping
"""

# capitulo-08-particionamento-data-skipping
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
from deltalake import write_deltalake

con = duckdb.connect()

# Criar DataFrame com dados para particionar
df = con.execute("""
    SELECT
        i as order_id,
        'Customer ' || (i % 1000) as customer_name,
        ['US', 'UK', 'BR', 'JP'][i % 4 + 1] as country,
        CAST('2024-01-01' AS DATE) + (i % 365) * INTERVAL '1 day' as order_date,
        RANDOM() * 1000 as amount
    FROM range(0, 100000) tbl(i)
""").df()

# Adicionar colunas de partição explícitas
df['year'] = df['order_date'].dt.year
df['month'] = df['order_date'].dt.month

# Escrever tabela particionada
write_deltalake(
    "./sales_partitioned",
    df,
    partition_by=["year", "month"],
    mode="overwrite"
)

print("✓ Partitioned table created!")

# Verificar estrutura
import os
for root, dirs, files in os.walk("./sales_partitioned"):
    level = root.replace("./sales_partitioned", "").count(os.sep)
    indent = " " * 2 * level
    print(f"{indent}{os.path.basename(root)}/")

# Exemplo/Bloco 2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Criar DataFrame
df = spark.range(0, 100000).selectExpr(
    "id as order_id",
    "concat('Customer ', id % 1000) as customer_name",
    "date_add('2024-01-01', cast(id % 365 as int)) as order_date",
    "rand() * 1000 as amount"
)

# Adicionar colunas de partição
df = df.withColumn("year", year(col("order_date"))) \
       .withColumn("month", month(col("order_date")))

# Escrever particionado
df.write \
    .format("delta") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .save("./sales_partitioned_spark")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()

# Habilitar profiling
con.execute("SET enable_profiling=true")
con.execute("SET profiling_mode='detailed'")

# Query com partition filter
result = con.execute("""
    SELECT COUNT(*)
    FROM delta_scan('./sales_partitioned')
    WHERE year = 2024 AND month = 1
""").fetchone()

print(f"Result: {result[0]:,} rows")

# Ver quais arquivos foram lidos
profile = con.execute("PRAGMA last_profiling_output").fetchone()[0]
print("\nProfile:")
print(profile)

# Buscar por "files read" ou "partitions scanned"

# Exemplo/Bloco 4
partition_by=["year", "month"]
   partition_by=["date"]

# Exemplo/Bloco 5
partition_by=["region"]  # 5-10 regiões
   partition_by=["country"]  # ~200 países

# Exemplo/Bloco 6
partition_by=["year", "month", "day"]
   partition_by=["country", "state"]

# Exemplo/Bloco 7
partition_by=["customer_id"]  # Milhões de valores
   partition_by=["order_id"]  # Cada valor é único

# Exemplo/Bloco 8
partition_by=["description"]  # Nunca usado em WHERE

# Exemplo/Bloco 9
partition_by=["year", "month", "day", "hour", "minute"]  # Muito granular

# Exemplo/Bloco 10
import duckdb

def analyze_partitions(table_path: str):
    """
    Analisar distribuição de tamanho das partições
    """
    con = duckdb.connect()

    # Não há função direta, mas podemos usar filesystem
    import os

    partition_sizes = {}

    for root, dirs, files in os.walk(table_path):
        if files and root != table_path and '_delta_log' not in root:
            # Calcular tamanho da partição
            size = sum(
                os.path.getsize(os.path.join(root, f))
                for f in files
                if f.endswith('.parquet')
            )
            partition = root.replace(table_path, '').strip('/')
            partition_sizes[partition] = size

    # Análise
    total_size = sum(partition_sizes.values())
    avg_size = total_size / len(partition_sizes) if partition_sizes else 0

    print(f"Total partitions: {len(partition_sizes)}")
    print(f"Total size: {total_size / 1024**3:.2f} GB")
    print(f"Average partition size: {avg_size / 1024**2:.2f} MB")

    # Partições muito pequenas ou grandes
    small = [p for p, s in partition_sizes.items() if s < 10*1024**2]  # < 10MB
    large = [p for p, s in partition_sizes.items() if s > 10*1024**3]  # > 10GB

    if small:
        print(f"\n⚠ Warning: {len(small)} partitions < 10MB")
        print("Consider using coarser partitioning")

    if large:
        print(f"\n⚠ Warning: {len(large)} partitions > 10GB")
        print("Consider using finer partitioning")

# Uso
analyze_partitions("./sales_partitioned")

# Exemplo/Bloco 11
import duckdb
import time

def measure_data_skipping(table_path: str, filter_clause: str):
    """
    Medir efetividade de data skipping
    """
    con = duckdb.connect()
    con.execute("SET enable_profiling=true")

    # Query com filtro
    query = f"""
        SELECT COUNT(*), AVG(amount)
        FROM delta_scan('{table_path}')
        WHERE {filter_clause}
    """

    start = time.time()
    result = con.execute(query).fetchone()
    elapsed = time.time() - start

    print(f"Filter: {filter_clause}")
    print(f"Results: {result}")
    print(f"Time: {elapsed:.3f}s")

    # Ver profile para entender quais arquivos foram lidos
    # (DuckDB não expõe isso diretamente, mas pode inferir do tempo)

    return elapsed

# Comparar diferentes filtros
print("=== FILTER EFFECTIVENESS ===\n")

# Filtro seletivo (deve ser rápido)
t1 = measure_data_skipping(
    './sales_partitioned',
    "year = 2024 AND month = 1 AND amount > 9000"
)

print()

# Filtro menos seletivo (mais lento)
t2 = measure_data_skipping(
    './sales_partitioned',
    "amount > 100"
)

print(f"\nSpeedup with selective filter: {t2/t1:.2f}x")

# Exemplo/Bloco 12
import duckdb
from deltalake import write_deltalake

con = duckdb.connect()

# Ler tabela não-particionada
df = con.execute("""
    SELECT * FROM delta_scan('./sales_old')
""").df()

# Adicionar colunas de partição
df['year'] = df['order_date'].dt.year
df['month'] = df['order_date'].dt.month

# Escrever nova tabela particionada
write_deltalake(
    "./sales_new_partitioned",
    df,
    partition_by=["year", "month"],
    mode="overwrite"
)

print("✓ Table repartitioned!")

# Exemplo/Bloco 13
# Particionamento atual: muito granular (por dia)
# Novo particionamento: por mês

con = duckdb.connect()

# Ler tabela com partições diárias
df = con.execute("""
    SELECT * FROM delta_scan('./sales_daily_partitions')
""").df()

# Re-particionar por mês
df['year'] = df['order_date'].dt.year
df['month'] = df['order_date'].dt.month

write_deltalake(
    "./sales_monthly_partitions",
    df,
    partition_by=["year", "month"],
    mode="overwrite"
)

# Exemplo/Bloco 14
import duckdb
from deltalake import write_deltalake
from datetime import datetime, timedelta
import pandas as pd

def create_temporal_partitioned_table():
    """
    Criar tabela otimizada para queries temporais
    """
    con = duckdb.connect()

    # Gerar dados para 2 anos
    df = con.execute("""
        SELECT
            i as event_id,
            'Event-' || i as event_name,
            CAST('2023-01-01' AS DATE) + (i % 730) * INTERVAL '1 day' as event_date,
            ['web', 'mobile', 'api'][i % 3 + 1] as source,
            ['US', 'UK', 'BR', 'JP', 'DE'][i % 5 + 1] as country,
            RANDOM() * 100 as metric_value
        FROM range(0, 1000000) tbl(i)
    """).df()

    # Adicionar colunas de partição hierárquicas
    df['year'] = df['event_date'].dt.year
    df['month'] = df['event_date'].dt.month

    # Escrever particionado
    write_deltalake(
        "./events_partitioned",
        df,
        partition_by=["year", "month"],
        mode="overwrite"
    )

    print("✓ Temporal table created with 2 years of data")
    print(f"  Total rows: {len(df):,}")
    print(f"  Partitions: year × month = 24 partitions")

    return df

def benchmark_temporal_queries(table_path: str):
    """
    Benchmark queries temporais
    """
    con = duckdb.connect()
    queries = [
        ("Single month", "year = 2024 AND month = 1"),
        ("Quarter", "year = 2024 AND month BETWEEN 1 AND 3"),
        ("Year", "year = 2024"),
        ("Last 30 days", "event_date >= '2024-12-01'"),
    ]

    print("\n=== TEMPORAL QUERY PERFORMANCE ===")

    for name, where_clause in queries:
        import time

        query = f"""
            SELECT
                COUNT(*) as events,
                COUNT(DISTINCT country) as countries,
                AVG(metric_value) as avg_metric
            FROM delta_scan('{table_path}')
            WHERE {where_clause}
        """

        start = time.time()
        result = con.execute(query).fetchone()
        elapsed = time.time() - start

        print(f"\n{name}:")
        print(f"  Filter: {where_clause}")
        print(f"  Events: {result[0]:,}")
        print(f"  Time: {elapsed:.3f}s")

if __name__ == "__main__":
    # Criar tabela
    df = create_temporal_partitioned_table()

    # Benchmark
    benchmark_temporal_queries("./events_partitioned")

# Exemplo/Bloco 15
# Nota: write support ainda não disponível no DuckDB
# Deletion vectors são criados por Spark/Databricks
# DuckDB lê corretamente tabelas com deletion vectors

import duckdb

con = duckdb.connect()

# Ler tabela com deletion vectors
# (DuckDB automaticamente aplica deletion vectors)
result = con.execute("""
    SELECT COUNT(*)
    FROM delta_scan('./table_with_deletes')
""").fetchone()

print(f"Rows (após aplicar deletion vectors): {result[0]:,}")

# Exemplo/Bloco 16
# Análise de padrões de query
common_filters = {
    "temporal": 0.80,  # 80% queries filtram por data
    "region": 0.40,    # 40% filtram por região
    "status": 0.20     # 20% filtram por status
}

# Particionar pelas colunas mais filtradas
# partition_by=["date", "region"]

# Exemplo/Bloco 17
# ❌ Ruim: muitas partições pequenas
partition_by=["year", "month", "day", "hour"]

# ✓ Bom: partições balanceadas
partition_by=["year", "month"]

# Exemplo/Bloco 18
# Script de monitoramento periódico
def check_partition_health(table_path):
    sizes = analyze_partition_sizes(table_path)

    if any(s < 10*1024**2 for s in sizes):  # < 10MB
        print("⚠ Consider consolidating small partitions")

    if any(s > 10*1024**3 for s in sizes):  # > 10GB
        print("⚠ Consider splitting large partitions")

# Exemplo/Bloco 19
# Via Databricks Delta Lake
# (DuckDB não cria, mas lê eficientemente)
# OPTIMIZE table ZORDER BY (column1, column2)

