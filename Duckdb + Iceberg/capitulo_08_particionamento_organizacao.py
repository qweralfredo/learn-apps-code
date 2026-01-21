# -*- coding: utf-8 -*-
"""
Iceberg-08-particionamento-organizacao
"""

# Iceberg-08-particionamento-organizacao
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
con.execute("LOAD iceberg")

# Query com filtro de data
result = con.execute("""
    SELECT count(*)
    FROM iceberg_scan('s3://bucket/sales')
    WHERE order_date >= '2024-01-01'
      AND order_date < '2024-02-01'
""").fetchone()

# Iceberg lê apenas partições de janeiro 2024!
# Partições de outros meses são ignoradas

# Exemplo/Bloco 2
import duckdb
import time

con = duckdb.connect()
con.execute("LOAD iceberg")

# Sem filtro de partição (lê tudo)
start = time.time()
count1 = con.execute("""
    SELECT count(*)
    FROM iceberg_scan('s3://bucket/large_table')
""").fetchone()[0]
time1 = time.time() - start

# Com filtro de partição (lê só o necessário)
start = time.time()
count2 = con.execute("""
    SELECT count(*)
    FROM iceberg_scan('s3://bucket/large_table')
    WHERE event_date = '2024-01-15'
""").fetchone()[0]
time2 = time.time() - start

print(f"Sem filtro: {time1:.2f}s ({count1:,} linhas)")
print(f"Com filtro: {time2:.2f}s ({count2:,} linhas)")
print(f"Speedup: {time1/time2:.1f}x")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("LOAD iceberg")

# Analisar distribuição de arquivos
metadata = con.execute("""
    SELECT
        file_path,
        record_count,
        file_size_in_bytes / 1024 / 1024 as size_mb
    FROM iceberg_metadata('s3://bucket/partitioned_table')
    WHERE status = 'EXISTING'
    ORDER BY file_path
""").df()

print("Distribuição de arquivos:")
print(metadata.groupby(
    metadata['file_path'].str.extract(r'/(\\w+)=')[0]
)['record_count'].agg(['count', 'sum', 'mean']))

# Exemplo/Bloco 4
import duckdb

def find_small_partitions(table_path, threshold_mb=10):
    """Encontra partições pequenas que podem ser compactadas"""
    con = duckdb.connect()
    con.execute("LOAD iceberg")

    small_files = con.execute(f"""
        SELECT
            file_path,
            record_count,
            file_size_in_bytes / 1024 / 1024 as size_mb
        FROM iceberg_metadata('{table_path}')
        WHERE status = 'EXISTING'
          AND file_size_in_bytes / 1024 / 1024 < {threshold_mb}
        ORDER BY size_mb
    """).df()

    return small_files

# Usar
small = find_small_partitions('s3://bucket/sales', threshold_mb=50)
print(f"Arquivos pequenos encontrados: {len(small)}")

# Exemplo/Bloco 5
# Iceberg lê corretamente mesmo com múltiplos partition specs
result = con.execute("""
    SELECT
        date_trunc('month', order_date) as month,
        count(*) as orders
    FROM iceberg_scan('s3://bucket/evolved_table')
    GROUP BY month
    ORDER BY month
""").df()

# Funciona mesmo que:
# - Dados de 2023 estejam particionados por dia
# - Dados de 2024 estejam particionados por hora

# Exemplo/Bloco 6
import duckdb

def analyze_partition_candidates(table_path):
    """Analisa quais colunas são boas candidatas para particionamento"""
    con = duckdb.connect()
    con.execute("LOAD iceberg")

    # Pegar amostra
    sample = con.execute(f"""
        SELECT *
        FROM iceberg_scan('{table_path}')
        USING SAMPLE 10%
    """).df()

    # Analisar cardinalidade de cada coluna
    for col in sample.columns:
        unique_count = sample[col].nunique()
        total_count = len(sample)
        cardinality_ratio = unique_count / total_count

        print(f"{col}:")
        print(f"  Valores únicos: {unique_count:,}")
        print(f"  Ratio: {cardinality_ratio:.2%}")

        if 0.01 < cardinality_ratio < 0.5:
            print(f"  ✅ Boa candidata para particionamento")
        else:
            print(f"  ❌ Não recomendada")
        print()

analyze_partition_candidates('s3://bucket/sales')

# Exemplo/Bloco 7
import duckdb

def needs_compaction(table_path, ideal_size_mb=128):
    """Verifica se tabela precisa de compactação"""
    con = duckdb.connect()
    con.execute("LOAD iceberg")

    stats = con.execute(f"""
        SELECT
            count(*) as file_count,
            avg(file_size_in_bytes / 1024 / 1024) as avg_size_mb,
            min(file_size_in_bytes / 1024 / 1024) as min_size_mb,
            max(file_size_in_bytes / 1024 / 1024) as max_size_mb
        FROM iceberg_metadata('{table_path}')
        WHERE status = 'EXISTING'
    """).fetchone()

    file_count, avg_size, min_size, max_size = stats

    print(f"Estatísticas de arquivos:")
    print(f"  Total: {file_count:,}")
    print(f"  Tamanho médio: {avg_size:.2f} MB")
    print(f"  Range: {min_size:.2f} - {max_size:.2f} MB")

    if avg_size < ideal_size_mb * 0.5:
        print(f"⚠️  Recomendado: Compactar arquivos")
        return True
    else:
        print(f"✅ Tamanho de arquivos OK")
        return False

needs_compaction('s3://bucket/sales')

# Exemplo/Bloco 8
# Regra geral para particionamento por data:

# Dados pequenos (< 1 GB/dia): Particionar por mês
# Dados médios (1-100 GB/dia): Particionar por dia
# Dados grandes (> 100 GB/dia): Particionar por hora

# Evitar:
# - Partições muito pequenas (< 100 MB)
# - Partições muito grandes (> 1 GB)
# - Muitas partições (> 10.000)

# Exemplo/Bloco 9
import duckdb

class IcebergPartitionMonitor:
    def __init__(self, table_path):
        self.table_path = table_path
        self.con = duckdb.connect()
        self.con.execute("LOAD iceberg")

    def partition_stats(self):
        """Estatísticas por partição"""
        return self.con.execute(f"""
            SELECT
                regexp_extract(file_path, '/(\\w+)=(\\w+)/', 1) as partition_key,
                regexp_extract(file_path, '/(\\w+)=(\\w+)/', 2) as partition_value,
                count(*) as file_count,
                sum(record_count) as total_records,
                sum(file_size_in_bytes) / 1024 / 1024 as total_mb
            FROM iceberg_metadata('{self.table_path}')
            WHERE status = 'EXISTING'
            GROUP BY partition_key, partition_value
            ORDER BY total_mb DESC
        """).df()

# Usar
monitor = IcebergPartitionMonitor('s3://bucket/sales')
stats = monitor.partition_stats()
print(stats.head(20))

