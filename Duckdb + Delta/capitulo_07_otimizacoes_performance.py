# -*- coding: utf-8 -*-
"""
capitulo-07-otimizacoes-performance
"""

# capitulo-07-otimizacoes-performance
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
import time

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Habilitar profiling para ver otimizações
# con.execute("SET enable_profiling='json'") # Alterado para evitar erro de versao
con.execute("PRAGMA enable_profiling='json'")
con.execute("PRAGMA profiling_mode='detailed'")

# Query com filter pushdown
start = time.time()
result = con.execute("""
    SELECT COUNT(*), SUM(amount)
    FROM delta_scan('./large_delta_table')
    WHERE date = '2024-01-15'
        AND region = 'US'
""").fetchone()
elapsed = time.time() - start

print(f"Results: {result}")
print(f"Time: {elapsed:.2f}s")

# Ver profile da query
profile = con.execute("-- -- -- -- -- -- -- -- PRAGMA last_profiling_output").fetchone()[0]
print(profile)

# Exemplo/Bloco 2
import duckdb
import time

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Teste 1: SELECT *
start = time.time()
df1 = con.execute("""
    SELECT *
    FROM delta_scan('./sales')
    LIMIT 100000
""").df()
time1 = time.time() - start

# Teste 2: Projeção específica
start = time.time()
df2 = con.execute("""
    SELECT customer_id, date, amount
    FROM delta_scan('./sales')
    LIMIT 100000
""").df()
time2 = time.time() - start

print(f"SELECT * : {time1:.2f}s")
print(f"Projection: {time2:.2f}s")
print(f"Speedup: {time1/time2:.2f}x")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Analisar metadata Parquet da tabela Delta
metadata = con.execute("""
    SELECT *
    FROM parquet_metadata('./my_delta_table/part-00000.parquet')
""").df()

print("Row Groups:")
print(metadata[['row_group_id', 'num_rows', 'total_compressed_size']])

# Exemplo/Bloco 4
from deltalake import write_deltalake
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Criar DataFrame grande
df = con.execute("""
    SELECT
        i as id,
        'value-' || i as data
    FROM range(0, 10000000) tbl(i)
""").df()

# Escrever com row group size otimizado
write_deltalake(
    "./optimized_table",
    df,
    # pandas chunksize controla row group size
    engine='pyarrow',
    # Ajustar via PyArrow
)

# Exemplo/Bloco 5
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ver compression codec usado
metadata = con.execute("""
    SELECT
        column_name,
        codec,
        total_compressed_size,
        total_uncompressed_size,
        (total_uncompressed_size::FLOAT / total_compressed_size) as compression_ratio
    FROM parquet_metadata('./my_delta_table/part-00000.parquet')
""").df()

print(metadata)

# Exemplo/Bloco 6
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake

# Para escrita rápida e queries frequentes: SNAPPY
write_deltalake(
    "./fast_table",
    df,
    mode="overwrite"
)  # Snappy é padrão

# Para armazenamento eficiente: ZSTD
# (Requer configuração via PyArrow)
table = pa.Table.from_pandas(df)
pq.write_table(
    table,
    "./compressed_table/data.parquet",
    compression='zstd',
    compression_level=3
)

# Exemplo/Bloco 7
import duckdb
import time

con = duckdb.connect('benchmark.db')

# Teste 1: Query em Delta
start = time.time()
result1 = con.execute("""
    SELECT region, COUNT(*), SUM(amount)
    FROM delta_scan('./sales')
    GROUP BY region
""").fetchdf()
delta_time = time.time() - start

# Carregar para DuckDB
con.execute("""
    CREATE OR REPLACE TABLE sales_local AS
    SELECT * FROM delta_scan('./sales')
""")

# Teste 2: Query em DuckDB native
start = time.time()
result2 = con.execute("""
    SELECT region, COUNT(*), SUM(amount)
    FROM sales_local
    GROUP BY region
""").fetchdf()
native_time = time.time() - start

print(f"Delta scan: {delta_time:.2f}s")
print(f"DuckDB native: {native_time:.2f}s")
print(f"Speedup: {delta_time/native_time:.2f}x")

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
con.execute("SET enable_profiling=true")
con.execute("SET profiling_mode='detailed'")

# Executar query
result = con.execute("""
    SELECT region, COUNT(*)
    FROM delta_scan('./sales')
    WHERE date = '2024-01-15'
    GROUP BY region
""").fetchdf()

# Ver profile detalhado
profile = con.execute("-- -- -- -- -- -- -- -- PRAGMA last_profiling_output").fetchone()[0]

# Parsear profile
print("=== QUERY PROFILE ===")
print(profile)

# Métricas importantes:
# - Rows scanned
# - Files read
# - Execution time por operador
# - Memory usage

# Exemplo/Bloco 9
import duckdb
import time
from dataclasses import dataclass
from typing import List

@dataclass
class QueryMetrics:
    query: str
    execution_time: float
    rows_processed: int
    memory_used: int

class PerformanceMonitor:
    """Monitor de performance para queries Delta"""

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con
        self.metrics: List[QueryMetrics] = []

    def execute_and_measure(self, query: str) -> QueryMetrics:
        """Executar query e coletar métricas"""

        # Habilitar profiling
        self.con.execute("SET enable_profiling=true")

        # Executar query
        start = time.time()
        result = self.con.execute(query).fetchdf()
        execution_time = time.time() - start

        # Coletar métricas
        rows = len(result)

        metrics = QueryMetrics(
            query=query[:100],  # Primeiros 100 chars
            execution_time=execution_time,
            rows_processed=rows,
            memory_used=0  # Implementar se necessário
        )

        self.metrics.append(metrics)
        return metrics

    def print_summary(self):
        """Imprimir resumo de performance"""
        print("\n=== PERFORMANCE SUMMARY ===")
        total_time = sum(m.execution_time for m in self.metrics)
        print(f"Total queries: {len(self.metrics)}")
        print(f"Total time: {total_time:.2f}s")
        print(f"Average time: {total_time/len(self.metrics):.2f}s")

        print("\n=== SLOWEST QUERIES ===")
        sorted_metrics = sorted(
            self.metrics,
            key=lambda m: m.execution_time,
            reverse=True
        )[:5]

        for i, m in enumerate(sorted_metrics, 1):
            print(f"{i}. {m.execution_time:.2f}s - {m.query}...")


# Uso
if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    monitor = PerformanceMonitor(con)

    # Executar queries com monitoramento
    queries = [
        "SELECT COUNT(*) FROM delta_scan('./sales')",
        "SELECT region, COUNT(*) FROM delta_scan('./sales') GROUP BY region",
        "SELECT * FROM delta_scan('./sales') WHERE date = '2024-01-15'"
    ]

    for query in queries:
        metrics = monitor.execute_and_measure(query)
        print(f"[OK] Query completed in {metrics.execution_time:.2f}s")

    monitor.print_summary()

