# Capítulo 09: Performance e Otimizações

## Introdução

Este capítulo foca em **otimizações práticas** para maximizar a performance do DuckDB ao trabalhar com open table file formats. Vamos cobrir benchmarks, técnicas de tuning e best practices testadas em produção.

## Arquitetura de Performance do DuckDB

### Execução Vetorizada

DuckDB processa dados em **vetores** (blocos de ~2048 linhas), aproveitando:
- SIMD (Single Instruction, Multiple Data)
- Cache CPU eficiente
- Menos chamadas de função

```python
import duckdb
import time

conn = duckdb.connect()

# Query vetorizada (rápida)
start = time.time()
result = conn.execute("""
    SELECT SUM(value) FROM read_parquet('data.parquet')
""").fetchone()
vectorized_time = time.time() - start

print(f"Processamento vetorizado: {vectorized_time:.3f}s")
```

### Paralelização Automática

```python
import duckdb
import os

# DuckDB usa todos os cores por padrão
print(f"Cores disponíveis: {os.cpu_count()}")

conn = duckdb.connect()

# Configurar threads manualmente (se necessário)
conn.execute("SET threads TO 8")

# Query paralela automática
result = conn.execute("""
    SELECT 
        category,
        COUNT(*) as count,
        SUM(value) as total
    FROM read_parquet('large_dataset.parquet')
    GROUP BY category
""").fetchdf()
```

### Pushdown de Predicados

DuckDB "empurra" filtros para a camada de I/O:

```sql
-- Filtro aplicado na leitura (não após carregar tudo)
SELECT * 
FROM delta_scan('s3://bucket/table')
WHERE date >= '2024-01-01'  -- Filtrado durante leitura
  AND value > 1000;         -- Filtrado durante leitura
```

## Benchmarks: Comparando Formatos

### Setup de Benchmark

```python
import duckdb
import pandas as pd
import numpy as np
import time
from typing import Dict

class FormatBenchmark:
    def __init__(self, rows=10_000_000):
        self.rows = rows
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; INSTALL iceberg;")
        self.conn.execute("LOAD delta; LOAD iceberg;")
        self.setup_data()
    
    def setup_data(self):
        """Cria dataset de teste"""
        print(f"Criando dataset com {self.rows:,} registros...")
        
        df = pd.DataFrame({
            'id': range(self.rows),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], self.rows),
            'region': np.random.choice(['North', 'South', 'East', 'West'], self.rows),
            'value': np.random.rand(self.rows) * 1000,
            'date': pd.date_range('2024-01-01', periods=self.rows, freq='s')
        })
        
        # Salvar em diferentes formatos
        print("Salvando Parquet...")
        df.to_parquet('./bench_parquet.parquet', compression='snappy')
        
        print("Salvando Delta...")
        self.conn.execute("""
            COPY (SELECT * FROM df) 
            TO './bench_delta' 
            (FORMAT DELTA)
        """)
        
        print("Setup completo!")
        self.df = df
    
    def benchmark_query(self, query: str, format_name: str) -> Dict:
        """Executa benchmark de uma query"""
        # Warm-up
        self.conn.execute(query).fetchall()
        
        # Benchmark real (3 execuções)
        times = []
        for _ in range(3):
            start = time.time()
            result = self.conn.execute(query).fetchall()
            times.append(time.time() - start)
        
        return {
            'format': format_name,
            'avg_time': np.mean(times),
            'min_time': np.min(times),
            'max_time': np.max(times),
            'rows': len(result)
        }
    
    def run_benchmarks(self):
        """Executa suite completa de benchmarks"""
        results = []
        
        # Benchmark 1: Full scan
        print("\n=== Benchmark 1: Full Scan ===")
        
        queries = {
            'Parquet': """
                SELECT COUNT(*) FROM read_parquet('./bench_parquet.parquet')
            """,
            'Delta': """
                SELECT COUNT(*) FROM delta_scan('./bench_delta')
            """
        }
        
        for fmt, query in queries.items():
            result = self.benchmark_query(query, fmt)
            results.append({**result, 'test': 'full_scan'})
            print(f"{fmt}: {result['avg_time']:.3f}s")
        
        # Benchmark 2: Filtered query
        print("\n=== Benchmark 2: Filtered Query ===")
        
        queries = {
            'Parquet': """
                SELECT category, SUM(value) 
                FROM read_parquet('./bench_parquet.parquet')
                WHERE date >= '2024-06-01'
                GROUP BY category
            """,
            'Delta': """
                SELECT category, SUM(value)
                FROM delta_scan('./bench_delta')
                WHERE date >= '2024-06-01'
                GROUP BY category
            """
        }
        
        for fmt, query in queries.items():
            result = self.benchmark_query(query, fmt)
            results.append({**result, 'test': 'filtered'})
            print(f"{fmt}: {result['avg_time']:.3f}s")
        
        # Benchmark 3: Aggregation heavy
        print("\n=== Benchmark 3: Heavy Aggregation ===")
        
        queries = {
            'Parquet': """
                SELECT 
                    category,
                    region,
                    DATE_TRUNC('day', date) as day,
                    COUNT(*) as count,
                    AVG(value) as avg_val,
                    MIN(value) as min_val,
                    MAX(value) as max_val,
                    STDDEV(value) as stddev_val
                FROM read_parquet('./bench_parquet.parquet')
                GROUP BY category, region, day
            """,
            'Delta': """
                SELECT 
                    category,
                    region,
                    DATE_TRUNC('day', date) as day,
                    COUNT(*) as count,
                    AVG(value) as avg_val,
                    MIN(value) as min_val,
                    MAX(value) as max_val,
                    STDDEV(value) as stddev_val
                FROM delta_scan('./bench_delta')
                GROUP BY category, region, day
            """
        }
        
        for fmt, query in queries.items():
            result = self.benchmark_query(query, fmt)
            results.append({**result, 'test': 'aggregation'})
            print(f"{fmt}: {result['avg_time']:.3f}s")
        
        return pd.DataFrame(results)

# Executar benchmarks
bench = FormatBenchmark(rows=1_000_000)
results_df = bench.run_benchmarks()

print("\n=== Resumo dos Benchmarks ===")
print(results_df.groupby(['test', 'format'])['avg_time'].mean())
```

### Resultados Esperados

```
=== Benchmark 1: Full Scan ===
Parquet: 0.234s
Delta: 0.287s

=== Benchmark 2: Filtered Query ===
Parquet: 0.156s
Delta: 0.178s

=== Benchmark 3: Heavy Aggregation ===
Parquet: 1.234s
Delta: 1.345s

Observações:
- Parquet ~10-20% mais rápido (menos overhead de metadados)
- Delta tem overhead do transaction log
- Ambos se beneficiam de pushdown e vetorização
```

## Otimizações Práticas

### 1. Particionamento Estratégico

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")

# ❌ MAU: Sem particionamento
conn.execute("""
    COPY (SELECT * FROM source_table)
    TO './data_no_partition'
    (FORMAT DELTA)
""")

# ✅ BOM: Particionado por coluna relevante
conn.execute("""
    COPY (SELECT * FROM source_table)
    TO './data_partitioned'
    (FORMAT DELTA, PARTITION_BY (year, month))
""")

# Query com partition pruning
result = conn.execute("""
    SELECT COUNT(*) 
    FROM delta_scan('./data_partitioned')
    WHERE year = 2024 AND month = 6
""").fetchone()
# Lê apenas partição específica!
```

### 2. Compressão Otimizada

```python
# Comparar algoritmos de compressão
compressions = ['snappy', 'gzip', 'zstd', 'lz4']

for compression in compressions:
    conn.execute(f"""
        COPY (SELECT * FROM large_table)
        TO './data_{compression}.parquet'
        (FORMAT PARQUET, COMPRESSION {compression})
    """)

# Benchmark leitura
import os

for compression in compressions:
    file_size = os.path.getsize(f'./data_{compression}.parquet') / 1024 / 1024
    
    start = time.time()
    conn.execute(f"""
        SELECT COUNT(*) FROM read_parquet('./data_{compression}.parquet')
    """).fetchone()
    read_time = time.time() - start
    
    print(f"{compression}: {file_size:.2f}MB, {read_time:.3f}s")

# Resultados típicos:
# snappy: 100MB, 0.150s  (rápido, compressão média)
# gzip:   75MB,  0.220s  (lento, alta compressão)
# zstd:   70MB,  0.165s  (melhor custo-benefício)
# lz4:    105MB, 0.140s  (muito rápido, baixa compressão)
```

**Recomendação**: Use **ZSTD** para melhor equilíbrio.

### 3. Row Group Size

```python
# ❌ Row groups muito pequenos (overhead)
conn.execute("""
    COPY (SELECT * FROM data)
    TO './small_rowgroups.parquet'
    (FORMAT PARQUET, ROW_GROUP_SIZE 10000)
""")

# ✅ Row groups otimizados
conn.execute("""
    COPY (SELECT * FROM data)
    TO './optimal_rowgroups.parquet'
    (FORMAT PARQUET, ROW_GROUP_SIZE 100000)
""")

# Regra: 64MB-256MB por row group
# Para 10GB dataset: 100-150 row groups
```

### 4. Column Pruning

```python
# ❌ Ler todas as colunas
bad_query = """
    SELECT * 
    FROM read_parquet('./wide_table.parquet')
    WHERE id = 12345
"""

# ✅ Ler apenas colunas necessárias
good_query = """
    SELECT id, name, value
    FROM read_parquet('./wide_table.parquet')
    WHERE id = 12345
"""

# Economia: ~10x menos I/O em tabelas com 50+ colunas
```

### 5. Predicate Pushdown

```sql
-- ✅ Filtros aplicados na leitura
SELECT * 
FROM delta_scan('s3://bucket/sales')
WHERE 
    date >= '2024-01-01'  -- Pushdown: Apenas partições relevantes
    AND region = 'US'     -- Pushdown: Min/max statistics
    AND amount > 1000;    -- Pushdown: Bloom filters (se disponível)

-- Resultado: Lê ~1% dos dados vs 100%
```

### 6. Caching Inteligente

```python
import duckdb

# Configurar memória disponível
conn = duckdb.connect()
conn.execute("SET memory_limit='8GB'")

# Cache automático de resultados
conn.execute("SET enable_object_cache=true")

# Query 1 (cold - lê do disco)
start = time.time()
conn.execute("SELECT COUNT(*) FROM large_table").fetchone()
cold_time = time.time() - start

# Query 2 (warm - usa cache)
start = time.time()
conn.execute("SELECT COUNT(*) FROM large_table").fetchone()
warm_time = time.time() - start

print(f"Cold: {cold_time:.3f}s, Warm: {warm_time:.3f}s")
print(f"Speedup: {cold_time/warm_time:.1f}x")
```

### 7. Agregações Pre-computadas

```python
# ❌ Agregação on-the-fly sempre
slow_query = """
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(value) as revenue
    FROM large_sales
    GROUP BY month
"""

# ✅ Materializar agregações frequentes
conn.execute("""
    CREATE TABLE monthly_revenue AS
    SELECT 
        DATE_TRUNC('month', date) as month,
        SUM(value) as revenue
    FROM large_sales
    GROUP BY month
""")

# Queries subsequentes são instantâneas
fast_query = "SELECT * FROM monthly_revenue WHERE month >= '2024-01-01'"
```

## Configurações de Performance

### DuckDB Settings

```python
import duckdb

conn = duckdb.connect()

# 1. Threads
conn.execute("SET threads TO 16")  # Usar todos os cores

# 2. Memória
conn.execute("SET memory_limit='16GB'")
conn.execute("SET temp_directory='/fast-ssd/duckdb_temp'")

# 3. Paralelismo
conn.execute("SET max_memory='16GB'")
conn.execute("SET force_parallelism=true")

# 4. Otimizador
conn.execute("SET optimizer_join_order=true")

# 5. Extensões
conn.execute("SET autoload_known_extensions=true")
conn.execute("SET autoinstall_known_extensions=true")

# Verificar configurações
settings = conn.execute("SELECT * FROM duckdb_settings()").fetchdf()
print(settings[settings['name'].str.contains('thread|memory|parallel')])
```

### Cloud Optimizations (S3/GCS/Azure)

```python
import duckdb

conn = duckdb.connect()

# AWS S3
conn.execute("""
    CREATE SECRET aws_secret (
        TYPE S3,
        KEY_ID 'xxx',
        SECRET 'xxx',
        REGION 'us-east-1'
    )
""")

# Otimizações S3
conn.execute("SET s3_region='us-east-1'")
conn.execute("SET s3_url_style='path'")  # ou 'vhost'
conn.execute("SET http_timeout=120000")  # 2 minutos

# Paralelismo em leitura
conn.execute("SET s3_use_ssl=true")

# Query otimizada
result = conn.execute("""
    SELECT * FROM delta_scan('s3://bucket/table')
    WHERE date >= '2024-01-01'
""").fetchdf()
```

## Profiling e Debugging

### EXPLAIN para Entender Query Plans

```sql
-- Ver plano de execução
EXPLAIN 
SELECT 
    category,
    SUM(value) as total
FROM read_parquet('./data.parquet')
WHERE date >= '2024-01-01'
GROUP BY category;
```

**Output típico**:
```
┌───────────────────────────┐
│      PROJECTION           │
│   ─────────────           │
│   category                │
│   SUM(value)              │
└─────────────┬─────────────┘
              │
┌─────────────▼─────────────┐
│      HASH_GROUP_BY        │
│   ─────────────           │
│   Groups: category        │
└─────────────┬─────────────┘
              │
┌─────────────▼─────────────┐
│        FILTER             │
│   ─────────────           │
│   date >= 2024-01-01      │ <- Pushdown!
└─────────────┬─────────────┘
              │
┌─────────────▼─────────────┐
│    PARQUET_SCAN           │
│   ─────────────           │
│   Filters: date >= ...    │
│   Columns: category, value│
└───────────────────────────┘
```

### EXPLAIN ANALYZE para Profiling

```sql
-- Profiling com métricas reais
EXPLAIN ANALYZE 
SELECT * FROM large_table 
WHERE value > 1000;
```

**Output inclui**:
- Tempo por operador
- Rows processadas
- Memory usage

### Python Profiling

```python
import duckdb
import cProfile
import pstats

def complex_query():
    conn = duckdb.connect()
    result = conn.execute("""
        SELECT 
            category,
            region,
            DATE_TRUNC('month', date) as month,
            COUNT(*) as count,
            SUM(value) as revenue
        FROM read_parquet('./large_dataset.parquet')
        GROUP BY category, region, month
        ORDER BY revenue DESC
    """).fetchdf()
    return result

# Profile
profiler = cProfile.Profile()
profiler.enable()
result = complex_query()
profiler.disable()

# Análise
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)
```

## Monitoramento em Produção

### Metrics Collector

```python
import duckdb
import time
from dataclasses import dataclass
from typing import List

@dataclass
class QueryMetrics:
    query: str
    execution_time: float
    rows_returned: int
    memory_used: int

class DuckDBMonitor:
    def __init__(self):
        self.metrics: List[QueryMetrics] = []
        self.conn = duckdb.connect()
    
    def execute_monitored(self, query: str):
        """Executa query com monitoramento"""
        start = time.time()
        
        result = self.conn.execute(query).fetchall()
        
        execution_time = time.time() - start
        rows = len(result)
        
        # Aproximação de memória (via pragma)
        memory_info = self.conn.execute(
            "SELECT * FROM pragma_database_size()"
        ).fetchone()
        
        metric = QueryMetrics(
            query=query[:100],  # Truncar query
            execution_time=execution_time,
            rows_returned=rows,
            memory_used=memory_info[0] if memory_info else 0
        )
        
        self.metrics.append(metric)
        
        # Alertar se query lenta
        if execution_time > 5.0:
            print(f"⚠️  Slow query detected: {execution_time:.2f}s")
            print(f"Query: {query[:100]}...")
        
        return result
    
    def report(self):
        """Gera relatório de performance"""
        if not self.metrics:
            print("Nenhuma métrica coletada")
            return
        
        import pandas as pd
        df = pd.DataFrame([vars(m) for m in self.metrics])
        
        print("\n=== Performance Report ===")
        print(f"Total queries: {len(df)}")
        print(f"Avg execution time: {df['execution_time'].mean():.3f}s")
        print(f"Max execution time: {df['execution_time'].max():.3f}s")
        print(f"Total rows: {df['rows_returned'].sum():,}")
        
        # Queries mais lentas
        print("\n=== Top 5 Slowest Queries ===")
        slowest = df.nlargest(5, 'execution_time')
        for _, row in slowest.iterrows():
            print(f"{row['execution_time']:.3f}s - {row['query']}")

# Uso
monitor = DuckDBMonitor()

# Executar queries monitoradas
monitor.execute_monitored("SELECT COUNT(*) FROM large_table")
monitor.execute_monitored("SELECT * FROM large_table WHERE id > 1000000")

# Relatório
monitor.report()
```

## Checklist de Otimização

### Antes de Otimizar

- [ ] Identificar query lenta (>1s para interactive, >10s para batch)
- [ ] Usar EXPLAIN ANALYZE para entender bottleneck
- [ ] Verificar se dados estão particionados adequadamente
- [ ] Conferir se índices/statistics estão atualizados

### Otimizações Rápidas

- [ ] Usar column pruning (SELECT apenas colunas necessárias)
- [ ] Aplicar predicate pushdown (WHERE no scan)
- [ ] Particionar por coluna de filtro comum
- [ ] Usar compressão ZSTD
- [ ] Configurar threads adequadamente

### Otimizações Avançadas

- [ ] Pre-agregar dados frequentemente consultados
- [ ] Usar tabelas materializadas
- [ ] Otimizar row group size
- [ ] Implementar caching de resultados
- [ ] Considerar formato especializado (CarbonData para filtros)

## Próximos Passos

No capítulo final, vamos ver casos de uso reais e projetos práticos completos usando DuckDB com open table file formats.

## Recursos

- [DuckDB Performance Guide](https://duckdb.org/docs/guides/performance/overview)
- [Parquet Best Practices](https://parquet.apache.org/docs/)
- [Query Optimization Techniques](https://duckdb.org/docs/guides/performance/query_optimization)

---

**Exercício Prático**:

Otimize uma query lenta:

```python
import duckdb
import time

conn = duckdb.connect()

# Query lenta (sem otimização)
slow_query = """
    SELECT * FROM read_parquet('./large_data.parquet')
    WHERE category = 'Electronics'
"""

# Medir tempo
start = time.time()
result_slow = conn.execute(slow_query).fetchall()
slow_time = time.time() - start

# Query otimizada (column pruning + pushdown)
fast_query = """
    SELECT id, name, price 
    FROM read_parquet('./large_data.parquet')
    WHERE category = 'Electronics'
"""

start = time.time()
result_fast = conn.execute(fast_query).fetchall()
fast_time = time.time() - start

print(f"Slow: {slow_time:.3f}s")
print(f"Fast: {fast_time:.3f}s")
print(f"Speedup: {slow_time/fast_time:.2f}x")
```
