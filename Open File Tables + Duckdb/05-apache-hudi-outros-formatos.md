# Capítulo 05: Apache Hudi e Outros Formatos Emergentes

## Apache Hudi: Hadoop Upserts Deletes and Incrementals

**Apache Hudi** (originalmente da Uber) é um formato de tabela open-source otimizado para **streaming** e **atualizações incrementais** em data lakes. O nome é um acrônimo de seu propósito: **Hadoop Upserts Deletes and Incrementals**.

### Características Principais

| Característica | Descrição |
|----------------|-----------|
| **Upserts Rápidos** | Operações UPDATE/DELETE eficientes |
| **Incremental Processing** | Processa apenas dados novos/modificados |
| **Copy-on-Write (CoW)** | Otimizado para leitura |
| **Merge-on-Read (MoR)** | Otimizado para escrita |
| **Timeline** | Histórico completo de operações |
| **Streaming First** | Projetado para ingestão contínua |

### Quando usar Hudi?

✅ **Use Hudi quando**:
- Precisa de upserts frequentes (CDC - Change Data Capture)
- Trabalha com streaming (Kafka, Flink, Spark Streaming)
- Requer processamento incremental eficiente
- Dados chegam continuamente e precisam ser atualizados

❌ **Não use Hudi para**:
- Dados append-only (use Parquet simples)
- Analytics pesado sem updates (considere Iceberg)
- Simplicidade é prioridade (considere Delta)

## Arquitetura do Hudi

### Estrutura de Diretórios

```
tabela_hudi/
├── .hoodie/
│   ├── hoodie.properties
│   ├── 20240101120000.commit
│   ├── 20240101130000.commit
│   └── archived/
├── 2024/
│   ├── 01/
│   │   ├── 15/
│   │   │   ├── <file_id>_<commit_time>.parquet
│   │   │   └── .<file_id>_<commit_time>.log (MoR only)
```

**Componentes**:
- **`.hoodie/`**: Metadados da tabela (timeline, commits, rollbacks)
- **Data Files**: Arquivos Parquet (base)
- **Log Files**: Deltas incrementais (apenas em MoR)

### Copy-on-Write (CoW) vs Merge-on-Read (MoR)

```
┌────────────────────────────────────────────────────────────┐
│                     Copy-on-Write (CoW)                     │
├────────────────────────────────────────────────────────────┤
│ Escrita: Reescreve arquivo completo em cada update         │
│ Leitura: Lê apenas arquivos Parquet (rápido)               │
│ Uso: Quando leituras >> escritas                           │
└────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────┐
│                     Merge-on-Read (MoR)                     │
├────────────────────────────────────────────────────────────┤
│ Escrita: Grava deltas em log files (rápido)                │
│ Leitura: Merge de base + logs (mais lento)                 │
│ Uso: Quando escritas >> leituras ou baixa latência         │
└────────────────────────────────────────────────────────────┘
```

## Lendo Hudi com DuckDB

**Importante**: DuckDB não tem extensão Hudi nativa (ainda), mas pode ler os arquivos Parquet subjacentes.

### Leitura Direta de Parquet

```sql
-- Ler arquivos base Parquet do Hudi
SELECT * FROM read_parquet('data/tabela_hudi/**/*.parquet');

-- Com filtro de partições Hive-style
SELECT * 
FROM read_parquet('data/tabela_hudi/**/*.parquet')
WHERE year = 2024 AND month = 6;

-- Evitar log files (.log) - apenas CoW ou snapshots MoR
SELECT * 
FROM read_parquet('data/tabela_hudi/**/*.parquet')
WHERE _hoodie_commit_time > '20240101000000';
```

### Python com Hudi

```python
import duckdb

conn = duckdb.connect()

# Ler tabela Hudi (arquivos Parquet)
df = conn.execute("""
    SELECT 
        *,
        _hoodie_commit_time,
        _hoodie_commit_seqno,
        _hoodie_record_key
    FROM read_parquet('data/vendas_hudi/**/*.parquet')
    WHERE _hoodie_commit_time >= '20240601000000'
""").fetchdf()

print(f"Registros: {len(df)}")
print(df.head())
```

### Incremental Query Simulation

```python
import duckdb

class HudiIncrementalReader:
    def __init__(self, hudi_path):
        self.conn = duckdb.connect()
        self.hudi_path = hudi_path
        self.last_commit = None
    
    def read_incremental(self):
        """Lê apenas commits novos desde a última leitura"""
        query = f"""
            SELECT * FROM read_parquet('{self.hudi_path}/**/*.parquet')
        """
        
        if self.last_commit:
            query += f" WHERE _hoodie_commit_time > '{self.last_commit}'"
        
        df = self.conn.execute(query).fetchdf()
        
        if len(df) > 0:
            self.last_commit = df['_hoodie_commit_time'].max()
        
        return df

# Uso
reader = HudiIncrementalReader('./data/vendas_hudi')

# Primeira leitura (full scan)
batch1 = reader.read_incremental()
print(f"Batch 1: {len(batch1)} registros")

# Próxima leitura (apenas novos commits)
batch2 = reader.read_incremental()
print(f"Batch 2: {len(batch2)} registros novos")
```

## Criando Tabelas Hudi (com Spark)

Para criar/escrever em Hudi, use Apache Spark:

```python
# PySpark com Hudi
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HudiWrite") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .getOrCreate()

# Configurações Hudi
hudi_options = {
    'hoodie.table.name': 'vendas',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'data',
    'hoodie.datasource.write.table.name': 'vendas',
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'updated_at'
}

# Escrever dados
df = spark.read.parquet("data/vendas.parquet")

df.write \
    .format("hudi") \
    .options(**hudi_options) \
    .mode("append") \
    .save("s3://bucket/vendas_hudi")

# Depois, ler com DuckDB
import duckdb
conn = duckdb.connect()
result = conn.execute("""
    SELECT * FROM read_parquet('s3://bucket/vendas_hudi/**/*.parquet')
""").fetchdf()
```

### Operações Hudi Típicas

```python
# UPSERT (atualizar ou inserir)
df.write \
    .format("hudi") \
    .options(**{'hoodie.datasource.write.operation': 'upsert'}) \
    .mode("append") \
    .save("path/to/hudi")

# INSERT (apenas novos registros)
df.write \
    .format("hudi") \
    .options(**{'hoodie.datasource.write.operation': 'insert'}) \
    .mode("append") \
    .save("path/to/hudi")

# BULK_INSERT (carga inicial rápida)
df.write \
    .format("hudi") \
    .options(**{'hoodie.datasource.write.operation': 'bulk_insert'}) \
    .mode("overwrite") \
    .save("path/to/hudi")
```

## Apache Paimon: Streaming Lakehouse

**Apache Paimon** (anteriormente Flink Table Store) é um formato de tabela criado especificamente para **streaming em tempo real** e integração nativa com Apache Flink.

### Características Principais

- ✅ **Streaming First**: Projetado para ingestão contínua
- ✅ **Lake Format**: Armazena dados em object storage (S3/HDFS)
- ✅ **Changelog**: Suporte nativo a CDC (Change Data Capture)
- ✅ **Primary Keys**: Otimizado para upserts por chave
- ✅ **Bucket Architecture**: Distribui dados em buckets para performance

### Arquitetura do Paimon

```
tabela_paimon/
├── manifest/
│   ├── manifest-list-xxx
│   └── manifest-xxx
├── snapshot/
│   ├── snapshot-1
│   └── snapshot-2
└── bucket-0/
    ├── data-xxx.orc
    └── changelog-xxx.orc
```

### Lendo Paimon com DuckDB

**Status**: Suporte limitado, leia arquivos ORC/Parquet subjacentes:

```sql
-- Se Paimon grava em Parquet
SELECT * FROM read_parquet('data/tabela_paimon/bucket-*/*.parquet');

-- Se Paimon grava em ORC (requer instalação)
INSTALL orc;
LOAD orc;
SELECT * FROM read_orc('data/tabela_paimon/bucket-*/*.orc');
```

### Criando Tabelas Paimon (com Flink)

```sql
-- Flink SQL
CREATE TABLE vendas (
    id BIGINT PRIMARY KEY NOT ENFORCED,
    produto STRING,
    valor DECIMAL(10, 2),
    data_venda TIMESTAMP
) WITH (
    'connector' = 'paimon',
    'path' = 's3://bucket/vendas_paimon',
    'bucket' = '8'
);

-- Inserir streaming
INSERT INTO vendas
SELECT id, produto, valor, data_venda
FROM kafka_source;
```

### Python com Flink e Paimon

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

# Setup Flink
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

# Criar catálogo Paimon
table_env.execute_sql("""
    CREATE CATALOG paimon_catalog WITH (
        'type' = 'paimon',
        'warehouse' = 's3://warehouse'
    )
""")

table_env.use_catalog('paimon_catalog')

# Criar tabela
table_env.execute_sql("""
    CREATE TABLE vendas_streaming (
        venda_id BIGINT PRIMARY KEY NOT ENFORCED,
        produto STRING,
        valor DECIMAL(10, 2),
        ts TIMESTAMP(3)
    ) WITH (
        'bucket' = '4',
        'changelog-producer' = 'input'
    )
""")

# Inserir dados
table_env.execute_sql("""
    INSERT INTO vendas_streaming
    SELECT * FROM source_kafka
""")
```

**Depois, ler com DuckDB**:

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL orc; LOAD orc;")

# Ler snapshots Paimon
df = conn.execute("""
    SELECT * FROM read_orc('s3://warehouse/vendas_streaming/bucket-*/*.orc')
""").fetchdf()

print(df)
```

## Comparação: Hudi vs Paimon

| Aspecto | Apache Hudi | Apache Paimon |
|---------|-------------|---------------|
| **Foco** | Upserts, CDC | Streaming real-time |
| **Engine Principal** | Spark | Flink |
| **Formato Base** | Parquet | ORC ou Parquet |
| **Latência Escrita** | Baixa (MoR) | Muito Baixa |
| **Streaming Native** | ⚠️ | ✅✅ |
| **Primary Keys** | ✅ | ✅ |
| **Changelog** | ✅ | ✅✅ |
| **Maturidade** | Alta | Média-Alta |

## Outros Formatos Emergentes

### DuckLake (2025)

**DuckLake** é uma proposta do ecossistema DuckDB/MotherDuck para simplificar table formats.

**Conceito**:
- Metadados em SQL (banco DuckDB) em vez de JSON/Avro
- Sem dependência de Spark/Flink para operações básicas
- Ideal para cargas single-node ou pequenas

```sql
-- Conceito DuckLake (futuro)
CREATE TABLE vendas_ducklake (
    id INTEGER,
    produto VARCHAR,
    valor DECIMAL(10,2)
) USING DUCKLAKE
LOCATION './data/vendas_ducklake';

-- Metadados armazenados localmente
CREATE TABLE _ducklake_metadata (
    table_name VARCHAR,
    version INTEGER,
    snapshot_path VARCHAR,
    created_at TIMESTAMP
);
```

**Status**: Experimental, aguardando implementação oficial.

### Vortex

**Vortex** é um formato experimental focado em **compressão extrema** e **queries analíticas**.

**Características**:
- Compressão adaptativa
- Encoding moderno (além de RLE/Dictionary)
- Foco em columnar next-gen

**Status**: Prova de conceito, não pronto para produção.

## Integrando Múltiplos Formatos no DuckDB

### Consulta Federada

```sql
-- Instalar extensões
INSTALL delta;
INSTALL iceberg;
LOAD delta;
LOAD iceberg;

-- Query cross-format
WITH 
    delta_data AS (
        SELECT 'Delta' as source, * 
        FROM delta_scan('s3://warehouse/vendas_delta')
    ),
    iceberg_data AS (
        SELECT 'Iceberg' as source, * 
        FROM iceberg_scan('s3://warehouse/vendas_iceberg')
    ),
    hudi_data AS (
        SELECT 'Hudi' as source, * 
        FROM read_parquet('s3://warehouse/vendas_hudi/**/*.parquet')
    )
SELECT 
    source,
    COUNT(*) as registros,
    SUM(valor) as receita_total
FROM (
    SELECT * FROM delta_data
    UNION ALL
    SELECT * FROM iceberg_data
    UNION ALL
    SELECT * FROM hudi_data
)
GROUP BY source;
```

### Pipeline Multi-Format

```python
import duckdb

class MultiFormatLakehouse:
    def __init__(self):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; LOAD delta;")
        self.conn.execute("INSTALL iceberg; LOAD iceberg;")
    
    def read_delta(self, path):
        return self.conn.execute(f"""
            SELECT 'Delta' as format, * FROM delta_scan('{path}')
        """).fetchdf()
    
    def read_iceberg(self, path):
        return self.conn.execute(f"""
            SELECT 'Iceberg' as format, * FROM iceberg_scan('{path}')
        """).fetchdf()
    
    def read_hudi(self, path):
        return self.conn.execute(f"""
            SELECT 'Hudi' as format, * FROM read_parquet('{path}/**/*.parquet')
        """).fetchdf()
    
    def unified_view(self, delta_path, iceberg_path, hudi_path):
        """Cria view unificada de múltiplos formatos"""
        return self.conn.execute(f"""
            SELECT 'Delta' as source, * FROM delta_scan('{delta_path}')
            UNION ALL
            SELECT 'Iceberg' as source, * FROM iceberg_scan('{iceberg_path}')
            UNION ALL
            SELECT 'Hudi' as source, * 
            FROM read_parquet('{hudi_path}/**/*.parquet')
        """).fetchdf()

# Uso
lakehouse = MultiFormatLakehouse()
unified = lakehouse.unified_view(
    './data/sales_delta',
    './data/sales_iceberg',
    './data/sales_hudi'
)
print(unified)
```

## Escolhendo o Formato Certo

### Árvore de Decisão

```
Você precisa de streaming real-time com Flink?
├─ SIM → Apache Paimon
└─ NÃO
   └─ Precisa de upserts/CDC frequentes?
      ├─ SIM → Apache Hudi
      └─ NÃO
         └─ Múltiplas engines (Trino, Presto, Flink)?
            ├─ SIM → Apache Iceberg
            └─ NÃO
               └─ Usa Databricks/Spark?
                  ├─ SIM → Delta Lake
                  └─ NÃO → Parquet simples ou Delta/Iceberg
```

### Matriz de Decisão

| Requisito | Delta | Iceberg | Hudi | Paimon |
|-----------|-------|---------|------|--------|
| Append-only | ✅ | ✅ | ⚠️ | ⚠️ |
| Upserts | ✅ | ⚠️ | ✅✅ | ✅ |
| Streaming | ⚠️ | ✅ | ✅ | ✅✅ |
| Multi-Engine | ⚠️ | ✅✅ | ✅ | ⚠️ |
| Partition Evolution | ❌ | ✅✅ | ⚠️ | ✅ |
| Simplicidade | ✅✅ | ✅ | ⚠️ | ⚠️ |

## Próximos Passos

No próximo capítulo, vamos explorar o **Lance Format**, um formato especializado para Machine Learning, busca vetorial e workloads de IA.

## Recursos

- [Apache Hudi Docs](https://hudi.apache.org/)
- [Apache Paimon Docs](https://paimon.apache.org/)
- [Hudi with Spark](https://hudi.apache.org/docs/quick-start-guide)
- [Paimon with Flink](https://paimon.apache.org/docs/master/engines/flink/)

---

**Exercício Prático**:

Simule leitura incremental de Hudi:

```python
import duckdb
import pandas as pd
from datetime import datetime

conn = duckdb.connect()

# Simular dados Hudi com commit times
df = pd.DataFrame({
    'id': range(1, 101),
    'produto': ['Produto_' + str(i) for i in range(1, 101)],
    'valor': [100 + i for i in range(1, 101)],
    '_hoodie_commit_time': ['20240601120000'] * 50 + ['20240602120000'] * 50
})

# Salvar como Parquet (simula Hudi CoW)
df.to_parquet('./test_hudi/data.parquet')

# Ler apenas commits mais recentes
recent = conn.execute("""
    SELECT * FROM read_parquet('./test_hudi/data.parquet')
    WHERE _hoodie_commit_time >= '20240602000000'
""").fetchdf()

print(f"Commits recentes: {len(recent)} registros")
print(recent.head())
```
