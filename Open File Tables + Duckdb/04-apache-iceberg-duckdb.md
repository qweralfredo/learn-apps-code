# Capítulo 04: Apache Iceberg com DuckDB

## Introdução ao Apache Iceberg

**Apache Iceberg** é um formato de tabela open-source criado pela Netflix e agora um projeto top-level da Apache Software Foundation. Foi projetado desde o início para ser **engine-agnostic** e resolver problemas de escala que outros formatos enfrentavam.

### Por que Iceberg?

A Netflix tinha problemas com tabelas Hive tradicionais:
- Lentidão ao listar arquivos (centenas de milhares)
- Impossibilidade de evolução de particionamento
- Problemas com leituras concorrentes
- Metadados ineficientes

**Solução**: Iceberg foi criado com arquitetura moderna de metadados.

### Características Principais

| Característica | Descrição |
|----------------|-----------|
| **Hidden Partitioning** | Usuário não precisa saber como dados estão particionados |
| **Partition Evolution** | Mude estratégia de particionamento sem reescrever |
| **Schema Evolution** | Adicione/remova/renomeie colunas com segurança |
| **Time Travel** | Consulte snapshots históricos |
| **Serializable Isolation** | Transações ACID completas |
| **Multi-Engine** | Funciona com Spark, Flink, Trino, DuckDB, etc. |

## Instalando a Extensão Iceberg no DuckDB

### SQL

```sql
-- Instalar
INSTALL iceberg;

-- Carregar
LOAD iceberg;

-- Verificar
SELECT * FROM duckdb_extensions() 
WHERE extension_name = 'iceberg';
```

### Python

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg")
conn.execute("LOAD iceberg")

# Testar
result = conn.execute("""
    SELECT extension_name, loaded, installed 
    FROM duckdb_extensions() 
    WHERE extension_name = 'iceberg'
""").fetchall()

print("Iceberg Extension:", result)
```

## Estrutura de uma Tabela Iceberg

Iceberg usa uma arquitetura hierárquica de metadados:

```
tabela_iceberg/
├── metadata/
│   ├── v1.metadata.json              # Snapshot 1
│   ├── v2.metadata.json              # Snapshot 2
│   ├── snap-5839472039485839.avro   # Manifest List
│   └── manifest-xxx.avro             # Manifests
└── data/
    ├── part-00000.parquet
    ├── part-00001.parquet
    └── part-00002.parquet
```

**Camadas de Metadados**:

1. **Metadata JSON**: Aponta para snapshots, schema, particionamento
2. **Manifest List**: Lista de todos os manifests de um snapshot
3. **Manifest Files**: Listam arquivos de dados + estatísticas (min/max, null count)
4. **Data Files**: Arquivos Parquet com os dados reais

## Lendo Tabelas Iceberg

### Leitura Básica

```sql
-- Sintaxe básica
SELECT * FROM iceberg_scan('path/to/iceberg_table');

-- Com filtros
SELECT 
    categoria,
    COUNT(*) as vendas,
    SUM(valor) as receita
FROM iceberg_scan('./data/vendas_iceberg')
WHERE data >= '2024-01-01'
GROUP BY categoria
ORDER BY receita DESC;
```

### Leitura Remota (S3/GCS/Azure)

```sql
-- Configurar AWS Secret
CREATE SECRET aws_iceberg (
    TYPE S3,
    KEY_ID 'sua_key',
    SECRET 'seu_secret',
    REGION 'us-east-1'
);

-- Ler tabela Iceberg no S3
SELECT * FROM iceberg_scan('s3://warehouse/db/tabela');

-- Query complexa
SELECT 
    DATE_TRUNC('month', event_date) as month,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM iceberg_scan('s3://analytics/events_iceberg')
WHERE event_date BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY month, event_type
ORDER BY month, event_count DESC;
```

### Python

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")

# Ler e analisar
df = conn.execute("""
    SELECT 
        category,
        COUNT(*) as products,
        AVG(price) as avg_price,
        MAX(price) as max_price
    FROM iceberg_scan('./data/products_iceberg')
    GROUP BY category
""").fetchdf()

print(df)
```

## Hidden Partitioning: A Vantagem do Iceberg

### Problema com Hive/Delta

```sql
-- Hive-style: Usuário PRECISA saber o particionamento
SELECT * FROM vendas
WHERE year = 2024 AND month = 6;  -- Tem que especificar!
```

### Solução Iceberg

```sql
-- Iceberg: Transparent partitioning
SELECT * FROM iceberg_scan('./data/vendas_iceberg')
WHERE data >= '2024-06-01' AND data < '2024-07-01';
-- Iceberg automaticamente entende e faz partition pruning!
```

**Vantagem**: Queries funcionam mesmo se você mudar o particionamento depois.

## Time Travel com Iceberg

Iceberg mantém snapshots imutáveis de todos os estados da tabela.

### Consultar Snapshots

```sql
-- Snapshot mais recente
SELECT COUNT(*) FROM iceberg_scan('./data/vendas_iceberg');

-- Snapshot específico
SELECT COUNT(*) 
FROM iceberg_scan('./data/vendas_iceberg', 
    snapshot_id => '5839472039485839');

-- Por timestamp
SELECT COUNT(*) 
FROM iceberg_scan('./data/vendas_iceberg', 
    timestamp => '2024-06-15 14:30:00');

-- Listar todos os snapshots disponíveis
SELECT * FROM iceberg_metadata('./data/vendas_iceberg', 'snapshots');
```

### Exemplo Prático: Comparar Snapshots

```sql
WITH 
    current_data AS (
        SELECT produto_id, SUM(valor) as valor_atual
        FROM iceberg_scan('./data/vendas_iceberg')
        GROUP BY produto_id
    ),
    historical_data AS (
        SELECT produto_id, SUM(valor) as valor_historico
        FROM iceberg_scan('./data/vendas_iceberg', 
            timestamp => '2024-01-01 00:00:00')
        GROUP BY produto_id
    )
SELECT 
    COALESCE(c.produto_id, h.produto_id) as produto_id,
    COALESCE(c.valor_atual, 0) as atual,
    COALESCE(h.valor_historico, 0) as historico,
    COALESCE(c.valor_atual, 0) - COALESCE(h.valor_historico, 0) as crescimento
FROM current_data c
FULL OUTER JOIN historical_data h ON c.produto_id = h.produto_id
WHERE ABS(COALESCE(c.valor_atual, 0) - COALESCE(h.valor_historico, 0)) > 1000
ORDER BY ABS(crescimento) DESC
LIMIT 20;
```

## Metadados do Iceberg

O DuckDB permite explorar os metadados internos do Iceberg:

### Função iceberg_metadata()

```sql
-- Listar snapshots
SELECT * FROM iceberg_metadata('./data/vendas_iceberg', 'snapshots');

-- Ver schema
SELECT * FROM iceberg_metadata('./data/vendas_iceberg', 'schema');

-- Ver particionamento
SELECT * FROM iceberg_metadata('./data/vendas_iceberg', 'partitions');

-- Ver arquivos de dados
SELECT * FROM iceberg_metadata('./data/vendas_iceberg', 'files');
```

### Python: Análise Detalhada de Metadados

```python
import duckdb
import pandas as pd

class IcebergExplorer:
    def __init__(self, table_path):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL iceberg; LOAD iceberg;")
        self.table_path = table_path
    
    def get_snapshots(self):
        """Lista todos os snapshots"""
        return self.conn.execute(f"""
            SELECT * FROM iceberg_metadata('{self.table_path}', 'snapshots')
        """).fetchdf()
    
    def get_schema(self):
        """Retorna schema da tabela"""
        return self.conn.execute(f"""
            SELECT * FROM iceberg_metadata('{self.table_path}', 'schema')
        """).fetchdf()
    
    def get_files_stats(self):
        """Estatísticas dos arquivos de dados"""
        return self.conn.execute(f"""
            SELECT 
                COUNT(*) as num_files,
                SUM(file_size_in_bytes) / 1024 / 1024 as total_size_mb,
                AVG(file_size_in_bytes) / 1024 / 1024 as avg_size_mb,
                MIN(file_size_in_bytes) / 1024 / 1024 as min_size_mb,
                MAX(file_size_in_bytes) / 1024 / 1024 as max_size_mb
            FROM iceberg_metadata('{self.table_path}', 'files')
        """).fetchdf()
    
    def analyze_partitions(self):
        """Analisa distribuição de partições"""
        return self.conn.execute(f"""
            SELECT * FROM iceberg_metadata('{self.table_path}', 'partitions')
            ORDER BY record_count DESC
        """).fetchdf()

# Uso
explorer = IcebergExplorer('./data/vendas_iceberg')

print("=== Snapshots ===")
print(explorer.get_snapshots())

print("\n=== Schema ===")
print(explorer.get_schema())

print("\n=== Estatísticas de Arquivos ===")
print(explorer.get_files_stats())

print("\n=== Partições ===")
print(explorer.analyze_partitions())
```

## Criando Tabelas Iceberg

**Nota**: DuckDB (v1.4) pode **ler** Iceberg, mas para **escrever** você precisa de ferramentas como Spark, Flink ou PyIceberg.

### Usando PyIceberg

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType, DecimalType, DateType
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

# 1. Criar catálogo
catalog = load_catalog('my_catalog', 
    type='filesystem',
    warehouse='./warehouse'
)

# 2. Definir schema
schema = Schema(
    NestedField(1, "id", IntegerType(), required=True),
    NestedField(2, "data", DateType(), required=True),
    NestedField(3, "produto", StringType(), required=True),
    NestedField(4, "categoria", StringType(), required=True),
    NestedField(5, "valor", DecimalType(10, 2), required=True)
)

# 3. Criar tabela
table = catalog.create_table(
    identifier='db.vendas',
    schema=schema,
    partition_spec=PartitionSpec(
        PartitionField(source_id=2, field_id=1000, transform=MonthTransform(), name="data_month")
    )
)

# 4. Inserir dados
df = pd.DataFrame({
    'id': [1, 2, 3],
    'data': pd.to_datetime(['2024-01-15', '2024-02-20', '2024-03-10']),
    'produto': ['Produto A', 'Produto B', 'Produto C'],
    'categoria': ['Cat1', 'Cat2', 'Cat1'],
    'valor': [100.50, 200.75, 150.00]
})

arrow_table = pa.Table.from_pandas(df)
table.append(arrow_table)

print("Tabela Iceberg criada com sucesso!")
```

### Ler com DuckDB

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")

# Ler a tabela criada
result = conn.execute("""
    SELECT * FROM iceberg_scan('./warehouse/db/vendas')
""").fetchdf()

print(result)
```

## Schema Evolution no Iceberg

Iceberg permite evoluir o schema sem reescrever dados.

### Operações Suportadas (via Spark/PyIceberg)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog('my_catalog', type='filesystem', warehouse='./warehouse')
table = catalog.load_table('db.vendas')

# Adicionar coluna
table.update_schema().add_column('regiao', StringType()).commit()

# Renomear coluna
table.update_schema().rename_column('produto', 'nome_produto').commit()

# Remover coluna
table.update_schema().delete_column('categoria').commit()

# Mudar tipo (compatível)
table.update_schema().update_column('valor', DecimalType(12, 2)).commit()
```

### Ler Schema Evoluído no DuckDB

```sql
-- DuckDB lê o schema mais recente automaticamente
SELECT * FROM iceberg_scan('./warehouse/db/vendas');

-- Ver histórico de schemas
SELECT * FROM iceberg_metadata('./warehouse/db/vendas', 'schema');
```

## Partition Evolution: Recurso Único do Iceberg

Uma das features mais poderosas do Iceberg é poder **mudar o particionamento** sem reescrever dados.

### Exemplo

```python
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import DayTransform, MonthTransform

catalog = load_catalog('my_catalog', type='filesystem', warehouse='./warehouse')
table = catalog.load_table('db.vendas')

# Inicialmente particionado por mês
# ... dados escritos ...

# Depois, mudar para particionamento diário
table.update_spec().add_field('data', DayTransform(), 'data_day').commit()

# Novos dados usam particionamento diário
# Dados antigos continuam com particionamento mensal
# Queries funcionam transparentemente!
```

**No DuckDB**:

```sql
-- Query funciona sem mudanças, mesmo com particionamento híbrido
SELECT * FROM iceberg_scan('./warehouse/db/vendas')
WHERE data BETWEEN '2024-06-01' AND '2024-06-30';
-- Iceberg usa metadados para otimizar independente do particionamento
```

## Integração Multi-Engine

### Escrever com Spark, Ler com DuckDB

```python
# No Spark (PySpark)
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergWrite") \
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.my_catalog.type", "hadoop") \
    .config("spark.sql.catalog.my_catalog.warehouse", "s3://warehouse/") \
    .getOrCreate()

# Escrever dados
df = spark.read.parquet("data.parquet")
df.writeTo("my_catalog.db.vendas").create()

# No DuckDB (leitura)
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")

result = conn.execute("""
    SELECT * FROM iceberg_scan('s3://warehouse/db/vendas')
""").fetchdf()

print(result)
```

## Performance e Otimizações

### 1. Metadata Filtering

Iceberg armazena estatísticas (min/max, null count) nos manifests:

```sql
-- Query otimizada com metadata filtering
SELECT * FROM iceberg_scan('./data/vendas_iceberg')
WHERE valor > 10000;
-- Iceberg lê apenas manifests onde max(valor) > 10000
-- Arquivos com max(valor) < 10000 são pulados
```

### 2. Partition Pruning

```sql
-- Com particionamento por mês
SELECT * FROM iceberg_scan('./data/vendas_iceberg')
WHERE data >= '2024-06-01' AND data < '2024-07-01';
-- Lê apenas partição de junho
```

### 3. Column Pruning

```sql
-- Lê apenas colunas necessárias (formato colunar)
SELECT id, valor FROM iceberg_scan('./data/vendas_iceberg');
-- Não lê colunas produto, categoria, etc.
```

### Benchmark Exemplo

```python
import duckdb
import time

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")

# Query com filtros otimizados
start = time.time()
result = conn.execute("""
    SELECT 
        categoria,
        COUNT(*) as vendas,
        SUM(valor) as receita
    FROM iceberg_scan('s3://warehouse/vendas_iceberg')
    WHERE data >= '2024-01-01' 
      AND valor > 100
    GROUP BY categoria
""").fetchdf()
elapsed = time.time() - start

print(f"Query executada em {elapsed:.2f}s")
print(f"Rows retornadas: {len(result)}")
```

## Comparação: Delta Lake vs Iceberg

| Aspecto | Delta Lake | Apache Iceberg |
|---------|-----------|----------------|
| **Metadados** | JSON log | JSON + Avro manifests |
| **Hidden Partitioning** | ❌ | ✅ |
| **Partition Evolution** | ❌ | ✅ |
| **Multi-Engine** | ⚠️ (foco Spark) | ✅✅ (design) |
| **Time Travel** | ✅ | ✅ |
| **Schema Evolution** | ✅ | ✅ |
| **Compactação** | ✅ (OPTIMIZE) | ✅ (rewrite) |
| **Maturidade** | Alta | Alta |
| **Ecossistema** | Databricks | Netflix, Apple, AWS |

**Quando usar Iceberg**:
- Múltiplas engines (DuckDB, Trino, Flink, Spark)
- Partition evolution é crítico
- Performance de leitura é prioridade
- Arquitetura vendor-neutral

**Quando usar Delta**:
- Já usa Databricks/Spark
- Simplicidade é prioridade
- Ecossistema Databricks

## Casos de Uso Reais

### Caso 1: Analytics Federado

```sql
-- Combinar dados de múltiplas tabelas Iceberg
WITH 
    vendas AS (
        SELECT * FROM iceberg_scan('s3://warehouse/vendas')
    ),
    produtos AS (
        SELECT * FROM iceberg_scan('s3://warehouse/produtos')
    ),
    clientes AS (
        SELECT * FROM iceberg_scan('s3://warehouse/clientes')
    )
SELECT 
    c.segmento,
    p.categoria,
    COUNT(DISTINCT v.venda_id) as vendas,
    SUM(v.valor) as receita,
    AVG(v.valor) as ticket_medio
FROM vendas v
JOIN produtos p ON v.produto_id = p.id
JOIN clientes c ON v.cliente_id = c.id
WHERE v.data >= '2024-01-01'
GROUP BY c.segmento, p.categoria
ORDER BY receita DESC;
```

### Caso 2: Time Series Analysis

```python
import duckdb
import pandas as pd

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")

# Análise temporal com múltiplos snapshots
snapshots = [
    ('2024-01-01', '5839472039485839'),
    ('2024-02-01', '5839472039485840'),
    ('2024-03-01', '5839472039485841'),
]

results = []
for date, snapshot_id in snapshots:
    df = conn.execute(f"""
        SELECT 
            '{date}' as reference_date,
            COUNT(*) as records,
            SUM(valor) as revenue
        FROM iceberg_scan('./data/vendas', snapshot_id => '{snapshot_id}')
    """).fetchdf()
    results.append(df)

time_series = pd.concat(results)
print(time_series)
```

## Próximos Passos

No próximo capítulo, vamos explorar Apache Hudi e outros formatos emergentes como Apache Paimon.

## Recursos

- [Apache Iceberg Docs](https://iceberg.apache.org/)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
- [PyIceberg](https://py.iceberg.apache.org/)

---

**Exercício Prático**:

Explore metadados de uma tabela Iceberg:

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")

# Assumindo que você tem uma tabela Iceberg
table_path = './data/minha_tabela_iceberg'

# Ver snapshots
print("=== SNAPSHOTS ===")
snapshots = conn.execute(f"""
    SELECT * FROM iceberg_metadata('{table_path}', 'snapshots')
""").fetchdf()
print(snapshots)

# Ver arquivos
print("\n=== FILES ===")
files = conn.execute(f"""
    SELECT * FROM iceberg_metadata('{table_path}', 'files')
""").fetchdf()
print(files)
```
