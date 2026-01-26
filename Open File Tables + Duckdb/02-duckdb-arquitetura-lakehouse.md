# Capítulo 02: DuckDB e Arquiteturas de Lakehouse

## O que é uma Arquitetura Lakehouse?

Um **Lakehouse** combina o melhor dos **Data Lakes** (flexibilidade, baixo custo, armazenamento de dados brutos) com os **Data Warehouses** (estrutura, performance, transações ACID). É a evolução natural do armazenamento de dados analíticos.

### Data Lake vs Data Warehouse vs Lakehouse

```
┌─────────────────┬──────────────────┬──────────────────┬──────────────────┐
│ Característica  │ Data Lake        │ Data Warehouse   │ Lakehouse        │
├─────────────────┼──────────────────┼──────────────────┼──────────────────┤
│ Formato         │ Qualquer         │ Estruturado      │ Open Formats     │
│ Schema          │ Schema-on-read   │ Schema-on-write  │ Flexível         │
│ Transações      │ ❌               │ ✅               │ ✅               │
│ Performance     │ Baixa-Média      │ Alta             │ Alta             │
│ Custo Storage   │ Muito Baixo      │ Alto             │ Baixo            │
│ Tempo Real      │ Difícil          │ Difícil          │ ✅               │
│ ML/IA           │ ✅               │ Limitado         │ ✅               │
└─────────────────┴──────────────────┴──────────────────┴──────────────────┘
```

## DuckDB como Query Engine para Lakehouse

DuckDB é o **query engine ideal** para arquiteturas lakehouse modernas por várias razões:

### 1. Execução In-Process (Embedded)

```python
import duckdb

# DuckDB roda no mesmo processo da aplicação
conn = duckdb.connect()

# Consulta direta em arquivos sem servidor
result = conn.execute("""
    SELECT 
        categoria,
        SUM(valor) as total_vendas
    FROM 's3://meu-bucket/vendas/*.parquet'
    WHERE data >= '2024-01-01'
    GROUP BY categoria
""").fetchdf()

print(result)
```

**Vantagens**:
- ⚡ Zero latência de rede (sem servidor separado)
- 💾 Zero overhead de serialização
- 🔒 Sem necessidade de credenciais complexas
- 📦 Distribuível como biblioteca

### 2. Query Federation (Múltiplas Fontes)

DuckDB pode consultar múltiplos formatos e fontes simultaneamente:

```sql
-- Instalando extensões necessárias
INSTALL delta;
INSTALL iceberg;
INSTALL postgres;
LOAD delta;
LOAD iceberg;
LOAD postgres;

-- Query federado: Delta + Iceberg + PostgreSQL + Parquet
WITH 
    -- Dados em Delta Lake
    vendas_delta AS (
        SELECT * FROM delta_scan('s3://analytics/vendas_delta')
    ),
    -- Dados em Iceberg
    produtos_iceberg AS (
        SELECT * FROM iceberg_scan('s3://warehouse/produtos_iceberg')
    ),
    -- Dados em PostgreSQL (transacional)
    clientes_postgres AS (
        SELECT * FROM postgres_query('postgres_db', 
            'SELECT id, nome, segmento FROM clientes')
    ),
    -- Dados em Parquet (raw data lake)
    logs_parquet AS (
        SELECT * FROM read_parquet('s3://logs/eventos/*.parquet')
    )
SELECT 
    c.segmento,
    p.categoria,
    COUNT(DISTINCT v.venda_id) as total_vendas,
    SUM(v.valor) as receita_total,
    COUNT(l.evento_id) as total_eventos
FROM vendas_delta v
JOIN produtos_iceberg p ON v.produto_id = p.id
JOIN clientes_postgres c ON v.cliente_id = c.id
LEFT JOIN logs_parquet l ON l.venda_id = v.venda_id
WHERE v.data >= '2024-01-01'
GROUP BY c.segmento, p.categoria
ORDER BY receita_total DESC;
```

### 3. Zero-Copy e Streaming

DuckDB não copia dados desnecessariamente:

```sql
-- Pipeline de processamento com zero-copy
COPY (
    SELECT 
        YEAR(data) as ano,
        MONTH(data) as mes,
        categoria,
        COUNT(*) as vendas,
        SUM(valor) as receita
    FROM delta_scan('s3://raw/vendas_delta')
    WHERE data >= '2024-01-01'
    GROUP BY ano, mes, categoria
) TO 's3://processed/vendas_agregadas.parquet' 
(FORMAT PARQUET, COMPRESSION ZSTD);
```

**Performance**:
- Leitura streaming dos blocos
- Processamento vetorizado
- Escrita paralela otimizada

## Arquiteturas Lakehouse Típicas com DuckDB

### Arquitetura 1: Lakehouse Local (Dev/Analytics)

```
┌──────────────────────────────────────────────────────┐
│                  Aplicação Python                     │
│                                                       │
│  ┌─────────────────────────────────────────────┐    │
│  │           DuckDB (In-Process)                │    │
│  │                                               │    │
│  │  ┌──────────┐  ┌──────────┐  ┌───────────┐ │    │
│  │  │  Delta   │  │ Iceberg  │  │  Parquet  │ │    │
│  │  │Extension │  │Extension │  │  Native   │ │    │
│  │  └──────────┘  └──────────┘  └───────────┘ │    │
│  └─────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │  Local File System / S3 / GCS      │
        │                                     │
        │  data/                              │
        │  ├── delta_tables/                  │
        │  ├── iceberg_tables/                │
        │  └── parquet_files/                 │
        └────────────────────────────────────┘
```

**Exemplo Prático**:

```python
# analytics_pipeline.py
import duckdb
import pandas as pd

class LakehouseAnalytics:
    def __init__(self, storage_path='./data'):
        self.conn = duckdb.connect()
        self.storage_path = storage_path
        
        # Configurar extensões
        self.conn.execute("INSTALL delta")
        self.conn.execute("INSTALL iceberg")
        self.conn.execute("LOAD delta")
        self.conn.execute("LOAD iceberg")
    
    def analyze_sales(self, start_date, end_date):
        """Análise cross-format de vendas"""
        query = f"""
        WITH sales AS (
            SELECT * FROM delta_scan('{self.storage_path}/sales_delta')
            WHERE sale_date BETWEEN '{start_date}' AND '{end_date}'
        ),
        products AS (
            SELECT * FROM iceberg_scan('{self.storage_path}/products_iceberg')
        )
        SELECT 
            p.category,
            COUNT(DISTINCT s.sale_id) as total_sales,
            SUM(s.amount) as revenue,
            AVG(s.amount) as avg_ticket
        FROM sales s
        JOIN products p ON s.product_id = p.id
        GROUP BY p.category
        ORDER BY revenue DESC
        """
        return self.conn.execute(query).fetchdf()

# Uso
analytics = LakehouseAnalytics('./data')
result = analytics.analyze_sales('2024-01-01', '2024-12-31')
print(result)
```

### Arquitetura 2: Lakehouse Cloud (Produção)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Cloud Applications                           │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Python     │  │   Node.js    │  │   Jupyter    │         │
│  │   + DuckDB   │  │   + DuckDB   │  │   + DuckDB   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌──────────────────────────────────────────────┐
        │         Object Storage (S3/GCS/Azure)        │
        │                                               │
        │  lakehouse/                                   │
        │  ├── bronze/      (raw data - Parquet)       │
        │  ├── silver/      (cleaned - Delta Lake)     │
        │  └── gold/        (aggregated - Iceberg)     │
        └──────────────────────────────────────────────┘
```

**Exemplo com Secrets (AWS S3)**:

```sql
-- Configurar credenciais AWS
CREATE SECRET aws_secret (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

-- Bronze Layer: Dados brutos em Parquet
SELECT COUNT(*) 
FROM read_parquet('s3://lakehouse/bronze/events/**/*.parquet');

-- Silver Layer: Dados limpos em Delta Lake
SELECT * 
FROM delta_scan('s3://lakehouse/silver/events_cleaned')
WHERE event_date = '2024-01-15'
LIMIT 10;

-- Gold Layer: Agregações em Iceberg
SELECT 
    date_trunc('month', event_date) as month,
    event_type,
    COUNT(*) as event_count
FROM iceberg_scan('s3://lakehouse/gold/events_aggregated')
GROUP BY month, event_type
ORDER BY month DESC, event_count DESC;
```

### Arquitetura 3: Hybrid Lakehouse (Multi-Cloud)

```
┌────────────────────────────────────────────────────────┐
│                DuckDB Federation Layer                  │
└────────────────────────────────────────────────────────┘
        │              │              │
        ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│   AWS S3    │ │   GCS       │ │Azure Blob   │
│             │ │             │ │             │
│ Delta Lake  │ │  Iceberg    │ │  Parquet    │
└─────────────┘ └─────────────┘ └─────────────┘
```

**Configuração Multi-Cloud**:

```sql
-- Configurar múltiplos secrets
CREATE SECRET aws_prod (
    TYPE S3,
    KEY_ID 'AWS_KEY',
    SECRET 'AWS_SECRET',
    REGION 'us-east-1'
);

CREATE SECRET gcp_analytics (
    TYPE GCS,
    KEY_ID 'GCP_CLIENT_EMAIL',
    SECRET 'GCP_PRIVATE_KEY'
);

CREATE SECRET azure_backup (
    TYPE AZURE,
    CONNECTION_STRING 'DefaultEndpointsProtocol=https;...'
);

-- Query cross-cloud
WITH 
    aws_data AS (
        SELECT * FROM delta_scan('s3://prod-bucket/sales')
    ),
    gcp_data AS (
        SELECT * FROM iceberg_scan('gs://analytics-bucket/products')
    ),
    azure_data AS (
        SELECT * FROM read_parquet('az://backup-container/customers.parquet')
    )
SELECT 
    a.sale_date,
    g.product_name,
    z.customer_segment,
    SUM(a.amount) as total_revenue
FROM aws_data a
JOIN gcp_data g ON a.product_id = g.id
JOIN azure_data z ON a.customer_id = z.id
GROUP BY a.sale_date, g.product_name, z.customer_segment;
```

## Padrões de Lakehouse com DuckDB

### Padrão 1: Medallion Architecture (Bronze-Silver-Gold)

```python
# medallion_pipeline.py
import duckdb
from datetime import datetime

class MedallionPipeline:
    def __init__(self):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta")
        self.conn.execute("LOAD delta")
    
    def ingest_to_bronze(self, source_path, bronze_path):
        """
        Bronze: Dados brutos, sem transformação
        Formato: Parquet (mais rápido para escrita)
        """
        self.conn.execute(f"""
            COPY (
                SELECT 
                    *,
                    current_timestamp() as _ingested_at,
                    '{source_path}' as _source_file
                FROM read_csv('{source_path}', AUTO_DETECT=TRUE)
            ) TO '{bronze_path}/data.parquet' 
            (FORMAT PARQUET, PARTITION_BY (YEAR(_ingested_at), MONTH(_ingested_at)))
        """)
    
    def transform_to_silver(self, bronze_path, silver_path):
        """
        Silver: Dados limpos e validados
        Formato: Delta Lake (suporta updates e deletes)
        """
        self.conn.execute(f"""
            COPY (
                SELECT 
                    id,
                    TRIM(UPPER(name)) as name,
                    CAST(amount AS DECIMAL(10,2)) as amount,
                    TRY_CAST(date AS DATE) as date,
                    current_timestamp() as _processed_at
                FROM read_parquet('{bronze_path}/**/*.parquet')
                WHERE amount > 0 
                  AND date IS NOT NULL
            ) TO '{silver_path}' 
            (FORMAT DELTA)
        """)
    
    def aggregate_to_gold(self, silver_path, gold_path):
        """
        Gold: Agregações para analytics
        Formato: Iceberg (ótimo para queries analíticas)
        """
        self.conn.execute(f"""
            COPY (
                SELECT 
                    DATE_TRUNC('month', date) as month,
                    name,
                    COUNT(*) as transaction_count,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM delta_scan('{silver_path}')
                GROUP BY month, name
            ) TO '{gold_path}' 
            (FORMAT PARQUET)  -- Ou Iceberg quando disponível
        """)

# Uso
pipeline = MedallionPipeline()
pipeline.ingest_to_bronze('data/raw/sales.csv', 'lakehouse/bronze/sales')
pipeline.transform_to_silver('lakehouse/bronze/sales', 'lakehouse/silver/sales')
pipeline.aggregate_to_gold('lakehouse/silver/sales', 'lakehouse/gold/sales_summary')
```

### Padrão 2: Time Travel e Auditoria

```sql
-- Delta Lake com Time Travel
-- Versão atual
SELECT COUNT(*) as current_count 
FROM delta_scan('s3://lakehouse/silver/sales');

-- Versão de 7 dias atrás
SELECT COUNT(*) as count_7_days_ago
FROM delta_scan('s3://lakehouse/silver/sales', 
    timestamp => '2024-01-15 00:00:00');

-- Comparar mudanças
WITH 
    current AS (
        SELECT produto_id, SUM(valor) as valor_atual
        FROM delta_scan('s3://lakehouse/silver/sales')
        GROUP BY produto_id
    ),
    previous AS (
        SELECT produto_id, SUM(valor) as valor_anterior
        FROM delta_scan('s3://lakehouse/silver/sales', 
            version => 10)
        GROUP BY produto_id
    )
SELECT 
    c.produto_id,
    c.valor_atual,
    p.valor_anterior,
    c.valor_atual - p.valor_anterior as diferenca,
    ROUND((c.valor_atual - p.valor_anterior) / p.valor_anterior * 100, 2) as pct_mudanca
FROM current c
JOIN previous p ON c.produto_id = p.produto_id
WHERE ABS(c.valor_atual - p.valor_anterior) > 1000
ORDER BY ABS(diferenca) DESC;
```

### Padrão 3: Incremental Processing

```python
# incremental_processor.py
import duckdb
from datetime import datetime, timedelta

class IncrementalProcessor:
    def __init__(self, conn):
        self.conn = conn
        
    def get_last_processed_date(self, table_name):
        """Recupera a última data processada"""
        result = self.conn.execute(f"""
            SELECT MAX(processed_date) 
            FROM delta_scan('lakehouse/silver/{table_name}')
        """).fetchone()
        return result[0] if result[0] else datetime(2020, 1, 1)
    
    def process_incremental(self, source_table, target_table):
        """Processa apenas novos dados"""
        last_date = self.get_last_processed_date(target_table)
        
        self.conn.execute(f"""
            INSERT INTO delta_scan('lakehouse/silver/{target_table}')
            SELECT 
                *,
                current_timestamp() as processed_date
            FROM read_parquet('lakehouse/bronze/{source_table}/**/*.parquet')
            WHERE event_date > '{last_date}'
        """)
        
        print(f"Processed data after {last_date}")

# Uso
conn = duckdb.connect()
processor = IncrementalProcessor(conn)
processor.process_incremental('events', 'events_processed')
```

## Vantagens do DuckDB em Lakehouse

### 1. Simplicidade Operacional

```python
# Sem necessidade de clusters Spark ou infraestrutura complexa
import duckdb

# Uma linha de código para processar TB de dados
result = duckdb.sql("""
    SELECT * FROM 's3://huge-dataset/**/*.parquet' 
    WHERE date >= '2024-01-01'
""").fetchdf()
```

### 2. Baixo Custo

- **Sem servidor dedicado**: Roda na aplicação
- **Sem licenças**: Open source
- **Eficiência de storage**: Compressão otimizada
- **Pay-per-query**: Apenas storage em cloud

### 3. Flexibilidade

```sql
-- Mesma query funciona em diferentes ambientes
-- Laptop (desenvolvimento)
SELECT * FROM 'file:///local/data/sales.parquet';

-- Produção (cloud)
SELECT * FROM 's3://production/data/sales.parquet';

-- Híbrido
SELECT * FROM 'gs://analytics/data/sales.parquet';
```

## Limitações e Quando NÃO usar DuckDB

### Limitações:

1. **Single-node**: Não distribui processamento (use Spark para >TB frequente)
2. **Escritas concorrentes**: Não suporta múltiplos writers simultâneos
3. **Streaming real-time**: Melhor para batch/micro-batch

### Use Spark/Flink quando:
- Processamento distribuído constante
- Múltiplos writers simultâneos
- Streaming sub-segundo crítico

## Próximos Passos

No próximo capítulo, vamos mergulhar profundamente na extensão Delta Lake do DuckDB, explorando transações, time travel e otimizações práticas.

## Recursos

- [DuckDB Extensions](https://duckdb.org/docs/extensions/overview)
- [DuckDB Secrets](https://duckdb.org/docs/configuration/secrets_manager)
- [Data Lake vs Lakehouse](https://www.databricks.com/glossary/data-lakehouse)

---

**Exercício Prático**:

Crie uma arquitetura Medallion simples localmente:

```bash
mkdir -p lakehouse/{bronze,silver,gold}
```

```python
import duckdb
import pandas as pd

# Gerar dados de exemplo
df = pd.DataFrame({
    'id': range(1, 101),
    'product': ['Product_' + str(i % 10) for i in range(1, 101)],
    'amount': [100 + i * 10 for i in range(1, 101)],
    'date': pd.date_range('2024-01-01', periods=100)
})

# Bronze
df.to_parquet('lakehouse/bronze/sales.parquet')

# Silver (com Delta)
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")
conn.execute("""
    COPY (
        SELECT * FROM 'lakehouse/bronze/sales.parquet'
        WHERE amount > 0
    ) TO 'lakehouse/silver/sales' (FORMAT DELTA)
""")

print("Lakehouse criado com sucesso!")
```
