# -*- coding: utf-8 -*-
"""
capitulo_10_casos_uso_avancados
"""

# capitulo_10_casos_uso_avancados
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

print("""
Caso de Uso: Multi-Cloud ETL Pipeline
════════════════════════════════════════════════════════

Cenário:
────────
- Dados source em AWS S3
- Processing intermediário em GCP
- Resultado final em Azure
- Tudo orquestrado via DuckDB

Arquitetura:
────────────
AWS S3 (Raw) → DuckDB → GCS (Processed) → DuckDB → Azure (Final)
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL azure; LOAD azure;")

# Configure secrets
print("\n1. Configurando secrets...")

# AWS S3 (source)
con.execute("""
    CREATE SECRET s3_source (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'env;config',
        SCOPE 's3://company-raw-data/'
    )
""")

# GCP (intermediate)
con.execute("""
    CREATE SECRET gcs_processing (
        TYPE gcs,
        PROVIDER credential_chain,
        SCOPE 'gs://company-processing/'
    )
""")

# Azure (destination)
con.execute("""
    CREATE SECRET azure_final (
        TYPE azure,
        PROVIDER managed_identity,
        ACCOUNT_NAME 'companyfinal',
        SCOPE 'azure://final-data/'
    )
""")

print("✓ Secrets configurados")

# ETL Pipeline
etl_pipeline = """
-- Step 1: Extract from AWS S3
CREATE TEMP TABLE raw_events AS
SELECT
    event_id,
    user_id,
    event_type,
    timestamp,
    properties
FROM read_parquet('s3://company-raw-data/events/2024/01/*.parquet');

-- Step 2: Transform
CREATE TEMP TABLE processed_events AS
SELECT
    event_id,
    user_id,
    event_type,
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) OVER (PARTITION BY user_id) as user_event_count,
    JSON_EXTRACT_STRING(properties, '$.country') as country,
    JSON_EXTRACT_STRING(properties, '$.device') as device
FROM raw_events
WHERE timestamp >= '2024-01-01'
    AND event_type IN ('page_view', 'click', 'purchase');

-- Step 3: Load to GCS (intermediate)
COPY processed_events
TO 'gs://company-processing/events/processed.parquet'
(FORMAT PARQUET, PARTITION_BY (country, hour));

-- Step 4: Aggregations
CREATE TEMP TABLE aggregated_metrics AS
SELECT
    hour,
    country,
    device,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM processed_events
GROUP BY 1, 2, 3, 4;

-- Step 5: Load to Azure (final)
COPY aggregated_metrics
TO 'azure://final-data/metrics/daily_metrics.parquet'
(FORMAT PARQUET, COMPRESSION 'zstd');
"""

print("\n2. ETL Pipeline:")
print(etl_pipeline)

print("""
Vantagens Multi-Cloud:
──────────────────────
✓ Vendor lock-in mitigation
✓ Cost optimization por workload
✓ Geographic data residency
✓ Best-of-breed services
✓ Disaster recovery

Desafios:
─────────
✗ Egress costs (exceto R2)
✗ Complexidade de gestão
✗ Multiple authentication systems
✗ Data consistency
✗ Latência cross-cloud

Best Practices:
───────────────
✓ Minimize data movement
✓ Use compression (zstd)
✓ Partition appropriately
✓ Monitor costs
✓ Implement retry logic
""")

con.close()

# Exemplo/Bloco 2
import duckdb
from datetime import datetime, timedelta

print("""
Incremental Multi-Cloud Sync:
════════════════════════════════════════════════════════
""")

def incremental_multicloud_sync(source_secret, dest_secret, source_path, dest_path):
    """
    Sync incremental entre clouds
    """
    con = duckdb.connect('sync.duckdb')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    print(f"\n{'='*60}")
    print(f"Incremental Sync")
    print(f"{'='*60}")
    print(f"Source: {source_path}")
    print(f"Dest: {dest_path}")

    # 1. Obter último sync
    try:
        last_sync = con.execute("""
            SELECT MAX(sync_timestamp) as last_sync
            FROM sync_metadata
            WHERE source_path = ?
        """, [source_path]).fetchone()[0]
    except:
        # Primeira sync
        con.execute("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                source_path VARCHAR,
                dest_path VARCHAR,
                sync_timestamp TIMESTAMP,
                records_synced BIGINT
            )
        """)
        last_sync = datetime(2020, 1, 1)

    print(f"\n1. Last sync: {last_sync}")

    # 2. Read incremental data
    query = f"""
        SELECT *
        FROM read_parquet('{source_path}')
        WHERE updated_at > '{last_sync}'
    """

    print(f"2. Reading incremental data...")
    incremental_data = con.execute(query)

    # 3. Write to destination
    print(f"3. Writing to {dest_path}...")
    con.execute(f"""
        COPY (
            {query}
        ) TO '{dest_path}'
        (FORMAT PARQUET, COMPRESSION 'snappy', APPEND true)
    """)

    # 4. Update metadata
    records_count = con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
    con.execute("""
        INSERT INTO sync_metadata
        VALUES (?, ?, ?, ?)
    """, [source_path, dest_path, datetime.now(), records_count])

    print(f"4. Synced {records_count} records")
    print(f"✓ Sync completed!")

    con.close()

# Documentação
print("""
Implementação:
──────────────

# Schedule com cron ou Airflow
0 * * * * python incremental_sync.py  # A cada hora

# Ou via Airflow DAG
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    'multicloud_sync',
    schedule_interval='@hourly',
    catchup=False
)

sync_task = PythonOperator(
    task_id='incremental_sync',
    python_callable=incremental_multicloud_sync,
    op_kwargs={
        'source_secret': 's3_source',
        'dest_secret': 'azure_dest',
        'source_path': 's3://source/data/*.parquet',
        'dest_path': 'azure://dest/data/'
    },
    dag=dag
)

Monitoramento:
──────────────
- Records synced per run
- Sync duration
- Error rate
- Data lag
- Cost per sync
""")

# Exemplo/Bloco 3
import duckdb

print("""
Medallion Architecture com DuckDB:
════════════════════════════════════════════════════════

Bronze → Silver → Gold

Bronze: Raw data (S3)
Silver: Cleaned, validated (GCS)
Gold: Business aggregates (Azure)
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL azure; LOAD azure;")

# Configure secrets por layer
con.execute("""
    CREATE SECRET bronze_s3 (
        TYPE s3,
        PROVIDER credential_chain,
        SCOPE 's3://datalake-bronze/'
    )
""")

con.execute("""
    CREATE SECRET silver_gcs (
        TYPE gcs,
        PROVIDER credential_chain,
        SCOPE 'gs://datalake-silver/'
    )
""")

con.execute("""
    CREATE SECRET gold_azure (
        TYPE azure,
        PROVIDER service_principal,
        TENANT_ID 'tenant-id',
        CLIENT_ID 'client-id',
        CLIENT_SECRET 'client-secret',
        ACCOUNT_NAME 'datalakegold',
        SCOPE 'azure://gold/'
    )
""")

# Bronze → Silver
bronze_to_silver = """
-- Bronze Layer: Raw events from S3
CREATE OR REPLACE TEMP TABLE bronze_events AS
SELECT *
FROM read_parquet('s3://datalake-bronze/events/year=*/month=*/day=*/*.parquet');

-- Silver Layer: Cleaned and validated
CREATE OR REPLACE TEMP TABLE silver_events AS
SELECT
    event_id,
    user_id,
    -- Data quality: remove nulls
    COALESCE(event_type, 'unknown') as event_type,
    -- Data quality: valid timestamps only
    CASE
        WHEN timestamp >= '2020-01-01'
         AND timestamp <= CURRENT_TIMESTAMP
        THEN timestamp
        ELSE NULL
    END as timestamp,
    -- Data quality: validate JSON
    CASE
        WHEN JSON_VALID(properties)
        THEN properties
        ELSE NULL
    END as properties,
    CURRENT_TIMESTAMP as processed_at
FROM bronze_events
WHERE user_id IS NOT NULL  -- Data quality rule
    AND event_id IS NOT NULL;

-- Write to Silver (GCS)
COPY silver_events
TO 'gs://datalake-silver/events/'
(FORMAT PARQUET, PARTITION_BY (DATE_TRUNC('day', timestamp)));
"""

# Silver → Gold
silver_to_gold = """
-- Silver Layer: Read cleaned data
CREATE OR REPLACE TEMP TABLE silver_events AS
SELECT *
FROM read_parquet('gs://datalake-silver/events/year=*/month=*/day=*/*.parquet');

-- Gold Layer: Business metrics
CREATE OR REPLACE TEMP TABLE gold_user_metrics AS
SELECT
    user_id,
    DATE_TRUNC('day', timestamp) as date,
    COUNT(*) as total_events,
    COUNT(DISTINCT event_type) as unique_event_types,
    MIN(timestamp) as first_event,
    MAX(timestamp) as last_event,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
    SUM(CAST(JSON_EXTRACT(properties, '$.amount') AS DOUBLE)) as total_amount
FROM silver_events
WHERE timestamp IS NOT NULL
GROUP BY user_id, DATE_TRUNC('day', timestamp);

-- Write to Gold (Azure)
COPY gold_user_metrics
TO 'azure://gold/metrics/user_daily_metrics/'
(FORMAT PARQUET, PARTITION_BY (date));
"""

print("\n1. Bronze → Silver:")
print(bronze_to_silver)

print("\n2. Silver → Gold:")
print(silver_to_gold)

print("""
Data Quality Rules:
───────────────────

Bronze:
✓ Schema enforcement
✓ Duplicate detection
✓ Format validation

Silver:
✓ Null handling
✓ Type validation
✓ Referential integrity
✓ Business rule validation
✓ Deduplication

Gold:
✓ Aggregation accuracy
✓ Metric definitions
✓ SLA compliance
✓ Data freshness

Orchestration:
──────────────
1. Bronze ingestion: Real-time ou batch
2. Silver processing: Hourly
3. Gold aggregation: Daily
4. Retention: Bronze (30d), Silver (1y), Gold (7y)
""")

con.close()

# Exemplo/Bloco 4
import duckdb

print("""
Lambda Architecture com DuckDB:
════════════════════════════════════════════════════════

Batch Layer + Speed Layer → Serving Layer

Batch Layer: Complete historical data (S3)
Speed Layer: Recent data, incremental (GCS)
Serving Layer: Merged views (Azure)
""")

lambda_arch = """
-- Batch Layer: Historical aggregations
CREATE OR REPLACE VIEW batch_metrics AS
SELECT
    user_id,
    DATE_TRUNC('day', timestamp) as date,
    COUNT(*) as event_count,
    SUM(amount) as total_amount
FROM read_parquet('s3://datalake/batch/events/**/*.parquet')
WHERE timestamp < CURRENT_DATE
GROUP BY 1, 2;

-- Speed Layer: Real-time incremental
CREATE OR REPLACE VIEW speed_metrics AS
SELECT
    user_id,
    DATE_TRUNC('day', timestamp) as date,
    COUNT(*) as event_count,
    SUM(amount) as total_amount
FROM read_parquet('gs://datalake/speed/events/**/*.parquet')
WHERE timestamp >= CURRENT_DATE
GROUP BY 1, 2;

-- Serving Layer: Merged view
CREATE OR REPLACE VIEW serving_metrics AS
SELECT
    COALESCE(b.user_id, s.user_id) as user_id,
    COALESCE(b.date, s.date) as date,
    COALESCE(b.event_count, 0) + COALESCE(s.event_count, 0) as event_count,
    COALESCE(b.total_amount, 0) + COALESCE(s.total_amount, 0) as total_amount
FROM batch_metrics b
FULL OUTER JOIN speed_metrics s
    ON b.user_id = s.user_id
    AND b.date = s.date;

-- Materialize to serving layer
COPY serving_metrics
TO 'azure://serving/metrics/user_metrics.parquet'
(FORMAT PARQUET);
"""

print(lambda_arch)

print("""
Vantagens Lambda:
─────────────────
✓ Real-time + historical data
✓ Fault tolerance
✓ Scalability
✓ Reprocessing capability

DuckDB Role:
────────────
✓ Query engine para batch layer
✓ Merge de batch + speed layers
✓ Serving layer queries
✓ Data quality checks

Alternativa: Kappa Architecture
────────────────────────────────
Use apenas streaming (sem batch layer)
DuckDB queries real-time stream storage
""")

# Exemplo/Bloco 5
import duckdb

print("""
Database Federation com DuckDB:
════════════════════════════════════════════════════════

Cenário:
────────
- Users em PostgreSQL
- Orders em MySQL
- Products em S3 Parquet
- Analytics em DuckDB
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")
con.execute("INSTALL mysql_scanner; LOAD mysql_scanner;")

# Configure secrets
con.execute("""
    CREATE SECRET postgres_users (
        TYPE postgres,
        HOST 'postgres.example.com',
        DATABASE 'users_db',
        USER 'readonly',
        PASSWORD 'password',
        SSLMODE 'verify-full'
    )
""")

con.execute("""
    CREATE SECRET mysql_orders (
        TYPE mysql,
        HOST 'mysql.example.com',
        DATABASE 'orders_db',
        USER 'readonly',
        PASSWORD 'password',
        SSL_MODE 'REQUIRED'
    )
""")

con.execute("""
    CREATE SECRET s3_products (
        TYPE s3,
        PROVIDER credential_chain,
        SCOPE 's3://company-data/products/'
    )
""")

# ATTACH databases
con.execute("""
    ATTACH 'postgres:users_db' AS pg_users (
        TYPE postgres,
        SECRET postgres_users
    )
""")

con.execute("""
    ATTACH 'mysql:orders_db' AS mysql_orders (
        TYPE mysql,
        SECRET mysql_orders
    )
""")

# Federation query
federation_query = """
-- Customer 360 View: Data de 3 fontes diferentes
CREATE OR REPLACE VIEW customer_360 AS
SELECT
    -- PostgreSQL: User data
    u.user_id,
    u.email,
    u.name,
    u.country,
    u.created_at as user_since,

    -- MySQL: Order data
    o.order_stats.total_orders,
    o.order_stats.total_spent,
    o.order_stats.last_order_date,
    o.order_stats.avg_order_value,

    -- S3: Product preferences
    p.product_prefs.favorite_category,
    p.product_prefs.categories_purchased,
    p.product_prefs.product_diversity_score

FROM pg_users.users u

-- Join com MySQL orders
LEFT JOIN (
    SELECT
        customer_id,
        STRUCT_PACK(
            total_orders := COUNT(*),
            total_spent := SUM(amount),
            last_order_date := MAX(order_date),
            avg_order_value := AVG(amount)
        ) as order_stats
    FROM mysql_orders.orders
    GROUP BY customer_id
) o ON u.user_id = o.customer_id

-- Join com S3 products
LEFT JOIN (
    SELECT
        user_id,
        STRUCT_PACK(
            favorite_category := MODE(category),
            categories_purchased := COUNT(DISTINCT category),
            product_diversity_score := COUNT(DISTINCT product_id) / COUNT(*)
        ) as product_prefs
    FROM read_parquet('s3://company-data/products/user_products.parquet')
    GROUP BY user_id
) p ON u.user_id = p.user_id

WHERE u.status = 'active';

-- Analytics query
SELECT
    country,
    COUNT(*) as customers,
    AVG(order_stats.total_orders) as avg_orders_per_customer,
    AVG(order_stats.total_spent) as avg_lifetime_value,
    AVG(product_prefs.product_diversity_score) as avg_diversity
FROM customer_360
GROUP BY country
ORDER BY customers DESC;
"""

print("\nFederation Query:")
print(federation_query)

print("""
Performance Optimization:
─────────────────────────
✓ Push-down predicates to source databases
✓ Filter early (WHERE clauses)
✓ Aggregate at source when possible
✓ Use covering indexes on source databases
✓ Materialize frequently-used joins
✓ Partition S3 data appropriately

Monitoring:
───────────
- Query execution time
- Data volume transferred
- Source database load
- Cache hit rate
- Cost per query
""")

con.close()

# Exemplo/Bloco 6
import duckdb
from datetime import datetime

print("""
CDC Pattern com DuckDB:
════════════════════════════════════════════════════════

Tracking changes across multiple databases
""")

cdc_pattern = """
-- CDC tracking table
CREATE TABLE IF NOT EXISTS cdc_watermarks (
    source_database VARCHAR,
    source_table VARCHAR,
    last_sync_timestamp TIMESTAMP,
    last_sync_id BIGINT,
    records_synced BIGINT
);

-- CDC extraction from PostgreSQL
CREATE OR REPLACE TEMP TABLE pg_changes AS
SELECT
    'postgres' as source_db,
    'users' as source_table,
    *
FROM pg_users.users
WHERE updated_at > (
    SELECT COALESCE(MAX(last_sync_timestamp), '1970-01-01'::TIMESTAMP)
    FROM cdc_watermarks
    WHERE source_database = 'postgres'
        AND source_table = 'users'
);

-- CDC extraction from MySQL
CREATE OR REPLACE TEMP TABLE mysql_changes AS
SELECT
    'mysql' as source_db,
    'orders' as source_table,
    *
FROM mysql_orders.orders
WHERE updated_at > (
    SELECT COALESCE(MAX(last_sync_timestamp), '1970-01-01'::TIMESTAMP)
    FROM cdc_watermarks
    WHERE source_database = 'mysql'
        AND source_table = 'orders'
);

-- Merge changes to data lake
COPY pg_changes
TO 's3://datalake/cdc/users/incremental.parquet'
(FORMAT PARQUET, APPEND true);

COPY mysql_changes
TO 's3://datalake/cdc/orders/incremental.parquet'
(FORMAT PARQUET, APPEND true);

-- Update watermarks
INSERT INTO cdc_watermarks
SELECT
    'postgres' as source_database,
    'users' as source_table,
    MAX(updated_at) as last_sync_timestamp,
    MAX(user_id) as last_sync_id,
    COUNT(*) as records_synced
FROM pg_changes
UNION ALL
SELECT
    'mysql' as source_database,
    'orders' as source_table,
    MAX(updated_at) as last_sync_timestamp,
    MAX(order_id) as last_sync_id,
    COUNT(*) as records_synced
FROM mysql_changes;

-- Compaction: Merge incremental → full
CREATE OR REPLACE TABLE users_full AS
SELECT DISTINCT ON (user_id)
    *
FROM (
    SELECT * FROM read_parquet('s3://datalake/cdc/users/*.parquet')
)
ORDER BY user_id, updated_at DESC;

-- Export compacted
COPY users_full
TO 's3://datalake/full/users/snapshot.parquet'
(FORMAT PARQUET, COMPRESSION 'zstd');
"""

print(cdc_pattern)

print("""
CDC Best Practices:
───────────────────
✓ Use updated_at timestamps
✓ Implement watermark tracking
✓ Handle deletes (soft delete ou tombstones)
✓ Periodic full snapshots
✓ Compaction strategy
✓ Idempotent processing
✓ Monitoring e alerting

Scheduling:
───────────
High-frequency: Every 5 minutes
Standard: Hourly
Low-frequency: Daily
Compaction: Weekly
""")

# Exemplo/Bloco 7
import duckdb

print("""
Performance Optimization Strategies:
════════════════════════════════════════════════════════
""")

# Optimization examples
optimizations = """
1. Predicate Pushdown
   ──────────────────
   ❌ Slow:
   SELECT * FROM 's3://bucket/data/*.parquet'
   WHERE date >= '2024-01-01';

   ✓ Fast (Hive Partitioning):
   SELECT * FROM 's3://bucket/data/year=2024/month=01/*.parquet';

2. Projection Pushdown
   ────────────────────
   ❌ Slow:
   SELECT user_id, amount
   FROM 's3://bucket/wide_table.parquet';  -- Reads all columns

   ✓ Fast:
   SELECT user_id, amount
   FROM read_parquet('s3://bucket/wide_table.parquet',
                     columns=['user_id', 'amount']);

3. Parallel Reads
   ───────────────
   ✓ Fast:
   SELECT * FROM 's3://bucket/data/*.parquet';  -- Reads in parallel

   Configure threads:
   SET threads = 8;

4. Compression
   ────────────
   ✓ Best for network I/O:
   - zstd: Best compression ratio
   - snappy: Fast compression/decompression
   - gzip: Good balance

   COPY (...) TO 's3://bucket/data.parquet'
   (FORMAT PARQUET, COMPRESSION 'zstd');

5. File Sizing
   ────────────
   ✓ Optimal: 256MB - 1GB per file
   ❌ Too small: < 10MB (overhead)
   ❌ Too large: > 5GB (memory pressure)

6. Statistics
   ───────────
   ✓ Parquet row group statistics enable skipping

7. Caching
   ────────
   -- Cache hot data
   CREATE TEMP TABLE hot_data AS
   SELECT * FROM 's3://bucket/frequently_accessed.parquet';

8. Batch Operations
   ─────────────────
   ❌ Slow:
   FOR user IN users:
       SELECT * FROM orders WHERE user_id = user;

   ✓ Fast:
   SELECT * FROM orders WHERE user_id IN (SELECT user_id FROM users);
"""

print(optimizations)

# Benchmark framework
benchmark = """
-- Benchmark framework
CREATE TABLE query_benchmarks (
    query_id VARCHAR,
    query_text VARCHAR,
    execution_time_ms BIGINT,
    rows_processed BIGINT,
    timestamp TIMESTAMP
);

-- Measure query
CREATE OR REPLACE MACRO benchmark(query_id VARCHAR, query_text VARCHAR) AS (
    -- Execute and measure
    -- (DuckDB auto-timing in EXPLAIN ANALYZE)
    EXPLAIN ANALYZE query_text
);

-- Example
SELECT benchmark(
    'query_1',
    'SELECT * FROM s3://bucket/data.parquet WHERE date >= 2024-01-01'
);
"""

print("\nBenchmarking:")
print(benchmark)

con = duckdb.connect()
print("""
Monitoring Metrics:
───────────────────
✓ Query execution time
✓ Data scanned (GB)
✓ Network throughput
✓ Memory usage
✓ CPU utilization
✓ Cache hit rate
✓ Cost per query
""")
con.close()

# Exemplo/Bloco 8
print("""
Troubleshooting Guide:
════════════════════════════════════════════════════════

1. Authentication Errors
   ─────────────────────
   Error: "Access Denied" ou "Authentication failed"

   Checklist:
   ☐ Verify credentials are correct
   ☐ Check secret is created: SELECT * FROM duckdb_secrets()
   ☐ Verify SCOPE matches URL
   ☐ Use which_secret() to debug
   ☐ Check IAM permissions (AWS)
   ☐ Verify service account (GCP)
   ☐ Check Azure RBAC roles

   Debug:
   SELECT * FROM which_secret('s3://bucket/file.parquet', 's3');

2. SSL/TLS Errors
   ───────────────
   Error: "SSL verification failed"

   Solutions:
   ☐ Verify USE_SSL = true for production
   ☐ Check SSLMODE setting (postgres)
   ☐ Verify certificate paths
   ☐ Check certificate expiration
   ☐ Validate hostname matches certificate

   Debug:
   -- Test sem SSL primeiro (apenas dev!)
   CREATE SECRET test (
       TYPE s3,
       KEY_ID 'key',
       SECRET 'secret',
       USE_SSL false
   );

3. Extension Not Loaded
   ─────────────────────
   Error: "Extension not loaded"

   Solutions:
   ☐ INSTALL extension
   ☐ LOAD extension
   ☐ Check extension name (httpfs vs http_fs)
   ☐ Verify installation succeeded

   Debug:
   SELECT * FROM duckdb_extensions();

4. Timeout Errors
   ───────────────
   Error: "Connection timeout" ou "Read timeout"

   Solutions:
   ☐ Increase TIMEOUT parameter
   ☐ Check network connectivity
   ☐ Verify firewall rules
   ☐ Check server load
   ☐ Reduce query complexity

   Debug:
   CREATE SECRET s3_longer_timeout (
       TYPE s3,
       KEY_ID 'key',
       SECRET 'secret',
       TIMEOUT 120000  -- 2 minutos
   );

5. File Not Found
   ───────────────
   Error: "File not found" ou "No such key"

   Checklist:
   ☐ Verify file path is correct
   ☐ Check bucket/container name
   ☐ Verify permissions (read access)
   ☐ Check file actually exists
   ☐ Case sensitivity (S3 is case-sensitive)

   Debug:
   -- List files
   SELECT * FROM read_parquet('s3://bucket/path/*.parquet');

6. Memory Errors
   ──────────────
   Error: "Out of memory"

   Solutions:
   ☐ Reduce data volume (add filters)
   ☐ Use streaming: read_parquet_auto()
   ☐ Process in batches
   ☐ Increase available memory
   ☐ Use projection pushdown

   Debug:
   SET memory_limit = '8GB';
   SET temp_directory = '/path/to/large/disk';

7. Credential Chain Failures
   ──────────────────────────
   Error: "No credentials found in chain"

   Checklist:
   ☐ Verify environment variables set
   ☐ Check ~/.aws/credentials (AWS)
   ☐ Verify gcloud auth (GCP)
   ☐ Check Azure CLI auth
   ☐ Verify IAM role attached (EC2)

   Debug:
   -- Test each chain method individually
   CREATE SECRET s3_env_only (
       TYPE s3,
       PROVIDER credential_chain,
       CHAIN 'env'
   );

8. Persistent Secret Not Loading
   ──────────────────────────────
   Error: "Secret not found after restart"

   Checklist:
   ☐ Verify used CREATE PERSISTENT SECRET
   ☐ Check secret_directory setting
   ☐ Verify file permissions
   ☐ Check disk space
   ☐ Verify database path

   Debug:
   SELECT name, persistent, storage
   FROM duckdb_secrets();

9. Slow Queries
   ─────────────
   Issue: Query takes too long

   Solutions:
   ☐ Use EXPLAIN ANALYZE
   ☐ Check partitioning
   ☐ Verify predicate pushdown
   ☐ Add WHERE filters early
   ☐ Use covering projections
   ☐ Check file sizes
   ☐ Verify compression

   Debug:
   EXPLAIN ANALYZE
   SELECT * FROM 's3://bucket/data.parquet'
   WHERE date >= '2024-01-01';

10. Cross-Database Join Issues
    ───────────────────────────
    Issue: Joins between databases slow/failing

    Solutions:
    ☐ Materialize smaller table locally
    ☐ Use temp tables for intermediate results
    ☐ Optimize join order
    ☐ Add indexes on source databases
    ☐ Consider denormalization

    Debug:
    CREATE TEMP TABLE small_table AS
    SELECT * FROM mysql_db.small_dimension;

    SELECT *
    FROM large_s3_fact f
    JOIN small_table d ON f.id = d.id;
""")

# Exemplo/Bloco 9
import duckdb

print("""
Diagnostic Queries:
════════════════════════════════════════════════════════
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

diagnostics = """
-- 1. List all secrets
SELECT
    name,
    type,
    provider,
    scope,
    persistent,
    storage
FROM duckdb_secrets()
ORDER BY type, name;

-- 2. Check which secret will be used
SELECT *
FROM which_secret('s3://my-bucket/file.parquet', 's3');

-- 3. List installed extensions
SELECT
    extension_name,
    installed,
    loaded
FROM duckdb_extensions()
WHERE extension_name IN ('httpfs', 'azure', 'mysql_scanner', 'postgres_scanner');

-- 4. Check DuckDB settings
SELECT *
FROM duckdb_settings()
WHERE name IN (
    'secret_directory',
    'threads',
    'memory_limit',
    'temp_directory'
);

-- 5. View active connections (attached databases)
SELECT *
FROM duckdb_databases();

-- 6. Memory usage
SELECT *
FROM pragma_database_size();

-- 7. Table information
SELECT *
FROM information_schema.tables;
"""

print(diagnostics)

print("""
Logging e Monitoring:
─────────────────────

Python logging:
""")

logging_example = """
import logging
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='duckdb_secrets.log'
)

logger = logging.getLogger('duckdb_secrets')

try:
    con = duckdb.connect()
    logger.info("Connection established")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    logger.info("httpfs extension loaded")

    con.execute(\"\"\"
        CREATE SECRET s3_prod (
            TYPE s3,
            KEY_ID 'key',
            SECRET 'secret'
        )
    \"\"\")
    logger.info("Secret 's3_prod' created")

    result = con.execute("SELECT * FROM 's3://bucket/file.parquet'").df()
    logger.info(f"Query completed: {len(result)} rows")

except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)
finally:
    con.close()
    logger.info("Connection closed")
"""

print(logging_example)

con.close()

# Exemplo/Bloco 10
print("""
Docker Deployment:
════════════════════════════════════════════════════════
""")

dockerfile = """
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install DuckDB
RUN pip install --no-cache-dir duckdb

# Install cloud SDKs
RUN pip install boto3 azure-identity azure-storage-blob google-cloud-storage

# Copy application
COPY app/ /app/

# Environment variables (overridden at runtime)
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AZURE_TENANT_ID=""
ENV AZURE_CLIENT_ID=""
ENV AZURE_CLIENT_SECRET=""

# Run
CMD ["python", "main.py"]
"""

docker_compose = """
# docker-compose.yml
version: '3.8'

services:
  duckdb-app:
    build: .
    environment:
      # Load from .env file
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - GCP_PROJECT_ID=${GCP_PROJECT_ID}
      - AZURE_TENANT_ID=${AZURE_TENANT_ID}
      - AZURE_CLIENT_ID=${AZURE_CLIENT_ID}
      - AZURE_CLIENT_SECRET=${AZURE_CLIENT_SECRET}
    volumes:
      # Persistent secrets
      - ./secrets:/app/secrets:ro
      # DuckDB data
      - ./data:/app/data
    restart: unless-stopped
"""

print("Dockerfile:")
print(dockerfile)

print("\ndocker-compose.yml:")
print(docker_compose)

kubernetes = """
# Kubernetes Deployment
apiVersion: v1
kind: Secret
metadata:
  name: duckdb-secrets
type: Opaque
stringData:
  aws-access-key-id: "AKIAIOSFODNN7EXAMPLE"
  aws-secret-access-key: "wJalrXUtnFEMI..."

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: duckdb-app
spec:
  replicas: 3
  selector:
    matchLabels:
      app: duckdb-app
  template:
    metadata:
      labels:
        app: duckdb-app
    spec:
      containers:
      - name: duckdb-app
        image: mycompany/duckdb-app:latest
        env:
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: duckdb-secrets
              key: aws-access-key-id
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: duckdb-secrets
              key: aws-secret-access-key
        resources:
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: data
          mountPath: /app/data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: duckdb-data-pvc
"""

print("\nKubernetes:")
print(kubernetes)

print("""
Best Practices:
───────────────
✓ Use secrets managers (não env vars em prod)
✓ Read-only volume mounts para secrets
✓ Health checks
✓ Resource limits
✓ Logging to stdout/stderr
✓ Graceful shutdown
✓ Horizontal scaling considerations
""")

# Exemplo/Bloco 11
print("""
════════════════════════════════════════════════════════
            CURSO COMPLETO - RESUMO FINAL
════════════════════════════════════════════════════════

Capítulo 1: Introdução
───────────────────────
✓ O que são secrets
✓ CREATE/DROP SECRET
✓ duckdb_secrets()
✓ Temporary vs Persistent

Capítulo 2: Tipos de Secrets
─────────────────────────────
✓ S3, R2, GCS, Azure
✓ MySQL, PostgreSQL
✓ HTTP, Hugging Face

Capítulo 3: Cloud Storage
──────────────────────────
✓ S3 completo (URL styles, regions)
✓ Azure (connection string, managed identity)
✓ GCS (service accounts, ADC)
✓ R2 (Cloudflare)

Capítulo 4: Database Secrets
─────────────────────────────
✓ MySQL (SSL, connection strings)
✓ PostgreSQL (ATTACH, SSL)
✓ Cross-database queries
✓ ETL patterns

Capítulo 5: Persistent Secrets
───────────────────────────────
✓ Temporary vs Persistent
✓ secret_directory
✓ Backup e restore
✓ File permissions

Capítulo 6: Providers
─────────────────────
✓ config, credential_chain
✓ managed_identity
✓ service_principal
✓ Por ambiente

Capítulo 7: SCOPE e Named Secrets
──────────────────────────────────
✓ SCOPE hierarchy
✓ which_secret()
✓ Múltiplas credenciais
✓ Naming conventions

Capítulo 8: Extensions
──────────────────────
✓ httpfs (S3, GCS, HTTP)
✓ azure (Blob Storage)
✓ mysql_scanner, postgres_scanner
✓ Extension management

Capítulo 9: Segurança
─────────────────────
✓ Evitar exposição de credenciais
✓ Rotação de secrets
✓ SSL/TLS configuration
✓ Auditoria e logging
✓ Compliance

Capítulo 10: Casos de Uso Avançados
────────────────────────────────────
✓ ETL multi-cloud
✓ Medallion architecture
✓ Database federation
✓ CDC patterns
✓ Performance optimization
✓ Troubleshooting
✓ Production deployment

Próximos Passos:
────────────────
1. Implementar secrets em seus projetos
2. Configurar CI/CD com secrets
3. Estabelecer rotação policy
4. Setup monitoring e alerting
5. Documentar sua arquitetura
6. Treinar seu time
7. Regular security reviews

Recursos Adicionais:
────────────────────
- DuckDB Documentation: https://duckdb.org/docs
- GitHub Issues: https://github.com/duckdb/duckdb
- Discord Community: https://discord.duckdb.org
- Stack Overflow: tag [duckdb]

Lembre-se:
──────────
"Security is not a product, but a process" - Bruce Schneier

✓ Secrets são poderosos, mas requerem cuidado
✓ Sempre seguir security best practices
✓ Documentar decisões e configurações
✓ Automatizar quando possível
✓ Monitorar continuamente
✓ Estar preparado para incidentes

Boa sorte com seus projetos DuckDB! 🦆
════════════════════════════════════════════════════════
""")

# Exemplo/Bloco 12
"""
Projeto Final: Multi-Cloud Data Platform
═══════════════════════════════════════════════════════

Objetivo:
─────────
Implementar plataforma completa de dados usando DuckDB Secrets

Requisitos:
───────────
1. Multi-Cloud Setup
   - AWS S3 (raw data)
   - GCS (processed data)
   - Azure (analytics)

2. Database Integration
   - PostgreSQL (users)
   - MySQL (transactions)

3. Security
   - Credential chain
   - SSL/TLS everywhere
   - Rotação automática
   - Audit logging

4. Data Pipeline
   - Incremental ETL
   - Data quality checks
   - Partitioning strategy

5. Monitoring
   - Query performance
   - Secret usage
   - Error tracking
   - Cost monitoring

6. Documentation
   - Architecture diagram
   - Secret inventory
   - Runbooks
   - Troubleshooting guide

Entregáveis:
────────────
☐ Código Python completo
☐ Configuração de secrets
☐ CI/CD pipeline
☐ Docker/Kubernetes deployment
☐ Monitoring dashboards
☐ Documentação completa
☐ Testes automatizados

Tempo Estimado: 2-3 dias

Boa sorte! 🚀
"""

