# -*- coding: utf-8 -*-
"""
capitulo_08_secrets_extensions
"""

# capitulo_08_secrets_extensions
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

print("""
Extensions que Suportam Secrets:
════════════════════════════════════════════════════════

Extension          Secret Types        Descrição
────────────────────────────────────────────────────────
httpfs             s3, r2, gcs, http   Cloud storage, HTTP APIs
azure              azure               Azure Blob Storage
mysql_scanner      mysql               MySQL database access
postgres_scanner   postgres            PostgreSQL database access
iceberg            iceberg             Apache Iceberg catalogs

Fluxo de Trabalho:
──────────────────

1. INSTALL extension
2. LOAD extension
3. CREATE SECRET
4. Use recursos que dependem do extension + secret

Nota: Secrets só funcionam se extension estiver loaded!
""")

con = duckdb.connect()

# Verificar extensions instaladas
try:
    extensions = con.execute("""
        SELECT extension_name, installed, loaded
        FROM duckdb_extensions()
        WHERE extension_name IN ('httpfs', 'azure', 'mysql_scanner', 'postgres_scanner')
    """).df()

    print("\nStatus de Extensions:")
    print(extensions)
except:
    print("\nNota: Tabela duckdb_extensions() pode não estar disponível em todas as versões")

con.close()

# Exemplo/Bloco 2
import duckdb

def setup_extension_with_secret(extension_name, secret_config):
    """
    Setup completo: install extension + create secret
    """
    con = duckdb.connect()

    print(f"\n{'='*60}")
    print(f"Setup: {extension_name}")
    print(f"{'='*60}")

    # 1. Install
    print(f"1. Installing {extension_name}...")
    con.execute(f"INSTALL {extension_name}")
    print(f"   ✓ Installed")

    # 2. Load
    print(f"2. Loading {extension_name}...")
    con.execute(f"LOAD {extension_name}")
    print(f"   ✓ Loaded")

    # 3. Create secret
    print(f"3. Creating secret...")
    con.execute(secret_config)
    print(f"   ✓ Secret created")

    # 4. Verify
    secrets = con.execute("SELECT name, type FROM duckdb_secrets()").df()
    print(f"4. Verification:")
    print(f"   Secrets: {len(secrets)}")

    con.close()
    return True

# Exemplo
s3_config = """
    CREATE SECRET test_s3 (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret'
    )
"""

setup_extension_with_secret('httpfs', s3_config)

print("""
Ordem Importa!
──────────────
❌ Errado:
   CREATE SECRET ...  # Erro! Extension não carregada
   INSTALL httpfs
   LOAD httpfs

✓ Certo:
   INSTALL httpfs
   LOAD httpfs
   CREATE SECRET ...  # OK! Extension já está loaded
""")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()

# Setup httpfs
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

print("""
httpfs Extension - S3 Support:
════════════════════════════════════════════════════════

Funcionalidades:
────────────────
✓ Ler/escrever de/para S3
✓ Suporte a S3-compatible (MinIO, Wasabi, R2)
✓ Múltiplos secrets com SCOPE
✓ Credential chain support
✓ Parquet, CSV, JSON diretamente de S3
""")

# Configurar múltiplos secrets
con.execute("""
    CREATE SECRET s3_main (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1',
        SCOPE 's3://main-bucket/'
    )
""")

con.execute("""
    CREATE SECRET s3_analytics (
        TYPE s3,
        KEY_ID 'AKIAANALYTICS',
        SECRET 'analytics_secret',
        REGION 'us-west-2',
        SCOPE 's3://analytics-bucket/'
    )
""")

print("\nExemplos de Queries:")

# Read Parquet
query_read = """
SELECT *
FROM 's3://main-bucket/data/sales/2024/01/data.parquet'
WHERE amount > 1000
LIMIT 10
"""
print(f"\n1. Read Parquet:\n{query_read}")

# Write Parquet
query_write = """
COPY (
    SELECT
        date_trunc('month', order_date) as month,
        SUM(amount) as total
    FROM orders
    GROUP BY 1
) TO 's3://analytics-bucket/reports/monthly_summary.parquet'
(FORMAT PARQUET, COMPRESSION 'snappy')
"""
print(f"\n2. Write Parquet:\n{query_write}")

# Read CSV
query_csv = """
SELECT *
FROM read_csv('s3://main-bucket/data/customers.csv')
"""
print(f"\n3. Read CSV:\n{query_csv}")

# Read multiple files with glob
query_glob = """
SELECT *
FROM read_parquet('s3://main-bucket/data/sales/2024/*/*.parquet')
"""
print(f"\n4. Read with glob:\n{query_glob}")

con.close()

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

print("""
httpfs Extension - GCS Support:
════════════════════════════════════════════════════════
""")

# Method 1: Service Account
con.execute("""
    CREATE SECRET gcs_sa (
        TYPE gcs,
        KEY_ID 'service-account@project.iam.gserviceaccount.com',
        SECRET '/path/to/service-account-key.json',
        SCOPE 'gs://my-gcs-bucket/'
    )
""")

# Method 2: Credential chain
con.execute("""
    CREATE SECRET gcs_adc (
        TYPE gcs,
        PROVIDER credential_chain,
        SCOPE 'gs://another-bucket/'
    )
""")

print("""
GCS Queries:
────────────
""")

queries = [
    """
    -- Read from GCS
    SELECT * FROM 'gs://my-gcs-bucket/data.parquet'
    """,
    """
    -- Write to GCS
    COPY (SELECT * FROM my_table)
    TO 'gs://my-gcs-bucket/export/data.parquet'
    """,
    """
    -- Read multiple files
    SELECT * FROM read_parquet('gs://my-gcs-bucket/year=2024/month=*/*.parquet')
    """
]

for i, query in enumerate(queries, 1):
    print(f"{i}.{query}\n")

con.close()

# Exemplo/Bloco 5
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

print("""
httpfs Extension - HTTP Support:
════════════════════════════════════════════════════════
""")

# Bearer token
con.execute("""
    CREATE SECRET api_bearer (
        TYPE http,
        BEARER_TOKEN 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...',
        SCOPE 'https://api.example.com/'
    )
""")

# Basic auth
con.execute("""
    CREATE SECRET api_basic (
        TYPE http,
        USERNAME 'api_user',
        PASSWORD 'api_password',
        SCOPE 'https://internal-api.company.com/'
    )
""")

# Custom headers
con.execute("""
    CREATE SECRET api_custom (
        TYPE http,
        HEADERS MAP {
            'Authorization': 'Bearer token123',
            'X-API-Key': 'my_api_key',
            'User-Agent': 'DuckDB/1.0'
        },
        SCOPE 'https://custom-api.example.com/'
    )
""")

print("""
HTTP Queries:
─────────────
""")

examples = [
    """
    -- Read JSON from API with bearer token
    SELECT *
    FROM read_json('https://api.example.com/data/users.json')
    """,
    """
    -- Read CSV from authenticated endpoint
    SELECT *
    FROM read_csv('https://internal-api.company.com/reports/daily.csv')
    """,
    """
    -- Read Parquet from URL with custom headers
    SELECT *
    FROM 'https://custom-api.example.com/datasets/large.parquet'
    """
]

for i, example in enumerate(examples, 1):
    print(f"{i}.{example}\n")

con.close()

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs")
con.execute("LOAD httpfs")

print("""
httpfs Extension - Cloudflare R2 Support:
════════════════════════════════════════════════════════
""")

# R2 secret
con.execute("""
    CREATE SECRET r2_storage (
        TYPE r2,
        KEY_ID 'r2_access_key_id',
        SECRET 'r2_secret_access_key',
        ACCOUNT_ID 'cloudflare_account_id',
        SCOPE 'r2://my-r2-bucket/'
    )
""")

print("""
R2 Features:
────────────
✓ S3-compatible API
✓ Zero egress fees
✓ Global distribution
✓ Same query patterns as S3

R2 Queries:
───────────
""")

r2_queries = [
    """
    -- Read from R2
    SELECT * FROM 'r2://my-r2-bucket/data/events.parquet'
    WHERE event_date >= '2024-01-01'
    """,
    """
    -- Write to R2
    COPY (
        SELECT * FROM processed_data
    ) TO 'r2://my-r2-bucket/exports/processed.parquet'
    (FORMAT PARQUET, COMPRESSION 'zstd')
    """,
    """
    -- Read partitioned data
    SELECT *
    FROM read_parquet('r2://my-r2-bucket/events/year=*/month=*/*.parquet')
    WHERE year = 2024 AND month = 1
    """
]

for i, query in enumerate(r2_queries, 1):
    print(f"{i}.{query}\n")

con.close()

# Exemplo/Bloco 7
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure")
con.execute("LOAD azure")

print("""
azure Extension:
════════════════════════════════════════════════════════

Authentication Methods:
───────────────────────
1. Connection String
2. Account Key
3. Service Principal
4. Managed Identity
5. Credential Chain
""")

# Connection string
con.execute("""
    CREATE SECRET azure_conn (
        TYPE azure,
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key;EndpointSuffix=core.windows.net',
        SCOPE 'azure://production-container/'
    )
""")

# Managed identity (for Azure VMs)
con.execute("""
    CREATE SECRET azure_mi (
        TYPE azure,
        PROVIDER managed_identity,
        ACCOUNT_NAME 'myaccount',
        SCOPE 'azure://analytics-container/'
    )
""")

# Service principal
con.execute("""
    CREATE SECRET azure_sp (
        TYPE azure,
        PROVIDER service_principal,
        TENANT_ID '00000000-0000-0000-0000-000000000000',
        CLIENT_ID '11111111-1111-1111-1111-111111111111',
        CLIENT_SECRET 'client_secret',
        ACCOUNT_NAME 'myaccount',
        SCOPE 'azure://backup-container/'
    )
""")

print("""
Azure Queries:
──────────────
""")

azure_queries = [
    """
    -- Read from Azure Blob
    SELECT *
    FROM 'azure://production-container/data/sales.parquet'
    """,
    """
    -- Write to Azure Blob
    COPY (SELECT * FROM analytics)
    TO 'azure://analytics-container/reports/summary.parquet'
    """,
    """
    -- Read from hierarchical namespace (ADLS Gen2)
    SELECT *
    FROM 'azure://adls-container/bronze/events/2024/01/*.parquet'
    """,
    """
    -- Cross-container query
    SELECT
        p.product_id,
        p.name,
        s.quantity
    FROM 'azure://production-container/products.parquet' p
    JOIN 'azure://analytics-container/sales.parquet' s
      ON p.product_id = s.product_id
    """
]

for i, query in enumerate(azure_queries, 1):
    print(f"{i}.{query}\n")

con.close()

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure")
con.execute("LOAD azure")

print("""
Azure Data Lake Storage Gen2:
════════════════════════════════════════════════════════

ADLS Gen2 = Azure Blob Storage + Hierarchical Namespace

Features:
─────────
✓ Hierarchical file system
✓ POSIX permissions
✓ Better performance for analytics
✓ Direct compatibility with Hadoop/Spark
""")

# ADLS Gen2 secret
con.execute("""
    CREATE SECRET adls_gen2 (
        TYPE azure,
        PROVIDER service_principal,
        TENANT_ID 'tenant-id',
        CLIENT_ID 'client-id',
        CLIENT_SECRET 'client-secret',
        ACCOUNT_NAME 'adlsgen2account',
        SCOPE 'azure://datalake/'
    )
""")

print("""
ADLS Gen2 Patterns:
───────────────────

Medallion Architecture:
""")

medallion = [
    """
    -- Bronze Layer (raw data)
    SELECT *
    FROM 'azure://datalake/bronze/source_system/YYYY/MM/DD/*.parquet'
    """,
    """
    -- Silver Layer (cleaned, validated)
    CREATE TABLE silver.customers AS
    SELECT
        customer_id,
        clean_name(name) as name,
        validated_email(email) as email
    FROM 'azure://datalake/silver/customers/*.parquet'
    """,
    """
    -- Gold Layer (business aggregates)
    CREATE TABLE gold.customer_metrics AS
    SELECT
        date_trunc('month', order_date) as month,
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_spent
    FROM 'azure://datalake/gold/orders/*.parquet'
    GROUP BY 1, 2
    """
]

for i, pattern in enumerate(medallion, 1):
    print(f"{i}.{pattern}\n")

con.close()

# Exemplo/Bloco 9
import duckdb

con = duckdb.connect()
con.execute("INSTALL mysql_scanner")
con.execute("LOAD mysql_scanner")

print("""
mysql_scanner Extension:
════════════════════════════════════════════════════════

Features:
─────────
✓ ATTACH MySQL databases
✓ Query MySQL tables directly
✓ Push-down predicates
✓ Type mapping
✓ SSL support
""")

# MySQL secret
con.execute("""
    CREATE SECRET mysql_prod (
        TYPE mysql,
        HOST 'mysql.example.com',
        PORT 3306,
        DATABASE 'production',
        USER 'readonly_user',
        PASSWORD 'secure_password',
        SSL_MODE 'REQUIRED'
    )
""")

# ATTACH database
con.execute("""
    ATTACH 'mysql:production' AS mysql_prod (
        TYPE mysql,
        SECRET mysql_prod
    )
""")

print("""
MySQL Queries:
──────────────
""")

mysql_queries = [
    """
    -- Direct query on MySQL table
    SELECT *
    FROM mysql_prod.orders
    WHERE order_date >= '2024-01-01'
    LIMIT 100
    """,
    """
    -- JOIN MySQL with local data
    SELECT
        m.customer_id,
        m.customer_name,
        l.score
    FROM mysql_prod.customers m
    JOIN local_customer_scores l
      ON m.customer_id = l.customer_id
    """,
    """
    -- Aggregate from MySQL
    SELECT
        DATE_TRUNC('month', order_date) as month,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM mysql_prod.orders
    GROUP BY 1
    ORDER BY 1 DESC
    """,
    """
    -- Export MySQL to Parquet
    COPY (
        SELECT * FROM mysql_prod.large_table
    ) TO 'export/mysql_data.parquet'
    (FORMAT PARQUET, COMPRESSION 'snappy')
    """
]

for i, query in enumerate(mysql_queries, 1):
    print(f"{i}.{query}\n")

con.close()

# Exemplo/Bloco 10
import duckdb

print("""
MySQL ETL Patterns com DuckDB:
════════════════════════════════════════════════════════

Pattern 1: MySQL → DuckDB → Parquet
────────────────────────────────────
""")

etl_pattern_1 = """
-- 1. Setup
INSTALL mysql_scanner;
LOAD mysql_scanner;

CREATE SECRET mysql_source (
    TYPE mysql,
    HOST 'mysql.example.com',
    DATABASE 'source_db',
    USER 'etl_user',
    PASSWORD 'etl_password'
);

ATTACH 'mysql:source_db' AS mysql_src (
    TYPE mysql,
    SECRET mysql_source
);

-- 2. ETL: Incremental load
COPY (
    SELECT *
    FROM mysql_src.transactions
    WHERE updated_at >= (
        SELECT MAX(updated_at)
        FROM 'data/transactions/*.parquet'
    )
) TO 'data/transactions/incremental.parquet'
(FORMAT PARQUET, PARTITION_BY (year, month));
"""

print(etl_pattern_1)

print("""
Pattern 2: Multiple MySQL → Unified View
─────────────────────────────────────────
""")

etl_pattern_2 = """
-- Attach multiple MySQL databases
ATTACH 'mysql:db1' AS mysql_db1 (TYPE mysql, SECRET mysql_secret1);
ATTACH 'mysql:db2' AS mysql_db2 (TYPE mysql, SECRET mysql_secret2);
ATTACH 'mysql:db3' AS mysql_db3 (TYPE mysql, SECRET mysql_secret3);

-- Create unified view
CREATE VIEW unified_customers AS
SELECT 'db1' as source, * FROM mysql_db1.customers
UNION ALL
SELECT 'db2' as source, * FROM mysql_db2.customers
UNION ALL
SELECT 'db3' as source, * FROM mysql_db3.customers;

-- Query across all databases
SELECT
    source,
    COUNT(*) as customer_count,
    SUM(total_orders) as total_orders
FROM unified_customers
GROUP BY source;
"""

print(etl_pattern_2)

con = duckdb.connect()
print("\n✓ mysql_scanner patterns documented")
con.close()

# Exemplo/Bloco 11
import duckdb

con = duckdb.connect()
con.execute("INSTALL postgres_scanner")
con.execute("LOAD postgres_scanner")

print("""
postgres_scanner Extension:
════════════════════════════════════════════════════════

Features:
─────────
✓ ATTACH PostgreSQL databases
✓ Query PostgreSQL tables directly
✓ Push-down predicates and aggregations
✓ Type mapping (including JSONB, arrays)
✓ SSL/TLS support
✓ Connection pooling
""")

# PostgreSQL secrets for different environments
con.execute("""
    CREATE SECRET postgres_prod (
        TYPE postgres,
        HOST 'postgres.example.com',
        PORT 5432,
        DATABASE 'production',
        USER 'readonly_user',
        PASSWORD 'secure_password',
        SSLMODE 'verify-full',
        SSLROOTCERT '/path/to/root.crt'
    )
""")

con.execute("""
    CREATE SECRET postgres_analytics (
        TYPE postgres,
        CONNECTION_STRING 'postgresql://analyst:pass@analytics.example.com:5432/analytics?sslmode=require'
    )
""")

# ATTACH databases
con.execute("""
    ATTACH 'postgres:production' AS pg_prod (
        TYPE postgres,
        SECRET postgres_prod
    )
""")

con.execute("""
    ATTACH 'postgres:analytics' AS pg_analytics (
        TYPE postgres,
        SECRET postgres_analytics
    )
""")

print("""
PostgreSQL Queries:
───────────────────
""")

postgres_queries = [
    """
    -- Query PostgreSQL table
    SELECT *
    FROM pg_prod.orders
    WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'
    """,
    """
    -- JOIN PostgreSQL with DuckDB
    SELECT
        p.product_id,
        p.name,
        s.stock_level,
        s.warehouse_location
    FROM pg_prod.products p
    JOIN local_stock_data s
      ON p.product_id = s.product_id
    WHERE s.stock_level < 10
    """,
    """
    -- Aggregate from PostgreSQL
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as lifetime_value,
        MAX(order_date) as last_order
    FROM pg_prod.orders
    GROUP BY customer_id
    HAVING COUNT(*) > 5
    """,
    """
    -- Cross-database analytics
    SELECT
        'Production' as db,
        COUNT(*) as record_count
    FROM pg_prod.transactions
    UNION ALL
    SELECT
        'Analytics' as db,
        COUNT(*) as record_count
    FROM pg_analytics.processed_transactions
    """
]

for i, query in enumerate(postgres_queries, 1):
    print(f"{i}.{query}\n")

con.close()

# Exemplo/Bloco 12
import duckdb

print("""
PostgreSQL Advanced Patterns:
════════════════════════════════════════════════════════

Pattern 1: Incremental Sync
────────────────────────────
""")

incremental_sync = """
-- Track last sync
CREATE TABLE IF NOT EXISTS sync_metadata (
    table_name VARCHAR,
    last_sync_timestamp TIMESTAMP
);

-- Incremental load from PostgreSQL
CREATE TABLE updated_records AS
SELECT *
FROM pg_prod.events
WHERE updated_at > (
    SELECT COALESCE(MAX(last_sync_timestamp), '1970-01-01'::TIMESTAMP)
    FROM sync_metadata
    WHERE table_name = 'events'
);

-- Update sync metadata
INSERT INTO sync_metadata
VALUES ('events', CURRENT_TIMESTAMP);

-- Export to Parquet
COPY updated_records
TO 'data/events/incremental.parquet'
(FORMAT PARQUET);
"""

print(incremental_sync)

print("""
Pattern 2: PostgreSQL → DuckDB Analytics
─────────────────────────────────────────
""")

analytics_pattern = """
-- Create analytics view in DuckDB
CREATE VIEW customer_360 AS
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.segment,
    o.order_count,
    o.total_spent,
    o.last_order_date,
    s.support_tickets,
    s.satisfaction_score
FROM pg_prod.customers c
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_spent,
        MAX(order_date) as last_order_date
    FROM pg_prod.orders
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*) as support_tickets,
        AVG(satisfaction) as satisfaction_score
    FROM pg_prod.support_interactions
    GROUP BY customer_id
) s ON c.customer_id = s.customer_id;

-- Fast analytics on denormalized view
SELECT
    segment,
    COUNT(*) as customers,
    AVG(total_spent) as avg_lifetime_value,
    AVG(satisfaction_score) as avg_satisfaction
FROM customer_360
WHERE order_count > 0
GROUP BY segment
ORDER BY avg_lifetime_value DESC;
"""

print(analytics_pattern)

print("""
Pattern 3: Cross-Database ETL
──────────────────────────────
""")

cross_db_etl = """
-- Read from MySQL
ATTACH 'mysql:orders_db' AS mysql_orders (TYPE mysql, SECRET mysql_secret);

-- Read from PostgreSQL
ATTACH 'postgres:customers_db' AS pg_customers (TYPE postgres, SECRET pg_secret);

-- Join and export
COPY (
    SELECT
        o.order_id,
        o.order_date,
        o.amount,
        c.customer_name,
        c.customer_segment,
        c.customer_region
    FROM mysql_orders.orders o
    JOIN pg_customers.customers c
      ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2024-01-01'
) TO 'analytics/enriched_orders.parquet'
(FORMAT PARQUET, COMPRESSION 'zstd');
"""

print(cross_db_etl)

con = duckdb.connect()
print("\n✓ PostgreSQL patterns documented")
con.close()

# Exemplo/Bloco 13
import duckdb

print("""
Extension Auto-loading:
════════════════════════════════════════════════════════

DuckDB pode instalar e carregar extensions automaticamente
quando necessário.

Configuration:
──────────────
""")

con = duckdb.connect()

# Habilitar auto-install
con.execute("SET autoinstall_known_extensions = true")
con.execute("SET autoload_known_extensions = true")

print("""
Configurações:
──────────────
autoinstall_known_extensions = true
  → DuckDB instala extensions automaticamente quando necessário

autoload_known_extensions = true
  → DuckDB carrega extensions automaticamente

Exemplo:
────────
""")

example = """
-- Com auto-loading habilitado
SET autoinstall_known_extensions = true;
SET autoload_known_extensions = true;

-- Não precisa de INSTALL/LOAD explícito!
CREATE SECRET my_s3 (
    TYPE s3,
    KEY_ID 'key',
    SECRET 'secret'
);

-- httpfs será instalado e loaded automaticamente
SELECT * FROM 's3://bucket/file.parquet';
"""

print(example)

print("""
Vantagens:
──────────
✓ Menos boilerplate
✓ Código mais limpo
✓ Fácil para iniciantes

Desvantagens:
─────────────
✗ Menos controle
✗ Pode instalar extensions inesperadas
✗ Overhead de network em primeira uso

Recomendação:
─────────────
Development: auto-loading OK
Production: INSTALL/LOAD explícito
""")

con.close()

# Exemplo/Bloco 14
# 1. Instale e carregue httpfs e azure extensions
# 2. Crie secrets para S3, GCS, Azure
# 3. Teste leitura de arquivos (se tiver acesso)
# 4. Documente qual extension é necessária para cada tipo
# 5. Teste which_secret() para cada provider

# Sua solução aqui

# Exemplo/Bloco 15
# 1. Instale mysql_scanner e postgres_scanner
# 2. Crie secrets para ambos
# 3. Use ATTACH para conectar (se tiver databases)
# 4. Crie query que une dados de ambos
# 5. Export resultado para Parquet

# Sua solução aqui

# Exemplo/Bloco 16
# 1. Crie função que setup_complete():
#    - Verifica se extension está instalada
#    - Instala se necessário
#    - Load extension
#    - Cria secret
#    - Valida configuração
# 2. Teste com httpfs
# 3. Teste com azure
# 4. Adicione error handling

# Sua solução aqui

# Exemplo/Bloco 17
# 1. Configure PostgreSQL source com secret
# 2. Configure S3 destination com secret
# 3. Implemente ETL:
#    - Read from PostgreSQL
#    - Transform in DuckDB
#    - Write to S3 Parquet
# 4. Adicione incremental loading
# 5. Adicione error handling e logging

# Sua solução aqui

