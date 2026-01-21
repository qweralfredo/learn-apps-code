# -*- coding: utf-8 -*-
"""
capitulo_03_leitura_tabelas_iceberg
"""

# capitulo_03_leitura_tabelas_iceberg
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# Ler e converter para DataFrame
result = con.execute("""
    SELECT * FROM iceberg_scan('data/iceberg/sales')
    LIMIT 1000
""").df()

print(result.head())

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# Função para ler tabela Iceberg
def read_iceberg(table_path, filter_date=None):
    query = f"SELECT * FROM iceberg_scan('{table_path}')"

    if filter_date:
        query += f" WHERE order_date >= '{filter_date}'"

    return con.execute(query).df()

# Usar
df = read_iceberg('s3://bucket/sales', filter_date='2024-01-01')
print(df.shape)

# Exemplo/Bloco 3
import duckdb
from datetime import datetime, timedelta

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

def read_last_n_days(table_path, n_days=7):
    """Lê últimos N dias de uma tabela Iceberg"""
    cutoff_date = (datetime.now() - timedelta(days=n_days)).strftime('%Y-%m-%d')

    df = con.execute(f"""
        SELECT *
        FROM iceberg_scan('{table_path}')
        WHERE event_timestamp >= '{cutoff_date}'
    """).df()

    return df

# Ler últimos 7 dias
recent_data = read_last_n_days('s3://bucket/events', n_days=7)

# Exemplo/Bloco 4
import duckdb
import pandas as pd

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

# Análise mensal de vendas
monthly_sales = con.execute("""
    SELECT
        date_trunc('month', order_date) as month,
        count(DISTINCT customer_id) as unique_customers,
        count(*) as total_orders,
        sum(total_amount) as revenue,
        avg(total_amount) as avg_order_value
    FROM iceberg_scan('s3://analytics-bucket/sales')
    WHERE order_date >= '2024-01-01'
    GROUP BY month
    ORDER BY month
""").df()

print(monthly_sales)

# Exemplo/Bloco 5
import duckdb

def process_iceberg_table(source_table, output_file):
    """
    Lê tabela Iceberg, processa e salva resultado
    """
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    # Processar
    con.execute(f"""
        COPY (
            SELECT
                customer_id,
                sum(total_amount) as lifetime_value,
                count(*) as order_count,
                max(order_date) as last_order_date
            FROM iceberg_scan('{source_table}')
            GROUP BY customer_id
            HAVING lifetime_value > 1000
        ) TO '{output_file}'
        (FORMAT parquet, COMPRESSION zstd)
    """)

    print(f"Processado e salvo em {output_file}")

# Usar
process_iceberg_table(
    's3://bucket/sales',
    'customer_ltv.parquet'
)

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

# Join de múltiplas tabelas Iceberg
result = con.execute("""
    SELECT
        o.order_id,
        o.order_date,
        c.customer_name,
        c.region,
        p.product_name,
        p.category,
        o.quantity,
        o.total_amount
    FROM iceberg_scan('s3://warehouse/orders') o
    JOIN iceberg_scan('s3://warehouse/customers') c
        ON o.customer_id = c.customer_id
    JOIN iceberg_scan('s3://warehouse/products') p
        ON o.product_id = p.product_id
    WHERE o.order_date >= '2024-01-01'
    LIMIT 1000
""").df()

print(result.head())

# Exemplo/Bloco 7
import duckdb
from datetime import datetime

def monitor_iceberg_table(table_path):
    """
    Monitora estatísticas de uma tabela Iceberg
    """
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    stats = con.execute(f"""
        SELECT
            count(*) as total_rows,
            count(DISTINCT date_trunc('day', event_timestamp)) as days_of_data,
            min(event_timestamp) as earliest_event,
            max(event_timestamp) as latest_event,
            pg_size_pretty(sum(length(event_data::VARCHAR))) as approx_size
        FROM iceberg_scan('{table_path}')
    """).fetchone()

    print(f"""
    Tabela: {table_path}
    Total de linhas: {stats[0]:,}
    Dias de dados: {stats[1]}
    Evento mais antigo: {stats[2]}
    Evento mais recente: {stats[3]}
    Timestamp da análise: {datetime.now()}
    """)

# Usar
monitor_iceberg_table('s3://bucket/events')

# Exemplo/Bloco 8
import duckdb
import os

def safe_iceberg_scan(table_path):
    """Lê tabela Iceberg com tratamento de erros"""
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    try:
        result = con.execute(f"""
            SELECT count(*) FROM iceberg_scan('{table_path}')
        """).fetchone()
        print(f"✅ Tabela encontrada: {result[0]} linhas")
        return True
    except Exception as e:
        print(f"❌ Erro ao ler tabela: {e}")
        return False

safe_iceberg_scan('data/iceberg/sales')

# Exemplo/Bloco 9
import duckdb

import importlib.util


def has_module(name):
    return importlib.util.find_spec(name) is not None

def safe_install_ext(con, ext_name):
    try:
        con.execute(f"INSTALL {ext_name}")
        con.execute(f"LOAD {ext_name}")
        return True
    except Exception as e:
        print(f"Warning: Failed to install/load {ext_name} extension: {e}")
        return False


con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# Tentar diferentes versões
for version in ['1', '2', '3']:
    try:
        result = con.execute(f"""
            SELECT count(*)
            FROM iceberg_scan('data/iceberg/table', version = '{version}')
        """).fetchone()
        print(f"✅ Versão {version}: {result[0]} linhas")
        break
    except Exception as e:
        print(f"Versão {version} não encontrada")
