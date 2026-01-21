# -*- coding: utf-8 -*-
"""
capitulo-04-leitura-tabelas-delta
"""

# capitulo-04-leitura-tabelas-delta
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler Delta table para Pandas
df = con.execute("""
    SELECT *
    FROM delta_scan('./sales')
    WHERE order_date >= '2024-01-01'
""").df()

print(df.head())
print(f"Total rows: {len(df)}")

# Análise com Pandas
print(df.describe())
print(df.groupby('region')['total_amount'].sum())

# Exemplo/Bloco 2
import duckdb
import polars as pl

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler Delta table para Polars
df_polars = con.execute("""
    SELECT *
    FROM delta_scan('./sales')
""").pl()

# Análise com Polars
result = (
    df_polars
    .group_by('region')
    .agg([
        pl.col('total_amount').sum().alias('total_revenue'),
        pl.col('order_id').count().alias('order_count')
    ])
    .sort('total_revenue', descending=True)
)

print(result)

# Exemplo/Bloco 3
import duckdb
import pyarrow as pa

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler Delta table para Arrow
arrow_table = con.execute("""
    SELECT * FROM delta_scan('./sales') LIMIT 10000
""").arrow()

print(f"Schema: {arrow_table.schema}")
print(f"Num rows: {arrow_table.num_rows}")

# Exemplo/Bloco 4
import duckdb
import pandas as pd
from datetime import datetime, timedelta

def generate_sales_dashboard(delta_path: str):
    """
    Gerar métricas de dashboard a partir de tabela Delta
    """
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    # Período de análise
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90)

    # 1. Métricas Principais
    metrics = con.execute(f"""
        SELECT
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            AVG(total_amount) as avg_order_value,
            MAX(order_date) as last_order_date
        FROM delta_scan('{delta_path}')
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
    """).df()

    print("=== DASHBOARD DE VENDAS (Últimos 90 dias) ===")
    print(f"Clientes Únicos: {metrics['unique_customers'][0]:,}")
    print(f"Total de Pedidos: {metrics['total_orders'][0]:,}")
    print(f"Receita Total: ${metrics['total_revenue'][0]:,.2f}")
    print(f"Ticket Médio: ${metrics['avg_order_value'][0]:,.2f}")

    # 2. Vendas por Região
    by_region = con.execute(f"""
        SELECT
            region,
            COUNT(*) as orders,
            SUM(total_amount) as revenue,
            ROUND(SUM(total_amount) * 100.0 / SUM(SUM(total_amount)) OVER (), 2) as pct_revenue
        FROM delta_scan('{delta_path}')
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY region
        ORDER BY revenue DESC
    """).df()

    print("\n=== VENDAS POR REGIÃO ===")
    print(by_region.to_string(index=False))

    # 3. Tendência Diária
    daily_trend = con.execute(f"""
        SELECT
            order_date,
            COUNT(*) as orders,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order
        FROM delta_scan('{delta_path}')
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY order_date
        ORDER BY order_date
    """).df()

    print(f"\n=== TENDÊNCIA DIÁRIA (últimos 7 dias) ===")
    print(daily_trend.tail(7).to_string(index=False))

    # 4. Top 10 Clientes
    top_customers = con.execute(f"""
        SELECT
            customer_id,
            COUNT(*) as order_count,
            SUM(total_amount) as lifetime_value,
            MAX(order_date) as last_order
        FROM delta_scan('{delta_path}')
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY customer_id
        ORDER BY lifetime_value DESC
        LIMIT 10
    """).df()

    print("\n=== TOP 10 CLIENTES ===")
    print(top_customers.to_string(index=False))

    con.close()
    return metrics, by_region, daily_trend, top_customers

# Uso
if __name__ == "__main__":
    generate_sales_dashboard('./delta_tables/sales')

# Exemplo/Bloco 5
# Verificar se o diretório existe e contém _delta_log
from pathlib import Path

delta_path = Path('./my_delta_table')
log_path = delta_path / '_delta_log'

print(f"Delta table exists: {delta_path.exists()}")
print(f"Transaction log exists: {log_path.exists()}")

if log_path.exists():
    log_files = list(log_path.glob('*.json'))
    print(f"Log files found: {len(log_files)}")

