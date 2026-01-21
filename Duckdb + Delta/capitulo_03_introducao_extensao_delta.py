# -*- coding: utf-8 -*-
"""
capitulo-03-introducao-extensao-delta
"""

# capitulo-03-introducao-extensao-delta
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
from deltalake import write_deltalake

# Criar conexão DuckDB
con = duckdb.connect()

# Criar DataFrame de exemplo
df = con.execute("""
    SELECT
        i as id,
        i % 10 as category,
        i % 2 as partition_col,
        'value-' || i as description,
        CURRENT_DATE - (i % 100) * INTERVAL '1 day' as created_date,
        RANDOM() * 1000 as amount
    FROM range(0, 10000) tbl(i)
""").df()

# Escrever como tabela Delta (particionada)
write_deltalake(
    "./my_delta_table",
    df,
    partition_by=["partition_col"],
    mode="overwrite"
)

print("Delta table created successfully!")

# Ler com DuckDB
result = con.execute("""
    SELECT
        partition_col,
        COUNT(*) as total_rows,
        AVG(amount) as avg_amount
    FROM delta_scan('./my_delta_table')
    GROUP BY partition_col
    ORDER BY partition_col
""").fetchdf()

print(result)

# Exemplo/Bloco 2
from pyspark.sql import SparkSession

# Criar SparkSession com Delta Lake
spark = SparkSession.builder \
    .appName("CreateDeltaTable") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Criar DataFrame
df = spark.range(0, 10000).selectExpr(
    "id",
    "id % 10 as category",
    "id % 2 as partition_col",
    "concat('value-', id) as description"
)

# Escrever como Delta table
df.write \
    .format("delta") \
    .partitionBy("partition_col") \
    .mode("overwrite") \
    .save("./my_delta_table")

print("Delta table created with Spark!")

# Exemplo/Bloco 3
import duckdb
from deltalake import write_deltalake
import pandas as pd
from pathlib import Path

def create_sample_delta_tables():
    """
    Criar tabelas Delta de exemplo para aprendizado
    """
    con = duckdb.connect()

    # 1. Tabela de Clientes
    customers_df = con.execute("""
        SELECT
            i as customer_id,
            'Customer ' || i as customer_name,
            ['US', 'UK', 'BR', 'JP'][i % 4 + 1] as country,
            CURRENT_DATE - (i % 1000) * INTERVAL '1 day' as signup_date
        FROM range(1, 1001) tbl(i)
    """).df()

    write_deltalake("./delta_tables/customers", customers_df, mode="overwrite")

    # 2. Tabela de Produtos
    products_df = con.execute("""
        SELECT
            i as product_id,
            'Product ' || i as product_name,
            ['Electronics', 'Clothing', 'Food', 'Books'][i % 4 + 1] as category,
            10.0 + RANDOM() * 1000 as price
        FROM range(1, 101) tbl(i)
    """).df()

    write_deltalake("./delta_tables/products", products_df, mode="overwrite")

    # 3. Tabela de Vendas (particionada por data)
    sales_df = con.execute("""
        SELECT
            i as order_id,
            1 + (i % 1000) as customer_id,
            1 + (i % 100) as product_id,
            1 + (RANDOM() * 5)::INTEGER as quantity,
            CURRENT_DATE - (i % 365) * INTERVAL '1 day' as order_date,
            RANDOM() * 1000 as amount
        FROM range(1, 50001) tbl(i)
    """).df()

    write_deltalake(
        "./delta_tables/sales",
        sales_df,
        partition_by=["order_date"],
        mode="overwrite"
    )

    print("✓ Sample Delta tables created:")
    print("  - ./delta_tables/customers")
    print("  - ./delta_tables/products")
    print("  - ./delta_tables/sales (partitioned)")

    # Verificar tabelas
    for table in ['customers', 'products', 'sales']:
        count = con.execute(f"""
            SELECT COUNT(*) as total
            FROM delta_scan('./delta_tables/{table}')
        """).fetchone()[0]
        print(f"  - {table}: {count:,} rows")

    con.close()

if __name__ == "__main__":
    create_sample_delta_tables()

# Exemplo/Bloco 4
import json
from pathlib import Path

def explore_delta_log(delta_table_path):
    """
    Explorar transaction log de tabela Delta
    """
    log_path = Path(delta_table_path) / "_delta_log"

    print(f"Transaction log files in {log_path}:")
    for log_file in sorted(log_path.glob("*.json")):
        print(f"\n{log_file.name}:")
        with open(log_file) as f:
            for line in f:
                entry = json.loads(line)
                if 'add' in entry:
                    print(f"  ADD: {entry['add']['path']}")
                elif 'remove' in entry:
                    print(f"  REMOVE: {entry['remove']['path']}")
                elif 'metaData' in entry:
                    print(f"  METADATA: {entry['metaData'].get('name', 'N/A')}")

# Uso
explore_delta_log("./my_delta_table")

