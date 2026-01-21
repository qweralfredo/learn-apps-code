# -*- coding: utf-8 -*-
"""
capitulo-09-integracao-python-spark
"""

# capitulo-09-integracao-python-spark
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler tabela Delta completa para Pandas
df = con.execute("""
    SELECT * FROM delta_scan('./sales')
""").df()

print(type(df))  # <class 'pandas.core.frame.DataFrame'>
print(df.head())

# Exemplo/Bloco 2
# Aplicar filtros antes de converter para Pandas
df_filtered = con.execute("""
    SELECT
        customer_id,
        order_date,
        amount
    FROM delta_scan('./sales')
    WHERE order_date >= '2024-01-01'
        AND amount > 100
""").df()

print(f"Rows loaded: {len(df_filtered):,}")

# Exemplo/Bloco 3
import pandas as pd
from deltalake import write_deltalake

# Criar DataFrame Pandas
df = pd.DataFrame({
    'id': range(1000),
    'name': [f'Product {i}' for i in range(1000)],
    'price': pd.Series([10.5 + i * 0.1 for i in range(1000)])
})

# Escrever como tabela Delta
write_deltalake(
    "./products",
    df,
    mode="overwrite"
)

print("[OK] Pandas DataFrame written to Delta table")

# Exemplo/Bloco 4
import duckdb
import pandas as pd
from deltalake import write_deltalake

def etl_pipeline():
    """
    ETL completo: Delta -> Transform -> Delta
    """
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    # Extract: Ler de Delta
    print("1. Extracting from Delta...")
    df = con.execute("""
        SELECT * FROM delta_scan('./raw_data')
    """).df()

    # Transform: Processar com Pandas
    print("2. Transforming with Pandas...")
    df['total_with_tax'] = df['amount'] * 1.1
    df['category'] = df['product_name'].str.split().str[0]
    df_aggregated = df.groupby('category').agg({
        'amount': 'sum',
        'total_with_tax': 'sum'
    }).reset_index()

    # Load: Escrever para Delta
    print("3. Loading to Delta...")
    write_deltalake(
        "./processed_data",
        df_aggregated,
        mode="overwrite"
    )

    print("[OK] ETL pipeline completed")
    return df_aggregated

# Executar
result = etl_pipeline()
print(result)

# Exemplo/Bloco 5
import duckdb
import polars as pl

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler Delta para Polars
df_polars = con.execute("""
    SELECT * FROM delta_scan('./sales')
""").pl()

print(type(df_polars))  # <class 'polars.dataframe.frame.DataFrame'>

# Exemplo/Bloco 6
import duckdb
import polars as pl

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler de Delta
df = con.execute("SELECT * FROM delta_scan('./sales')").pl()

# Processar com Polars (sintaxe lazy)
result = (
    df
    .lazy()
    .filter(pl.col('amount') > 100)
    .with_columns([
        (pl.col('amount') * 1.1).alias('amount_with_tax'),
        pl.col('order_date').dt.year().alias('year')
    ])
    .group_by(['year', 'region'])
    .agg([
        pl.col('amount').sum().alias('total_amount'),
        pl.col('order_id').count().alias('order_count')
    ])
    .sort('year', descending=True)
    .collect()
)

print(result)

# Exemplo/Bloco 7
import polars as pl
from deltalake import write_deltalake

# Criar DataFrame Polars
df = pl.DataFrame({
    'id': range(1000),
    'value': [i * 2 for i in range(1000)]
})

# Converter para Pandas primeiro (deltalake requer Pandas ou Arrow)
df_pandas = df.to_pandas()

# Escrever para Delta
write_deltalake(
    "./polars_output",
    df_pandas,
    mode="overwrite"
)

# Exemplo/Bloco 8
import duckdb
import pyarrow as pa

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler Delta para Arrow Table
arrow_table = con.execute("""
    SELECT * FROM delta_scan('./sales')
""").arrow()

print(type(arrow_table))  # <class 'pyarrow.lib.Table'>
print(f"Schema: {arrow_table.schema}")
print(f"Rows: {arrow_table.num_rows:,}")

# Exemplo/Bloco 9
import duckdb
import pyarrow as pa
import pandas as pd

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Delta -> Arrow
arrow_table = con.execute("SELECT * FROM delta_scan('./sales')").arrow()

# Arrow -> Pandas (zero-copy quando possível)
df_pandas = arrow_table.to_pandas()

# Arrow -> Parquet file
import pyarrow.parquet as pq
pq.write_table(arrow_table, 'output.parquet')

# Parquet file -> DuckDB
result = con.execute("SELECT * FROM 'output.parquet'").df()

# Exemplo/Bloco 10
from deltalake import DeltaTable, write_deltalake
import pandas as pd

# Escrever tabela
df = pd.DataFrame({'id': [1, 2, 3], 'value': ['a', 'b', 'c']})
write_deltalake("./my_table", df, mode="overwrite")

# Ler metadados
dt = DeltaTable("./my_table")
print(f"Version: {dt.version()}")
print(f"Files: {dt.file_uris()}")
print(f"Schema: {dt.schema()}")

# Histórico de versões
print("\nHistory:")
for entry in dt.history():
    print(f"  Version {entry['version']}: {entry['operation']}")

# Exemplo/Bloco 11
from deltalake import write_deltalake
import pandas as pd

# Primeira escrita
df1 = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
write_deltalake("./table", df1, mode="overwrite")

# Append: adiciona linhas
df2 = pd.DataFrame({'id': [4, 5], 'name': ['D', 'E']})
write_deltalake("./table", df2, mode="append")

# Overwrite: substitui tudo
df3 = pd.DataFrame({'id': [10, 20], 'name': ['X', 'Y']})
write_deltalake("./table", df3, mode="overwrite")

# Exemplo/Bloco 12
from deltalake import write_deltalake
import pandas as pd

# Tabela inicial
df1 = pd.DataFrame({'id': [1, 2], 'name': ['A', 'B']})
write_deltalake("./evolving_table", df1, mode="overwrite")

# Adicionar nova coluna (schema evolution)
df2 = pd.DataFrame({
    'id': [3, 4],
    'name': ['C', 'D'],
    'new_column': [100, 200]
})

write_deltalake(
    "./evolving_table",
    df2,
    mode="append",
    schema_mode="merge"  # Permite adicionar colunas
)

# Ler com DuckDB
import duckdb
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
result = con.execute("SELECT * FROM delta_scan('./evolving_table')").df()
print(result)
# Linhas antigas terão NULL na nova coluna

# Exemplo/Bloco 13
from pyspark.sql import SparkSession

# Criar Spark session com Delta Lake
spark = SparkSession.builder \
    .appName("DeltaLakeApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .getOrCreate()

print(f"[OK] Spark {spark.version} with Delta Lake initialized")

# Exemplo/Bloco 14
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Criar DataFrame
df = spark.range(0, 100000).selectExpr(
    "id",
    "id * 2 as value",
    "current_date() as created_date"
)

# Escrever como Delta table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("./spark_delta_table")

print("[OK] Delta table created with Spark")

# Exemplo/Bloco 15
import duckdb

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Ler tabela criada pelo Spark
result = con.execute("""
    SELECT
        COUNT(*) as total_rows,
        AVG(value) as avg_value
    FROM delta_scan('./spark_delta_table')
""").fetchone()

print(f"Total rows: {result[0]:,}")
print(f"Average value: {result[1]:,.2f}")

# Exemplo/Bloco 16
from pyspark.sql import SparkSession
import duckdb

def spark_to_duckdb_pipeline():
    """
    Pipeline: Processar com Spark, analisar com DuckDB
    """

    # 1. Processar com Spark (escala para grandes volumes)
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print("1. Processing with Spark...")

    # Criar dataset grande
    df = spark.range(0, 10_000_000).selectExpr(
        "id",
        "id % 100 as category",
        "rand() * 1000 as amount",
        "current_date() - cast(rand() * 365 as int) as date"
    )

    # Escrever particionado
    df.write \
        .format("delta") \
        .partitionBy("category") \
        .mode("overwrite") \
        .save("./large_spark_table")

    spark.stop()

    # 2. Analisar com DuckDB (queries analíticas rápidas)
    print("2. Analyzing with DuckDB...")

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")

    # Query analítica
    analysis = con.execute("""
        SELECT
            category,
            COUNT(*) as total_records,
            AVG(amount) as avg_amount,
            MAX(amount) as max_amount
        FROM delta_scan('./large_spark_table')
        WHERE date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY category
        ORDER BY avg_amount DESC
        LIMIT 10
    """).df()

    print("\nTop 10 categories by average amount:")
    print(analysis)

# Executar
spark_to_duckdb_pipeline()

# Exemplo/Bloco 17
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Criar tabela inicial
df = spark.range(100).selectExpr("id", "id * 2 as value")
df.write.format("delta").mode("overwrite").save("./delta_advanced")

# Carregar como DeltaTable
deltaTable = DeltaTable.forPath(spark, "./delta_advanced")

# UPDATE
deltaTable.update(
    condition=col("id") < 10,
    set={"value": col("value") * 10}
)

# DELETE
deltaTable.delete(condition=col("id") > 90)

# MERGE (UPSERT)
new_data = spark.range(95, 105).selectExpr("id", "id * 3 as value")

deltaTable.alias("target").merge(
    new_data.alias("source"),
    "target.id = source.id"
).whenMatchedUpdate(
    set={"value": col("source.value")}
).whenNotMatchedInsert(
    values={"id": col("source.id"), "value": col("source.value")}
).execute()

# Ler resultado com DuckDB
import duckdb
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")
result = con.execute("""
    SELECT * FROM delta_scan('./delta_advanced')
    ORDER BY id
""").df()

print("After Spark operations:")
print(result)

# Exemplo/Bloco 18
from pyspark.sql import SparkSession
import duckdb
from datetime import datetime

class DataLakehouseWorkflow:
    """
    Workflow usando Spark para ETL pesado e DuckDB para analytics
    """

    def __init__(self, delta_path: str):
        self.delta_path = delta_path

    def heavy_etl_with_spark(self):
        """
        ETL pesado com Spark (ex: processar TBs de dados)
        """
        spark = SparkSession.builder \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .master("local[*]") \
            .getOrCreate()

        print("Running heavy ETL with Spark...")

        # Simular processamento pesado
        df = spark.range(0, 5_000_000).selectExpr(
            "id",
            "id % 1000 as customer_id",
            "rand() * 1000 as amount",
            "date_sub(current_date(), cast(rand() * 365 as int)) as order_date"
        )

        # Transformações complexas
        from pyspark.sql.functions import col, when, sum as spark_sum

        df_transformed = df \
            .withColumn("amount_category",
                when(col("amount") < 100, "small")
                .when(col("amount") < 500, "medium")
                .otherwise("large")
            ) \
            .withColumn("year", col("order_date").substr(1, 4).cast("int")) \
            .withColumn("month", col("order_date").substr(6, 2).cast("int"))

        # Escrever resultado
        df_transformed.write \
            .format("delta") \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .save(self.delta_path)

        spark.stop()
        print(f"[OK] ETL completed. Data written to {self.delta_path}")

    def fast_analytics_with_duckdb(self):
        """
        Analytics rápidas com DuckDB
        """
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")

        print("\nRunning fast analytics with DuckDB...")

        # Queries analíticas interativas
        queries = {
            "Total by category": """
                SELECT
                    amount_category,
                    COUNT(*) as orders,
                    SUM(amount) as total_amount
                FROM delta_scan('{}')
                GROUP BY amount_category
            """,
            "Monthly trend (2024)": """
                SELECT
                    month,
                    COUNT(*) as orders,
                    AVG(amount) as avg_amount
                FROM delta_scan('{}')
                WHERE year = 2024
                GROUP BY month
                ORDER BY month
            """,
            "Top 10 customers": """
                SELECT
                    customer_id,
                    COUNT(*) as order_count,
                    SUM(amount) as total_spent
                FROM delta_scan('{}')
                GROUP BY customer_id
                ORDER BY total_spent DESC
                LIMIT 10
            """
        }

        for name, query_template in queries.items():
            query = query_template.format(self.delta_path)
            result = con.execute(query).df()

            print(f"\n{name}:")
            print(result.to_string(index=False))

        con.close()

# Executar workflow
workflow = DataLakehouseWorkflow("./lakehouse_data")
workflow.heavy_etl_with_spark()
workflow.fast_analytics_with_duckdb()

# Exemplo/Bloco 19
# Célula 1: Setup
import duckdb
import pandas as pd
from deltalake import write_deltalake

con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Célula 2: Carregar dados
df = con.execute("""
    SELECT * FROM delta_scan('./sales')
    WHERE order_date >= '2024-01-01'
""").df()

display(df.head(10))

# Célula 3: Análise exploratória
summary = df.describe()
display(summary)

# Célula 4: Visualização
import matplotlib.pyplot as plt

df.groupby('region')['amount'].sum().plot(kind='bar')
plt.title('Sales by Region')
plt.ylabel('Total Amount')
plt.show()

# Célula 5: Agregação
monthly = con.execute("""
    SELECT
        DATE_TRUNC('month', order_date) as month,
        COUNT(*) as orders,
        SUM(amount) as revenue
    FROM delta_scan('./sales')
    WHERE order_date >= '2024-01-01'
    GROUP BY 1
    ORDER BY 1
""").df()

display(monthly)

# Exemplo/Bloco 20
# Heavy processing: Spark
spark_df.write.format("delta").save("./processed")

# Analytics: DuckDB
duckdb.execute("SELECT * FROM delta_scan('./processed')").df()

# Exemplo/Bloco 21
# Eficiente: zero-copy
arrow_table = con.execute("SELECT * FROM delta_scan('./data')").arrow()
df = arrow_table.to_pandas()

# Menos eficiente: cópia
df = con.execute("SELECT * FROM delta_scan('./data')").df()

