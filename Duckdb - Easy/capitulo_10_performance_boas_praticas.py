# -*- coding: utf-8 -*-
"""
capitulo_10_performance_boas_praticas
"""

# capitulo_10_performance_boas_praticas
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()

# Configurar memória e temp directory
con.execute("SET memory_limit='2GB'")
con.execute("SET temp_directory='/path/to/temp'")
con.execute("SET preserve_insertion_order=false")

# Query em dataset maior que a memória
result = con.sql("""
    SELECT
        customer_id,
        sum(amount) as total
    FROM 'huge_transactions.parquet'
    GROUP BY customer_id
    ORDER BY total DESC
""").df()

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()

# Criar tabela
con.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")

# Prepared statement (melhor performance)
stmt = con.prepare("INSERT INTO users VALUES ($1, $2)")

# Executar múltiplas vezes
for i in range(10000):
    stmt.execute([i, f"User_{i}"])

# Muito mais rápido que preparar query toda vez

# Exemplo/Bloco 3
import duckdb

# In-memory SEM compressão (padrão)
con = duckdb.connect(':memory:')

# In-memory COM compressão
con = duckdb.connect(':memory:', config={'compress': True})

# Persistente (sempre com compressão)
con = duckdb.connect('database.db')

# Exemplo/Bloco 4
import duckdb
import time

# Criar dados de teste
con = duckdb.connect()
con.execute("CALL dbgen(sf=1)")  # TPC-H scale factor 1

# Benchmark query
queries = [
    "SELECT count(*) FROM lineitem",
    "SELECT l_returnflag, sum(l_quantity) FROM lineitem GROUP BY l_returnflag",
    "SELECT * FROM lineitem WHERE l_shipdate > '1998-01-01' LIMIT 10"
]

for query in queries:
    start = time.time()
    result = con.execute(query).fetchall()
    elapsed = time.time() - start
    print(f"Query: {query[:50]}...")
    print(f"Tempo: {elapsed:.3f}s")
    print()

# Exemplo/Bloco 5
import duckdb
import time

def pipeline_otimizado():
    # Configuração
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=8")
    con.execute("SET temp_directory='/fast/disk/temp'")

    print("1. Convertendo CSV para Parquet...")
    start = time.time()
    con.execute("""
        COPY (SELECT * FROM 'raw_data.csv')
        TO 'data.parquet'
        (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)
    """)
    print(f"   Tempo: {time.time() - start:.2f}s")

    print("\n2. Análise exploratória...")
    con.execute("SUMMARIZE SELECT * FROM 'data.parquet'").show()

    print("\n3. Query otimizada...")
    start = time.time()
    result = con.execute("""
        SELECT
            category,
            date_trunc('month', date) as month,
            sum(amount) as total_amount,
            count(*) as transaction_count
        FROM 'data.parquet'
        WHERE date >= '2024-01-01'
          AND amount > 0
        GROUP BY category, month
        ORDER BY category, month
    """).df()
    print(f"   Tempo: {time.time() - start:.2f}s")
    print(f"   Resultados: {len(result)} linhas")

    print("\n4. Exportando resultados...")
    start = time.time()
    con.execute("""
        COPY (
            SELECT * FROM result
        ) TO 'monthly_summary.parquet'
        (FORMAT parquet, COMPRESSION zstd)
    """)
    print(f"   Tempo: {time.time() - start:.2f}s")

    con.close()

if __name__ == "__main__":
    pipeline_otimizado()

