# -*- coding: utf-8 -*-
"""
capitulo_06_integracao_python
"""

# capitulo_06_integracao_python
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

# Query simples
result = duckdb.sql("SELECT 42 AS answer")
result.show()

# Exemplo/Bloco 2
import duckdb

# Primeira query
r1 = duckdb.sql("SELECT 42 AS i")

# Segunda query referencia r1
duckdb.sql("SELECT i * 2 AS k FROM r1").show()

# Exemplo/Bloco 3
import duckdb

# Ler CSV como Relation
rel = duckdb.read_csv("example.csv")
print(rel)

# Query direta em arquivo CSV
result = duckdb.sql("SELECT * FROM 'example.csv'")
result.show()

# Exemplo/Bloco 4
import duckdb

# Ler Parquet
rel = duckdb.read_parquet("example.parquet")

# Query direta
duckdb.sql("SELECT * FROM 'example.parquet'").show()

# Exemplo/Bloco 5
import duckdb

# Ler JSON
rel = duckdb.read_json("example.json")

# Query direta
duckdb.sql("SELECT * FROM 'example.json'").show()

# Exemplo/Bloco 6
import duckdb
import pandas as pd

# Criar DataFrame Pandas
pandas_df = pd.DataFrame({"a": [42]})

# Consultar DataFrame diretamente pelo nome da variável
duckdb.sql("SELECT * FROM pandas_df").show()

# Exemplo/Bloco 7
import duckdb
import polars as pl

# Criar DataFrame Polars
polars_df = pl.DataFrame({"a": [42]})

# Consultar DataFrame Polars
duckdb.sql("SELECT * FROM polars_df").show()

# Exemplo/Bloco 8
import duckdb
import pyarrow as pa

# Criar Arrow Table
arrow_table = pa.Table.from_pydict({"a": [42]})

# Consultar Arrow Table
duckdb.sql("SELECT * FROM arrow_table").show()

# Exemplo/Bloco 9
import duckdb

result = duckdb.sql("SELECT 42 AS num, 'hello' AS text")

# Lista de tuplas
rows = result.fetchall()
print(rows)  # [(42, 'hello')]

# Uma linha
row = result.fetchone()
print(row)  # (42, 'hello')

# Exemplo/Bloco 10
import duckdb

result = duckdb.sql("SELECT 42 AS num, 'hello' AS text")
df = result.df()
print(type(df))  # <class 'pandas.core.frame.DataFrame'>
print(df)

# Exemplo/Bloco 11
import duckdb

result = duckdb.sql("SELECT 42 AS num, 'hello' AS text")
pl_df = result.pl()
print(type(pl_df))  # <class 'polars.dataframe.frame.DataFrame'>

# Exemplo/Bloco 12
import duckdb

result = duckdb.sql("SELECT 42 AS num, 'hello' AS text")
arrow = result.arrow()
print(type(arrow))  # <class 'pyarrow.lib.Table'>

# Exemplo/Bloco 13
import duckdb

result = duckdb.sql("SELECT 42 AS num, 100 AS total")
numpy_result = result.fetchnumpy()
print(numpy_result)
# {'num': array([42]), 'total': array([100])}

# Exemplo/Bloco 14
import duckdb

# Query e salvar como Parquet
duckdb.sql("SELECT 42 AS num").write_parquet("out.parquet")

# Ou usando COPY
duckdb.sql("COPY (SELECT 42) TO 'out.parquet'")

# Exemplo/Bloco 15
import duckdb

# Salvar como CSV
duckdb.sql("SELECT 42 AS num").write_csv("out.csv")

# Exemplo/Bloco 16
import duckdb

# Salvar como JSON
duckdb.sql("SELECT 42 AS num").write_json("out.json")

# Exemplo/Bloco 17
import duckdb

# Criar conexão em memória
con = duckdb.connect()
con.sql("SELECT 42").show()
con.close()

# Exemplo/Bloco 18
import duckdb

# Criar/conectar a arquivo de banco de dados
con = duckdb.connect("file.db")
con.sql("CREATE TABLE test (i INTEGER)")
con.sql("INSERT INTO test VALUES (42)")
con.table("test").show()
con.close()

# Exemplo/Bloco 19
import duckdb

with duckdb.connect("file.db") as con:
    con.sql("CREATE TABLE test (i INTEGER)")
    con.sql("INSERT INTO test VALUES (42)")
    con.table("test").show()
# Conexão fechada automaticamente

# Exemplo/Bloco 20
import duckdb

# Configurar número de threads
con = duckdb.connect(config={'threads': 4})

# Múltiplas configurações
con = duckdb.connect(config={
    'threads': 4,
    'max_memory': '4GB',
    'default_order': 'DESC'
})

# Exemplo/Bloco 21
import duckdb
import pandas as pd

# Criar dados de exemplo
vendas = pd.DataFrame({
    'data': pd.date_range('2024-01-01', periods=100),
    'produto': ['A', 'B', 'C'] * 33 + ['A'],
    'quantidade': range(1, 101),
    'preco': [10.5, 20.0, 15.75] * 33 + [10.5]
})

# Análise usando DuckDB diretamente no DataFrame
resultado = duckdb.sql("""
    SELECT
        produto,
        COUNT(*) as total_vendas,
        SUM(quantidade) as total_quantidade,
        SUM(quantidade * preco) as receita_total,
        AVG(preco) as preco_medio
    FROM vendas
    GROUP BY produto
    ORDER BY receita_total DESC
""").df()

print(resultado)

# Exemplo/Bloco 22
import duckdb

# Pipeline ETL completo
with duckdb.connect() as con:
    # Extract: Ler de múltiplas fontes
    con.sql("CREATE TABLE vendas AS SELECT * FROM 'vendas.csv'")
    con.sql("CREATE TABLE clientes AS SELECT * FROM 'clientes.parquet'")

    # Transform: Processar e limpar dados
    con.sql("""
        CREATE TABLE vendas_processadas AS
        SELECT
            v.venda_id,
            v.data,
            c.cliente_nome,
            c.regiao,
            v.valor_total,
            v.quantidade
        FROM vendas v
        JOIN clientes c ON v.cliente_id = c.id
        WHERE v.data >= '2024-01-01'
    """)

    # Load: Exportar para Parquet
    con.sql("""
        COPY vendas_processadas
        TO 'vendas_processadas.parquet'
        (FORMAT parquet, COMPRESSION zstd)
    """)

    print("ETL concluído!")

# Exemplo/Bloco 23
import duckdb
import pandas as pd

# Criar conexão global para o notebook
con = duckdb.connect()

# Exemplo/Bloco 24
# Carregar dados
df = pd.read_csv('sales.csv')

# Análise rápida com DuckDB
duckdb.sql("""
    SELECT
        category,
        COUNT(*) as count,
        AVG(price) as avg_price,
        SUM(revenue) as total_revenue
    FROM df
    GROUP BY category
    ORDER BY total_revenue DESC
""").show()

# Exemplo/Bloco 25
import duckdb
from concurrent.futures import ThreadPoolExecutor

# ✅ CORRETO: Cada thread tem sua própria conexão
def boa_pratica(thread_id):
    con = duckdb.connect('database.db')
    result = con.sql(f"SELECT {thread_id}").fetchall()
    con.close()
    return result

# ❌ INCORRETO: Compartilhando conexão entre threads
conexao_global = duckdb.connect()

def ma_pratica(thread_id):
    # Evite usar conexão compartilhada em múltiplas threads
    return conexao_global.sql(f"SELECT {thread_id}").fetchall()

# Uso correto com threads
with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(boa_pratica, range(4)))

# Exemplo/Bloco 26
import duckdb

con = duckdb.connect()

# Instalar extensão da comunidade
con.install_extension("h3", repository="community")

# Carregar extensão
con.load_extension("h3")

# Usar extensão
con.sql("SELECT h3_cell_to_lat_lng('8928308280fffff')").show()

# Exemplo/Bloco 27
import duckdb

# Permitir extensões não-assinadas (use com cuidado!)
con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

# Exemplo/Bloco 28
import duckdb

con = duckdb.connect()

# Criar prepared statement
stmt = con.prepare("SELECT $1 * $2")

# Executar com diferentes parâmetros
result = stmt.execute([2, 3]).fetchall()
print(result)  # [(6,)]

result = stmt.execute([5, 7]).fetchall()
print(result)  # [(35,)]

# Exemplo/Bloco 29
import duckdb

# Processar arquivo maior que a memória
with duckdb.connect() as con:
    # Configurar memória máxima
    con.execute("SET memory_limit='2GB'")

    # Query em arquivo grande
    result = con.sql("""
        SELECT
            date_trunc('month', timestamp) as month,
            count(*) as events,
            avg(duration) as avg_duration
        FROM 'large_file.parquet'
        WHERE status = 'success'
        GROUP BY month
        ORDER BY month
    """).df()

    print(result)

# Exemplo/Bloco 30
import duckdb
import pandas as pd

# Dados em memória (Pandas)
clientes_df = pd.read_csv('clientes.csv')

# Combinar com arquivo no disco
resultado = duckdb.sql("""
    SELECT
        v.venda_id,
        c.nome,
        v.valor,
        v.data
    FROM 'vendas.parquet' v
    JOIN clientes_df c ON v.cliente_id = c.id
    WHERE v.valor > 1000
    ORDER BY v.data DESC
    LIMIT 10
""").df()

print(resultado)

# Exemplo/Bloco 31
# ✅ BOM: Reutilizar conexão
con = duckdb.connect()
for i in range(1000):
    con.sql(f"SELECT {i}").fetchall()
con.close()

# ❌ RUIM: Criar nova conexão toda vez
for i in range(1000):
    con = duckdb.connect()
    con.sql(f"SELECT {i}").fetchall()
    con.close()

# Exemplo/Bloco 32
import duckdb

# Lento: CSV
duckdb.sql("SELECT * FROM 'large.csv'").df()

# Rápido: Parquet
duckdb.sql("SELECT * FROM 'large.parquet'").df()

