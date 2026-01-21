# -*- coding: utf-8 -*-
"""
capitulo-02-instalacao-e-configuracao
"""

# capitulo-02-instalacao-e-configuracao
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
print(duckdb.__version__)  # Deve mostrar 0.10.3 ou superior

# Exemplo/Bloco 2
import duckdb

# Criar conexão em memória
con = duckdb.connect()

# Executar query
result = con.execute("SELECT 42 as answer").fetchall()
print(result)  # [(42,)]

# Fechar conexão
con.close()

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()

# Instalar extensão
con.execute("INSTALL delta")

# Carregar extensão
con.execute("LOAD delta")

# Usar a extensão
result = con.execute("SELECT * FROM delta_scan('./my_delta_table')").fetchdf()
print(result)

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()

# Configurar durante a conexão
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")

# Ou no connect
con = duckdb.connect(config={
    'memory_limit': '4GB',
    'threads': 4
})

# Exemplo/Bloco 5
import duckdb

# Banco persistente
con = duckdb.connect('my_database.db')

# Em memória
con = duckdb.connect(':memory:')

# Read-only mode
con = duckdb.connect('my_database.db', read_only=True)

# Exemplo/Bloco 6
import duckdb
import sys

def setup_duckdb():
    """
    Setup completo do DuckDB com extensões necessárias
    """
    print(f"DuckDB version: {duckdb.__version__}")

    # Verificar versão mínima
    if duckdb.__version__ < '0.10.3':
        print("ERROR: DuckDB 0.10.3+ required for Delta support")
        sys.exit(1)

    # Criar conexão
    con = duckdb.connect('analytics.db')

    # Configurar performance
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=8")

    # Instalar extensões
    extensions = ['delta', 'httpfs', 'parquet']

    for ext in extensions:
        print(f"Installing {ext}...")
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")

    # Verificar instalação
    result = con.execute("""
        SELECT extension_name, loaded
        FROM duckdb_extensions()
        WHERE extension_name IN ('delta', 'httpfs', 'parquet')
    """).fetchdf()

    print("\nInstalled extensions:")
    print(result)

    return con

if __name__ == "__main__":
    con = setup_duckdb()
    print("\nDuckDB setup completed successfully!")
    con.close()

# Exemplo/Bloco 7
import duckdb

def test_delta_extension():
    """
    Testar funcionalidade da extensão Delta
    """
    con = duckdb.connect()

    # Carregar extensão automaticamente
    try:
        # Criar tabela Delta de teste com Python
        from deltalake import write_deltalake

        # Criar DataFrame de teste
        df = con.execute("""
            SELECT
                i as id,
                'value-' || i as value
            FROM range(0, 10) tbl(i)
        """).df()

        # Escrever Delta table
        write_deltalake("./test_delta", df)

        # Ler com DuckDB
        result = con.execute("""
            SELECT COUNT(*) as total
            FROM delta_scan('./test_delta')
        """).fetchone()

        print(f"Delta test passed! Rows: {result[0]}")
        return True

    except Exception as e:
        print(f"Delta test failed: {e}")
        return False
    finally:
        con.close()

if __name__ == "__main__":
    test_delta_extension()

# Exemplo/Bloco 8
# Instalar Jupyter
# pip install jupyter duckdb pandas

# Criar notebook
# jupyter notebook

# Exemplo/Bloco 9
# No notebook
import duckdb
import pandas as pd

con = duckdb.connect()

# Query e display
df = con.execute("SELECT * FROM delta_scan('./my_table')").df()
display(df)

