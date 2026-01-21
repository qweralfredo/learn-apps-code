# -*- coding: utf-8 -*-
"""
capitulo_02_instalacao_configuracao
"""

# capitulo_02_instalacao_configuracao
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

# Query simples para testar
duckdb.sql("SELECT 42").show()

# Exemplo/Bloco 2
import duckdb

# Conexão padrão (em memória)
con = duckdb.connect()
con.sql("SELECT 42 AS x").show()

# Exemplo/Bloco 3
import duckdb

# Criar ou conectar a um arquivo de banco de dados
con = duckdb.connect("file.db")
con.sql("CREATE TABLE test (i INTEGER)")
con.sql("INSERT INTO test VALUES (42)")
con.table("test").show()
con.close()

# Exemplo/Bloco 4
import duckdb

with duckdb.connect("file.db") as con:
    con.sql("CREATE TABLE test (i INTEGER)")
    con.sql("INSERT INTO test VALUES (42)")
    con.table("test").show()
# Conexão é fechada automaticamente

# Exemplo/Bloco 5
import duckdb

# Limitar a 1 thread (útil para debugging)
con = duckdb.connect(config={'threads': 1})

# Exemplo/Bloco 6
import duckdb

# Múltiplas configurações
con = duckdb.connect(config={
    'threads': 4,
    'max_memory': '4GB',
    'default_order': 'DESC'
})

# Exemplo/Bloco 7
import duckdb

# ✅ CORRETO: Cada thread tem sua própria conexão
def boa_pratica():
    con = duckdb.connect()
    con.sql("SELECT 1").fetchall()

# ❌ INCORRETO: Compartilhando conexão entre threads
conexao_global = duckdb.connect()

def ma_pratica():
    # Evite usar conexão global em múltiplas threads
    return conexao_global.sql("SELECT 1").fetchall()

# Exemplo/Bloco 8
import duckdb

r1 = duckdb.sql("SELECT 42 AS i")
duckdb.sql("SELECT i * 2 AS k FROM r1").show()

# Exemplo/Bloco 9
import duckdb

con = duckdb.connect()

# Instalar extensão da comunidade
con.install_extension("h3", repository="community")

# Carregar extensão
con.load_extension("h3")

# Exemplo/Bloco 10
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})

