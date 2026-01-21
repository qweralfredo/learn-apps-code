# -*- coding: utf-8 -*-
"""
Capítulo 2: Instalação e Configuração da Extensão httpfs
==========================================================

Este script demonstra a instalação e configuração da extensão httpfs.

Autor: Curso DuckDB + S3
"""

import duckdb
import os

if os.name == 'nt':
    os.system('chcp 65001 > nul')

print("=" * 70)
print("CAPÍTULO 2: INSTALAÇÃO E CONFIGURAÇÃO HTTPFS")
print("=" * 70)

# Criar conexão
conn = duckdb.connect(':memory:')

# =============================================================================
# 1. INSTALAR EXTENSÃO HTTPFS
# =============================================================================
print("\n1. Instalando extensão httpfs")
print("-" * 70)

try:
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    print("✓ Extensão httpfs instalada e carregada com sucesso!")
except Exception as e:
    print(f"Nota: {e}")
    print("A extensão pode já estar instalada.")

# =============================================================================
# 2. VERIFICAR INSTALAÇÃO
# =============================================================================
print("\n2. Verificando instalação")
print("-" * 70)

# Listar extensões
result = conn.execute("""
    SELECT name, loaded, installed, description
    FROM duckdb_extensions()
    WHERE name = 'httpfs'
""").fetchdf()
print(result)

# =============================================================================
# 3. TESTAR CONECTIVIDADE HTTP(S)
# =============================================================================
print("\n3. Testando conectividade HTTP(S)")
print("-" * 70)

# Ler arquivo Parquet público (exemplo DuckDB)
print("Lendo arquivo Parquet público...")
try:
    result = conn.execute("""
        SELECT count(*) as total_records
        FROM 'https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet'
    """).fetchone()
    print(f"✓ Sucesso! Total de registros: {result[0]}")
except Exception as e:
    print(f"❌ Erro: {e}")
    print("Verifique sua conexão com a internet.")

# =============================================================================
# 4. EXPLORAR METADADOS SEM DOWNLOAD COMPLETO
# =============================================================================
print("\n4. Explorar metadados (Partial Reading)")
print("-" * 70)

try:
    # Ver schema sem baixar dados
    result = conn.execute("""
        SELECT *
        FROM parquet_schema('https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet')
    """).fetchdf()
    print("Schema do arquivo:")
    print(result)
except Exception as e:
    print(f"Erro ao acessar metadados: {e}")

# =============================================================================
# 5. PARTIAL READING DEMONSTRATION
# =============================================================================
print("\n5. Demonstração de Partial Reading")
print("-" * 70)

try:
    # Leitura de coluna específica (não baixa o arquivo inteiro)
    result = conn.execute("""
        SELECT n_name, n_comment
        FROM 'https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet'
        LIMIT 5
    """).fetchdf()
    print("Primeiras 5 nações (apenas 2 colunas):")
    print(result)
except Exception as e:
    print(f"Erro: {e}")

# =============================================================================
# 6. CONFIGURAÇÕES AVANÇADAS
# =============================================================================
print("\n6. Configurações avançadas (opcionais)")
print("-" * 70)

# Configurar timeout
print("Configurando timeout HTTP...")
conn.execute("SET http_timeout = 30000")  # 30 segundos

# Configurar threads
print("Configurando threads...")
conn.execute("SET threads = 4")

# Verificar configurações
result = conn.execute("""
    SELECT
        current_setting('http_timeout') as http_timeout,
        current_setting('threads') as threads
""").fetchdf()
print(result)

# =============================================================================
# 7. EXEMPLO PRÁTICO: ANÁLISE DE DADOS PÚBLICOS
# =============================================================================
print("\n7. Exemplo prático: Análise de dados públicos")
print("-" * 70)

try:
    result = conn.execute("""
        SELECT
            n_regionkey,
            count(*) as num_nations,
            listagg(n_name, ', ') as nations
        FROM 'https://shell.duckdb.org/data/tpch/0_01/parquet/nation.parquet'
        GROUP BY n_regionkey
        ORDER BY n_regionkey
    """).fetchdf()
    print("Nações por região:")
    print(result)
except Exception as e:
    print(f"Erro: {e}")

# =============================================================================
# 8. EXERCÍCIOS PRÁTICOS
# =============================================================================
print("\n8. Exercícios práticos")
print("-" * 70)

print("""
Exercícios:

1. Instale a extensão httpfs e verifique se está carregada
2. Leia o arquivo customer.parquet público e conte os registros
3. Veja o schema do arquivo lineitem.parquet
4. Configure timeout de 60 segundos
5. Leia apenas 3 colunas do arquivo orders.parquet

URLs de exemplo:
- https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet
- https://shell.duckdb.org/data/tpch/0_01/parquet/lineitem.parquet
- https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet
""")

# Exercício 1 - Solução
print("\nExercício 2 - Solução (customer.parquet):")
try:
    result = conn.execute("""
        SELECT count(*) as total_customers
        FROM 'https://shell.duckdb.org/data/tpch/0_01/parquet/customer.parquet'
    """).fetchone()
    print(f"✓ Total de clientes: {result[0]}")
except Exception as e:
    print(f"Erro: {e}")

# =============================================================================
# CONCLUSÃO
# =============================================================================
print("\n" + "=" * 70)
print("CONCLUSÃO")
print("=" * 70)
print("""
Neste capítulo você aprendeu:
✓ Instalar e carregar a extensão httpfs
✓ Verificar extensões instaladas
✓ Testar conectividade com arquivos públicos
✓ Usar partial reading para otimização
✓ Configurar timeouts e threads
✓ Explorar metadados de arquivos remotos

Próximo capítulo: Configuração de credenciais AWS
""")

conn.close()
print("\n✓ Conexão fechada!")
