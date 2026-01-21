# -*- coding: utf-8 -*-
"""
Capítulo 2: Instalação e Configuração do DuckDB
Demonstração de instalação, configuração e verificação do ambiente
"""

import duckdb
import sys
import os

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def verificar_versao():
    """Verificar versão do DuckDB"""
    print("\n1. VERIFICAÇÃO DE VERSÃO")
    print("-" * 60)
    print(f"   Versão do DuckDB: {duckdb.__version__}")

    con = duckdb.connect()
    version = con.execute("SELECT version()").fetchone()[0]
    platform = con.execute("SELECT platform FROM pragma_version()").fetchone()[0]
    print(f"   Versão completa: {version}")
    print(f"   Plataforma: {platform}")
    con.close()

def configurar_extensoes():
    """Instalar e carregar extensões necessárias"""
    print("\n2. CONFIGURAÇÃO DE EXTENSÕES")
    print("-" * 60)

    con = duckdb.connect()

    # Extensão Delta
    print("   Instalando extensão Delta...")
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    print("   ✓ Extensão Delta instalada")

    # Extensão httpfs (para S3/Azure/GCS)
    print("   Instalando extensão httpfs...")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    print("   ✓ Extensão httpfs instalada")

    # Verificar extensões instaladas
    print("\n   Extensões instaladas:")
    extensoes = con.execute("""
        SELECT
            extension_name,
            loaded,
            installed
        FROM duckdb_extensions()
        WHERE installed = true
    """).df()
    print(extensoes.to_string(index=False))

    con.close()

def configurar_performance():
    """Configurar parâmetros de performance"""
    print("\n3. CONFIGURAÇÃO DE PERFORMANCE")
    print("-" * 60)

    con = duckdb.connect()

    # Configurar memória
    con.execute("SET memory_limit='4GB'")
    print("   ✓ Memory limit: 4GB")

    # Configurar threads
    con.execute("SET threads=4")
    print("   ✓ Threads: 4")

    # Habilitar progress bar
    con.execute("SET enable_progress_bar=true")
    print("   ✓ Progress bar habilitada")

    # Verificar configurações
    print("\n   Configurações atuais:")
    config = con.execute("""
        SELECT name, value
        FROM duckdb_settings()
        WHERE name IN ('memory_limit', 'threads', 'enable_progress_bar')
    """).df()
    print(config.to_string(index=False))

    con.close()

def criar_banco_persistente():
    """Criar banco de dados persistente"""
    print("\n4. BANCO DE DADOS PERSISTENTE")
    print("-" * 60)

    db_path = "C:/projetos/Cursos/Duckdb + Delta/code/analytics.duckdb"

    # Criar banco persistente
    con = duckdb.connect(db_path)
    print(f"   ✓ Banco criado: {db_path}")

    # Criar tabela de exemplo
    con.execute("""
        CREATE TABLE IF NOT EXISTS config (
            chave VARCHAR,
            valor VARCHAR,
            data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    con.execute("""
        INSERT INTO config VALUES
        ('versao', '1.0', CURRENT_TIMESTAMP),
        ('ambiente', 'desenvolvimento', CURRENT_TIMESTAMP)
    """)
    print("   ✓ Tabela 'config' criada")

    # Verificar
    df = con.execute("SELECT * FROM config").df()
    print("\n   Dados na tabela 'config':")
    print(df.to_string(index=False))

    con.close()

    # Verificar tamanho do arquivo
    if os.path.exists(db_path):
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"\n   Tamanho do arquivo: {size_mb:.2f} MB")

def testar_delta_extension():
    """Testar funcionalidade da extensão Delta"""
    print("\n5. TESTE DA EXTENSÃO DELTA")
    print("-" * 60)

    try:
        from deltalake import write_deltalake
        import pandas as pd

        # Criar DataFrame de teste
        df = pd.DataFrame({
            'id': range(10),
            'nome': [f'Item {i}' for i in range(10)],
            'valor': [i * 10.5 for i in range(10)]
        })

        # Escrever Delta table
        delta_path = "C:/projetos/Cursos/Duckdb + Delta/code/test_delta"
        write_deltalake(delta_path, df, mode="overwrite")
        print(f"   ✓ Delta table criada: {delta_path}")

        # Ler com DuckDB
        con = duckdb.connect()
        con.execute("LOAD delta")

        result = con.execute(f"""
            SELECT COUNT(*) as total, ROUND(AVG(valor), 2) as media
            FROM delta_scan('{delta_path}')
        """).fetchone()

        print(f"   ✓ Delta table lida com sucesso")
        print(f"   Total registros: {result[0]}")
        print(f"   Valor médio: {result[1]}")

        con.close()

    except ImportError:
        print("   ⚠ Biblioteca 'deltalake' não instalada")
        print("   Execute: pip install deltalake")
    except Exception as e:
        print(f"   ✗ Erro no teste: {e}")

def verificar_ambiente():
    """Verificar ambiente completo"""
    print("\n6. VERIFICAÇÃO DO AMBIENTE")
    print("-" * 60)

    con = duckdb.connect()

    # Verificar configurações importantes
    verificacoes = [
        ("Versão DuckDB", f"SELECT version()"),
        ("Plataforma", f"SELECT platform FROM pragma_version()"),
        ("Extensões disponíveis", f"SELECT COUNT(*) FROM duckdb_extensions()"),
        ("Extensões instaladas", f"SELECT COUNT(*) FROM duckdb_extensions() WHERE installed = true"),
    ]

    for nome, query in verificacoes:
        try:
            resultado = con.execute(query).fetchone()[0]
            print(f"   ✓ {nome}: {resultado}")
        except Exception as e:
            print(f"   ✗ {nome}: Erro - {e}")

    con.close()

def main():
    print("=" * 80)
    print("CAPÍTULO 2: INSTALAÇÃO E CONFIGURAÇÃO DO DUCKDB")
    print("=" * 80)

    verificar_versao()
    configurar_extensoes()
    configurar_performance()
    criar_banco_persistente()
    testar_delta_extension()
    verificar_ambiente()

    print("\n" + "=" * 80)
    print("✓ Setup completo do DuckDB concluído com sucesso!")
    print("=" * 80)
    print("\nPróximos passos:")
    print("  1. Extensões Delta e httpfs estão prontas para uso")
    print("  2. Banco de dados persistente criado em 'analytics.duckdb'")
    print("  3. Configurações de performance aplicadas")

if __name__ == "__main__":
    main()
