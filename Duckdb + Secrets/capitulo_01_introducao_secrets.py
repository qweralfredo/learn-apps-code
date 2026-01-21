# -*- coding: utf-8 -*-
"""
Capítulo 1: Introdução a Secrets no DuckDB
===========================================

Este script demonstra os conceitos básicos de Secrets no DuckDB.
"""

import duckdb
import os

# Configuração UTF-8 para Windows
if os.name == 'nt':
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def exemplo_01_criar_secrets_basicos():
    """Exemplo 1.1: Criar secrets básicos"""
    print("\n" + "="*60)
    print("Exemplo 1.1: Criar Secrets Básicos")
    print("="*60)

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Secret S3 básico (MOCK - não use credenciais reais!)
    con.execute("""
        CREATE SECRET my_s3_bucket (
            TYPE s3,
            KEY_ID 'AKIAIOSFODNN7EXAMPLE',
            SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            REGION 'us-east-1'
        )
    """)

    print("✓ Secret S3 criado com sucesso!")

    # Verificar secret
    secret_info = con.execute("""
        SELECT name, type, provider, scope
        FROM duckdb_secrets()
        WHERE name = 'my_s3_bucket'
    """).fetchone()

    if secret_info:
        print(f"\nNome: {secret_info[0]}")
        print(f"Tipo: {secret_info[1]}")
        print(f"Provider: {secret_info[2]}")
        print(f"Scope: {secret_info[3] if secret_info[3] else '(global)'}")

    con.close()


def exemplo_02_multiplos_tipos():
    """Exemplo 1.2: Criar secrets de diferentes tipos"""
    print("\n" + "="*60)
    print("Exemplo 1.2: Múltiplos Tipos de Secrets")
    print("="*60)

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Secret S3
    con.execute("""
        CREATE SECRET aws_data (
            TYPE s3,
            KEY_ID 'AKIAIOSFODNN7EXAMPLE',
            SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            REGION 'us-east-1'
        )
    """)

    # Secret HTTP com bearer token
    con.execute("""
        CREATE SECRET api_auth (
            TYPE http,
            BEARER_TOKEN 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.example'
        )
    """)

    # Listar todos os secrets
    secrets = con.execute("SELECT name, type, provider FROM duckdb_secrets()").df()

    print("\nSecrets criados:")
    print(secrets)

    con.close()


def exemplo_03_listar_secrets():
    """Exemplo 1.3: Listar e filtrar secrets"""
    print("\n" + "="*60)
    print("Exemplo 1.3: Listar e Filtrar Secrets")
    print("="*60)

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Criar alguns secrets
    con.execute("CREATE SECRET s1 (TYPE s3, KEY_ID 'k1', SECRET 's1')")
    con.execute("CREATE SECRET s2 (TYPE s3, KEY_ID 'k2', SECRET 's2')")
    con.execute("CREATE SECRET h1 (TYPE http, BEARER_TOKEN 'token')")

    # Listar todos
    all_secrets = con.execute("SELECT * FROM duckdb_secrets()").df()
    print("\nTodos os secrets:")
    print(all_secrets[['name', 'type', 'provider', 'persistent', 'storage']])

    # Filtrar apenas S3
    s3_secrets = con.execute("""
        SELECT name, type, provider
        FROM duckdb_secrets()
        WHERE type = 's3'
    """).df()

    print("\nApenas secrets S3:")
    print(s3_secrets)

    con.close()


def exemplo_04_deletar_secrets():
    """Exemplo 1.4: Deletar secrets"""
    print("\n" + "="*60)
    print("Exemplo 1.4: Deletar Secrets")
    print("="*60)

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Criar secret
    con.execute("CREATE SECRET temp_secret (TYPE s3, KEY_ID 'k', SECRET 's')")

    print("Antes de deletar:")
    print(con.execute("SELECT name FROM duckdb_secrets()").df())

    # Deletar secret
    con.execute("DROP SECRET temp_secret")

    print("\nDepois de deletar:")
    print(con.execute("SELECT name FROM duckdb_secrets()").df())

    # DROP IF EXISTS (não causa erro se não existir)
    con.execute("DROP SECRET IF EXISTS non_existent_secret")
    print("\n✓ DROP IF EXISTS executado sem erro")

    con.close()


def exemplo_05_which_secret():
    """Exemplo 1.5: Usar which_secret() para verificar matching"""
    print("\n" + "="*60)
    print("Exemplo 1.5: which_secret() - Verificar Matching")
    print("="*60)

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # Criar secrets com diferentes scopes
    con.execute("""
        CREATE SECRET bucket1 (
            TYPE s3,
            KEY_ID 'key1',
            SECRET 'secret1',
            SCOPE 's3://bucket1/'
        )
    """)

    con.execute("""
        CREATE SECRET bucket2 (
            TYPE s3,
            KEY_ID 'key2',
            SECRET 'secret2',
            SCOPE 's3://bucket2/'
        )
    """)

    # Verificar qual secret será usado para cada URL
    urls = [
        's3://bucket1/file.parquet',
        's3://bucket2/data.csv',
        's3://bucket3/other.parquet'
    ]

    print("\nVerificando matching de secrets:\n")
    for url in urls:
        try:
            result = con.execute(f"""
                SELECT name FROM which_secret('{url}', 's3')
            """).fetchone()

            if result:
                print(f"{url:40} → {result[0]}")
            else:
                print(f"{url:40} → Nenhum secret encontrado")
        except Exception as e:
            print(f"{url:40} → Erro: {e}")

    con.close()


def exercicio_01_pratico():
    """Exercício prático 1: Criar, listar e deletar secrets"""
    print("\n" + "="*60)
    print("EXERCÍCIO PRÁTICO 1: Gerenciamento de Secrets")
    print("="*60)

    con = duckdb.connect(':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # 1. Criar 3 secrets S3
    print("\n1. Criando 3 secrets S3...")
    con.execute("CREATE SECRET s3_prod (TYPE s3, KEY_ID 'prod_key', SECRET 'prod_secret')")
    con.execute("CREATE SECRET s3_dev (TYPE s3, KEY_ID 'dev_key', SECRET 'dev_secret')")
    con.execute("CREATE SECRET s3_test (TYPE s3, KEY_ID 'test_key', SECRET 'test_secret')")

    # 2. Listar todos
    print("\n2. Listando todos os secrets:")
    secrets = con.execute("SELECT name, type FROM duckdb_secrets()").df()
    print(secrets)

    # 3. Deletar um secret
    print("\n3. Deletando secret 's3_test'...")
    con.execute("DROP SECRET s3_test")

    # 4. Listar novamente
    print("\n4. Listando secrets restantes:")
    secrets_after = con.execute("SELECT name, type FROM duckdb_secrets()").df()
    print(secrets_after)

    print("\n✓ Exercício concluído!")

    con.close()


def main():
    """Função principal - executa todos os exemplos"""
    print("\n" + "="*70)
    print(" " * 10 + "CAPÍTULO 1: INTRODUÇÃO A SECRETS NO DUCKDB")
    print("="*70)

    exemplo_01_criar_secrets_basicos()
    exemplo_02_multiplos_tipos()
    exemplo_03_listar_secrets()
    exemplo_04_deletar_secrets()
    exemplo_05_which_secret()
    exercicio_01_pratico()

    print("\n" + "="*70)
    print("✓ Todos os exemplos do Capítulo 1 foram executados com sucesso!")
    print("="*70)


if __name__ == "__main__":
    main()
