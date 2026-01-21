# -*- coding: utf-8 -*-
"""
Secrets-01-introducao-secrets
"""

# Secrets-01-introducao-secrets
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()

# 1. Interface unificada para diferentes backends
con.execute("""
    CREATE SECRET s3_secret (
        TYPE s3,
        KEY_ID 'my_key',
        SECRET 'my_secret'
    )
""")

con.execute("""
    CREATE SECRET azure_secret (
        TYPE azure,
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;...'
    )
""")

# 2. Uso automático baseado em URL
# DuckDB escolhe o secret correto automaticamente
result = con.execute("SELECT * FROM 's3://bucket/file.parquet'").df()
print(result)

# 3. Listar secrets
secrets = con.execute("SELECT * FROM duckdb_secrets()").df()
print("\nSecrets configurados:")
print(secrets[['name', 'type', 'scope']])

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()

# Carregar extensions necessárias
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL azure; LOAD azure;")

# Ver tipos de secrets disponíveis
print("Tipos de secrets suportados:")
print("- s3: AWS S3")
print("- r2: Cloudflare R2")
print("- gcs: Google Cloud Storage")
print("- azure: Azure Blob Storage")
print("- http: HTTP/HTTPS")
print("- huggingface: Hugging Face")
print("- iceberg: Iceberg REST Catalog")
print("- mysql: MySQL Database")
print("- postgres: PostgreSQL Database")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secret para S3
con.execute("""
    CREATE SECRET aws_data (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1'
    )
""")

# Secret para HTTP com autenticação
con.execute("""
    CREATE SECRET api_auth (
        TYPE http,
        BEARER_TOKEN 'my_bearer_token_here'
    )
""")

# Listar todos os secrets
secrets = con.execute("SELECT name, type, provider FROM duckdb_secrets()").df()
print("Secrets criados:")
print(secrets)

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar secret para S3
con.execute("""
    CREATE SECRET my_s3_bucket (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1'
    )
""")

print("Secret criado com sucesso!")

# Verificar secret
secret_info = con.execute("""
    SELECT name, type, provider, scope
    FROM duckdb_secrets()
    WHERE name = 'my_s3_bucket'
""").fetchone()

print(f"\nNome: {secret_info[0]}")
print(f"Tipo: {secret_info[1]}")
print(f"Provider: {secret_info[2]}")
print(f"Scope: {secret_info[3]}")

# Usar secret para ler dados
# (assumindo que você tem um bucket S3 configurado)
# result = con.execute("SELECT * FROM 's3://my-bucket/data.parquet'").df()
# print(result)

# Exemplo/Bloco 5
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar secret apenas se não existir
con.execute("""
    CREATE SECRET IF NOT EXISTS my_s3 (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret'
    )
""")

print("Secret criado (ou já existia)")

# Tentar criar novamente - não causa erro
con.execute("""
    CREATE SECRET IF NOT EXISTS my_s3 (
        TYPE s3,
        KEY_ID 'another_key',
        SECRET 'another_secret'
    )
""")

print("Comando executado com sucesso (secret existente não foi substituído)")

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar secret
con.execute("""
    CREATE SECRET s3_auto (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        REGION 'us-east-1'
    )
""")

# DuckDB usa automaticamente o secret para queries S3
# Não é necessário referenciar o secret explicitamente
# result = con.execute("""
#     SELECT * FROM 's3://my-bucket/data.parquet'
# """).df()

print("Secret será usado automaticamente para URLs s3://")

# Exemplo/Bloco 7
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar múltiplos secrets com diferentes scopes
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
secret1 = con.execute("""
    SELECT * FROM which_secret('s3://bucket1/file.parquet', 's3')
""").df()

secret2 = con.execute("""
    SELECT * FROM which_secret('s3://bucket2/file.parquet', 's3')
""").df()

print("Para s3://bucket1/:")
print(secret1[['name', 'scope']])

print("\nPara s3://bucket2/:")
print(secret2[['name', 'scope']])

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar alguns secrets
con.execute("CREATE SECRET s1 (TYPE s3, KEY_ID 'k1', SECRET 's1')")
con.execute("CREATE SECRET s2 (TYPE s3, KEY_ID 'k2', SECRET 's2')")
con.execute("CREATE SECRET h1 (TYPE http, BEARER_TOKEN 'token')")

# Listar todos os secrets
all_secrets = con.execute("SELECT * FROM duckdb_secrets()").df()

print("Todos os secrets:")
print(all_secrets[['name', 'type', 'provider', 'persistent', 'storage']])

# Exemplo/Bloco 9
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar secrets
con.execute("CREATE SECRET aws1 (TYPE s3, KEY_ID 'k1', SECRET 's1')")
con.execute("CREATE SECRET aws2 (TYPE s3, KEY_ID 'k2', SECRET 's2')")
con.execute("CREATE SECRET api (TYPE http, BEARER_TOKEN 't')")

# Listar apenas secrets do tipo S3
s3_secrets = con.execute("""
    SELECT name, type, provider
    FROM duckdb_secrets()
    WHERE type = 's3'
""").df()

print("Secrets S3:")
print(s3_secrets)

# Listar apenas secrets persistentes
persistent_secrets = con.execute("""
    SELECT name, type, persistent
    FROM duckdb_secrets()
    WHERE persistent = true
""").df()

print(f"\nSecrets persistentes: {len(persistent_secrets)}")

# Exemplo/Bloco 10
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar secret
con.execute("CREATE SECRET temp_secret (TYPE s3, KEY_ID 'k', SECRET 's')")

# Verificar
print("Antes de deletar:")
print(con.execute("SELECT name FROM duckdb_secrets()").df())

# Deletar secret
con.execute("DROP SECRET temp_secret")

# Verificar novamente
print("\nDepois de deletar:")
print(con.execute("SELECT name FROM duckdb_secrets()").df())

# Exemplo/Bloco 11
import duckdb

con = duckdb.connect()

# Deletar secret que pode não existir
con.execute("DROP SECRET IF EXISTS non_existent_secret")
print("Comando executado sem erro (mesmo se secret não existia)")

# Sem IF EXISTS causaria erro se secret não existir
# con.execute("DROP SECRET non_existent_secret")  # ❌ Erro!

# Exemplo/Bloco 12
import duckdb

# Temporary secrets existem apenas durante a sessão
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar temporary secret (padrão)
con.execute("""
    CREATE SECRET temp_s3 (
        TYPE s3,
        KEY_ID 'temporary_key',
        SECRET 'temporary_secret'
    )
""")

# Verificar
secrets = con.execute("""
    SELECT name, persistent, storage
    FROM duckdb_secrets()
""").df()

print("Secret temporário:")
print(secrets)

# Fechar conexão
con.close()

# Reabrir - secret não existe mais
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

secrets_after = con.execute("SELECT name FROM duckdb_secrets()").df()
print(f"\nSecrets após reconectar: {len(secrets_after)}")

# Exemplo/Bloco 13
import duckdb

# Persistent secrets são salvos em disco
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar persistent secret
con.execute("""
    CREATE PERSISTENT SECRET persistent_s3 (
        TYPE s3,
        KEY_ID 'persistent_key',
        SECRET 'persistent_secret'
    )
""")

# Verificar
secrets = con.execute("""
    SELECT name, persistent, storage
    FROM duckdb_secrets()
""").df()

print("Secret persistente:")
print(secrets)

# Fechar e reabrir
con.close()
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secret ainda existe!
secrets_after = con.execute("""
    SELECT name FROM duckdb_secrets()
""").df()

print(f"\nSecrets após reconectar: {len(secrets_after)}")
print(secrets_after)

# Limpeza
con.execute("DROP PERSISTENT SECRET persistent_s3")

# Exemplo/Bloco 14
# 1. Crie 3 secrets do tipo S3 com nomes diferentes
# 2. Liste todos os secrets criados
# 3. Delete um dos secrets
# 4. Liste novamente para confirmar

# Sua solução aqui

# Exemplo/Bloco 15
# 1. Crie um secret temporário
# 2. Crie um secret persistente
# 3. Liste ambos e compare o campo 'persistent'
# 4. Feche a conexão e reabra
# 5. Verifique quais secrets ainda existem
# 6. Delete o secret persistente

# Sua solução aqui

# Exemplo/Bloco 16
# 1. Crie um secret do tipo HTTP com um bearer token
# 2. Verifique se foi criado corretamente
# 3. Use which_secret() para verificar qual secret seria usado
#    para uma URL 'https://api.example.com/data'

# Sua solução aqui

