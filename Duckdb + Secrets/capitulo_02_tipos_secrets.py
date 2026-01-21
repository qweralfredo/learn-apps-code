# -*- coding: utf-8 -*-
"""
Secrets-02-tipos-secrets
"""

# Secrets-02-tipos-secrets
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()

# Instalar extensions necessárias
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL azure; LOAD azure;")
con.execute("INSTALL mysql_scanner; LOAD mysql_scanner;")
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

# Criar um secret de cada tipo para demonstração
print("Tipos de secrets suportados:\n")

types_info = [
    ("s3", "AWS S3 e compatíveis (MinIO, Wasabi, etc.)"),
    ("r2", "Cloudflare R2 Storage"),
    ("gcs", "Google Cloud Storage"),
    ("azure", "Azure Blob Storage"),
    ("mysql", "MySQL/MariaDB databases"),
    ("postgres", "PostgreSQL databases"),
    ("http", "APIs HTTP/HTTPS"),
    ("huggingface", "Hugging Face Hub"),
    ("iceberg", "Apache Iceberg REST Catalog")
]

for type_name, description in types_info:
    print(f"  {type_name:12} - {description}")

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secret S3 completo
con.execute("""
    CREATE SECRET my_s3 (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1',
        SESSION_TOKEN 'optional_session_token',
        ENDPOINT 's3.amazonaws.com',
        URL_STYLE 'vhost',
        USE_SSL true
    )
""")

print("Secret S3 criado com sucesso!")

# Verificar parâmetros
info = con.execute("""
    SELECT name, type, provider, scope
    FROM duckdb_secrets()
    WHERE name = 'my_s3'
""").df()

print("\nInformações do secret:")
print(info)

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# MinIO
con.execute("""
    CREATE SECRET minio_secret (
        TYPE s3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT 'localhost:9000',
        USE_SSL false,
        URL_STYLE 'path'
    )
""")

# Wasabi
con.execute("""
    CREATE SECRET wasabi_secret (
        TYPE s3,
        KEY_ID 'WASABI_KEY_ID',
        SECRET 'WASABI_SECRET',
        ENDPOINT 's3.wasabisys.com',
        REGION 'us-east-1',
        USE_SSL true
    )
""")

# Listar secrets criados
secrets = con.execute("""
    SELECT name, type, scope
    FROM duckdb_secrets()
    WHERE type = 's3'
""").df()

print("Secrets S3-compatíveis:")
print(secrets)

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Usar AWS credential chain (variáveis de ambiente, IAM, etc.)
con.execute("""
    CREATE SECRET s3_chain (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'env;config;sts'
    )
""")

print("Secret com credential chain criado!")
print("DuckDB tentará obter credenciais de:")
print("  1. Variáveis de ambiente (AWS_ACCESS_KEY_ID, etc.)")
print("  2. Arquivo ~/.aws/config")
print("  3. STS (Security Token Service)")

# Exemplo/Bloco 5
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Cloudflare R2
con.execute("""
    CREATE SECRET cloudflare_r2 (
        TYPE r2,
        KEY_ID 'your_r2_access_key_id',
        SECRET 'your_r2_secret_access_key',
        ACCOUNT_ID 'your_cloudflare_account_id'
    )
""")

print("Secret Cloudflare R2 criado!")

# Verificar
info = con.execute("""
    SELECT name, type, provider
    FROM duckdb_secrets()
    WHERE type = 'r2'
""").df()

print("\nR2 Secrets:")
print(info)

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# R2 com endpoint específico
con.execute("""
    CREATE SECRET r2_custom (
        TYPE r2,
        KEY_ID 'r2_key',
        SECRET 'r2_secret',
        ACCOUNT_ID 'account_id',
        ENDPOINT 'https://account_id.r2.cloudflarestorage.com'
    )
""")

print("Secret R2 com endpoint customizado criado!")

# Exemplo/Bloco 7
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# GCS com service account key
con.execute("""
    CREATE SECRET gcs_service_account (
        TYPE gcs,
        KEY_ID 'service-account@project.iam.gserviceaccount.com',
        SECRET 'path/to/service-account-key.json'
    )
""")

print("Secret GCS com service account criado!")

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# GCS com access token
con.execute("""
    CREATE SECRET gcs_token (
        TYPE gcs,
        ACCESS_TOKEN 'ya29.c.Kl6iB...'
    )
""")

print("Secret GCS com access token criado!")
print("Nota: Access tokens expiram, use service accounts para uso persistente")

# Exemplo/Bloco 9
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# GCS usando credential chain (Application Default Credentials)
con.execute("""
    CREATE SECRET gcs_default (
        TYPE gcs,
        PROVIDER credential_chain
    )
""")

print("Secret GCS com credential chain criado!")
print("Usará Application Default Credentials do Google Cloud SDK")

# Exemplo/Bloco 10
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Azure com connection string
con.execute("""
    CREATE SECRET azure_conn_string (
        TYPE azure,
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net'
    )
""")

print("Secret Azure com connection string criado!")

# Exemplo/Bloco 11
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Azure com account key
con.execute("""
    CREATE SECRET azure_key (
        TYPE azure,
        ACCOUNT_NAME 'mystorageaccount',
        ACCOUNT_KEY 'base64_encoded_account_key_here'
    )
""")

print("Secret Azure com account key criado!")

# Exemplo/Bloco 12
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Azure com service principal
con.execute("""
    CREATE SECRET azure_sp (
        TYPE azure,
        PROVIDER service_principal,
        TENANT_ID 'tenant-id-here',
        CLIENT_ID 'client-id-here',
        CLIENT_SECRET 'client-secret-here',
        ACCOUNT_NAME 'mystorageaccount'
    )
""")

print("Secret Azure com service principal criado!")

# Exemplo/Bloco 13
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# HTTP com bearer token
con.execute("""
    CREATE SECRET api_bearer (
        TYPE http,
        BEARER_TOKEN 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...'
    )
""")

print("Secret HTTP com bearer token criado!")

# Exemplo de uso (teórico)
# result = con.execute("""
#     SELECT * FROM read_json('https://api.example.com/data')
# """).df()

# Exemplo/Bloco 14
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# HTTP com basic auth
con.execute("""
    CREATE SECRET api_basic (
        TYPE http,
        USERNAME 'api_user',
        PASSWORD 'api_password'
    )
""")

print("Secret HTTP com basic auth criado!")

# Exemplo/Bloco 15
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# HTTP com headers personalizados
con.execute("""
    CREATE SECRET api_headers (
        TYPE http,
        HEADERS MAP {
            'Authorization': 'Bearer token123',
            'X-API-Key': 'my_api_key',
            'User-Agent': 'DuckDB/1.0'
        }
    )
""")

print("Secret HTTP com headers customizados criado!")

# Exemplo/Bloco 16
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Hugging Face com token
con.execute("""
    CREATE SECRET huggingface_token (
        TYPE huggingface,
        TOKEN 'hf_...'
    )
""")

print("Secret Hugging Face criado!")

# Usar para acessar datasets privados
# result = con.execute("""
#     SELECT * FROM read_parquet('hf://datasets/username/dataset/file.parquet')
# """).df()

# Exemplo/Bloco 17
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Hugging Face com endpoint customizado
con.execute("""
    CREATE SECRET hf_custom (
        TYPE huggingface,
        TOKEN 'hf_token_here',
        ENDPOINT 'https://huggingface.co'
    )
""")

print("Secret Hugging Face com endpoint customizado criado!")

# Exemplo/Bloco 18
import duckdb

con = duckdb.connect()
con.execute("INSTALL mysql_scanner; LOAD mysql_scanner;")

# MySQL secret
con.execute("""
    CREATE SECRET mysql_db (
        TYPE mysql,
        HOST 'localhost',
        PORT 3306,
        DATABASE 'mydb',
        USER 'myuser',
        PASSWORD 'mypassword'
    )
""")

print("Secret MySQL criado!")

# Exemplo/Bloco 19
import duckdb

con = duckdb.connect()
con.execute("INSTALL mysql_scanner; LOAD mysql_scanner;")

# MySQL com SSL
con.execute("""
    CREATE SECRET mysql_ssl (
        TYPE mysql,
        HOST 'mysql.example.com',
        PORT 3306,
        DATABASE 'production',
        USER 'app_user',
        PASSWORD 'secure_password',
        SSL_MODE 'REQUIRED',
        SSL_CA '/path/to/ca-cert.pem',
        SSL_CERT '/path/to/client-cert.pem',
        SSL_KEY '/path/to/client-key.pem'
    )
""")

print("Secret MySQL com SSL criado!")

# Exemplo/Bloco 20
import duckdb

con = duckdb.connect()
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

# PostgreSQL secret
con.execute("""
    CREATE SECRET postgres_db (
        TYPE postgres,
        HOST 'localhost',
        PORT 5432,
        DATABASE 'mydb',
        USER 'postgres',
        PASSWORD 'password'
    )
""")

print("Secret PostgreSQL criado!")

# Exemplo/Bloco 21
import duckdb

con = duckdb.connect()
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

# PostgreSQL com connection string
con.execute("""
    CREATE SECRET postgres_uri (
        TYPE postgres,
        CONNECTION_STRING 'postgresql://user:password@localhost:5432/dbname?sslmode=require'
    )
""")

print("Secret PostgreSQL com connection string criado!")

# Exemplo/Bloco 22
import duckdb

con = duckdb.connect()
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

# PostgreSQL com SSL
con.execute("""
    CREATE SECRET postgres_ssl (
        TYPE postgres,
        HOST 'postgres.example.com',
        PORT 5432,
        DATABASE 'production',
        USER 'app_user',
        PASSWORD 'secure_password',
        SSLMODE 'require',
        SSLROOTCERT '/path/to/root.crt',
        SSLCERT '/path/to/client.crt',
        SSLKEY '/path/to/client.key'
    )
""")

print("Secret PostgreSQL com SSL criado!")

# Exemplo/Bloco 23
import duckdb
import pandas as pd

# Criar tabela comparativa
comparison = pd.DataFrame({
    'Tipo': ['s3', 'r2', 'gcs', 'azure', 'http', 'huggingface', 'mysql', 'postgres'],
    'Uso Principal': [
        'Object Storage',
        'Object Storage',
        'Object Storage',
        'Blob Storage',
        'APIs REST',
        'ML Datasets',
        'RDBMS',
        'RDBMS'
    ],
    'Extension Necessária': [
        'httpfs',
        'httpfs',
        'httpfs',
        'azure',
        'httpfs',
        'httpfs',
        'mysql_scanner',
        'postgres_scanner'
    ],
    'Credential Chain': [
        'Sim',
        'Não',
        'Sim',
        'Sim',
        'Não',
        'Não',
        'Não',
        'Não'
    ]
})

print("Comparação de Tipos de Secrets:")
print(comparison.to_string(index=False))

# Exemplo/Bloco 24
import duckdb

# Mostrar recursos por tipo
print("""
┌──────────────┬──────────┬──────────┬──────────┬────────────┐
│ Tipo         │ Múltiplos│ Scope    │ Persistent│ Provider   │
│              │ Secrets  │ Support  │ Support   │ Options    │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ s3           │    ✓     │    ✓     │    ✓      │ config,    │
│              │          │          │           │ credential │
│              │          │          │           │ chain      │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ r2           │    ✓     │    ✓     │    ✓      │ config     │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ gcs          │    ✓     │    ✓     │    ✓      │ config,    │
│              │          │          │           │ credential │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ azure        │    ✓     │    ✓     │    ✓      │ config,    │
│              │          │          │           │ managed_id,│
│              │          │          │           │ service_pr │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ http         │    ✓     │    ✓     │    ✓      │ config     │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ huggingface  │    ✓     │    ✓     │    ✓      │ config     │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ mysql        │    ✓     │    ✓     │    ✓      │ config     │
├──────────────┼──────────┼──────────┼──────────┼────────────┤
│ postgres     │    ✓     │    ✓     │    ✓      │ config     │
└──────────────┴──────────┴──────────┴──────────┴────────────┘
""")

# Exemplo/Bloco 25
# 1. Crie um secret S3 com todos os parâmetros principais
# 2. Crie um secret HTTP com bearer token
# 3. Crie um secret MySQL com configuração básica
# 4. Liste todos os secrets criados agrupados por tipo
# 5. Verifique qual extension cada secret requer

# Sua solução aqui

# Exemplo/Bloco 26
# 1. Crie secrets para S3, R2, GCS e Azure
# 2. Use nomes diferentes mas do mesmo padrão (ex: storage_s3, storage_r2)
# 3. Liste todos e compare os campos 'type' e 'provider'
# 4. Delete todos os secrets criados

# Sua solução aqui

# Exemplo/Bloco 27
# 1. Crie um secret MySQL com SSL habilitado
# 2. Crie um secret PostgreSQL com connection string
# 3. Liste ambos e verifique se foram criados corretamente
# 4. Tente criar um secret MySQL duplicado com IF NOT EXISTS
# 5. Verifique que o secret original não foi substituído

# Sua solução aqui

# Exemplo/Bloco 28
# 1. Crie 3 secrets HTTP diferentes:
#    - Um com bearer token
#    - Um com basic auth
#    - Um com headers customizados
# 2. Liste todos e compare os parâmetros
# 3. Use which_secret() para verificar qual seria usado para diferentes URLs
# 4. Delete todos os secrets HTTP

# Sua solução aqui

