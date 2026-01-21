# -*- coding: utf-8 -*-
"""
Secrets-03-cloud-storage-secrets
"""

# Secrets-03-cloud-storage-secrets
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secret S3 com TODOS os parâmetros disponíveis
con.execute("""
    CREATE SECRET s3_complete (
        TYPE s3,
        -- Credenciais
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        SESSION_TOKEN 'optional_session_token_for_temporary_credentials',

        -- Região e Endpoint
        REGION 'us-east-1',
        ENDPOINT 's3.amazonaws.com',

        -- Estilo de URL
        URL_STYLE 'vhost',  -- 'vhost' ou 'path'

        -- SSL/TLS
        USE_SSL true,

        -- Timeout e Performance
        TIMEOUT 60000,  -- em milissegundos

        -- Scope (opcional)
        SCOPE 's3://my-specific-bucket/'
    )
""")

print("Secret S3 completo criado!")

# Verificar
info = con.execute("""
    SELECT name, type, scope, provider
    FROM duckdb_secrets()
    WHERE name = 's3_complete'
""").df()

print("\nInformações do secret:")
print(info)

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Virtual-hosted style (padrão AWS)
# URL: https://bucket.s3.amazonaws.com/key
con.execute("""
    CREATE SECRET s3_vhost (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        URL_STYLE 'vhost'
    )
""")

# Path style
# URL: https://s3.amazonaws.com/bucket/key
con.execute("""
    CREATE SECRET s3_path (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        URL_STYLE 'path'
    )
""")

print("""
URL Styles:
┌─────────────────────────────────────────────────────────┐
│ vhost (Virtual-hosted):                                 │
│   https://bucket.s3.amazonaws.com/key                   │
│   ✓ Padrão AWS                                          │
│   ✓ Recomendado para S3                                 │
│                                                          │
│ path:                                                    │
│   https://s3.amazonaws.com/bucket/key                   │
│   ✓ Requerido para MinIO, alguns S3 compatíveis        │
│   ✓ Usado em endpoints personalizados                  │
└─────────────────────────────────────────────────────────┘
""")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Credenciais temporárias com session token
con.execute("""
    CREATE SECRET s3_temporary (
        TYPE s3,
        KEY_ID 'ASIATEMP...',
        SECRET 'temporary_secret',
        SESSION_TOKEN 'FwoGZXIvYXdzE...',
        REGION 'us-east-1'
    )
""")

print("Secret com credenciais temporárias (STS) criado!")
print("Válido apenas durante a sessão do token")

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Usar credential chain do AWS
con.execute("""
    CREATE SECRET s3_auto (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'env;config;sts;instance_metadata'
    )
""")

print("""
Credential Chain configurada!

Ordem de busca de credenciais:
1. env              → AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
2. config           → ~/.aws/credentials, ~/.aws/config
3. sts              → Assume Role via STS
4. instance_metadata → EC2 instance metadata (IAM role)

DuckDB tentará cada método até encontrar credenciais válidas.
""")

# Exemplo/Bloco 5
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secret para bucket de produção
con.execute("""
    CREATE SECRET s3_prod (
        TYPE s3,
        KEY_ID 'PROD_KEY',
        SECRET 'PROD_SECRET',
        REGION 'us-east-1',
        SCOPE 's3://production-bucket/'
    )
""")

# Secret para bucket de desenvolvimento
con.execute("""
    CREATE SECRET s3_dev (
        TYPE s3,
        KEY_ID 'DEV_KEY',
        SECRET 'DEV_SECRET',
        REGION 'us-west-2',
        SCOPE 's3://development-bucket/'
    )
""")

# Secret para bucket de analytics
con.execute("""
    CREATE SECRET s3_analytics (
        TYPE s3,
        KEY_ID 'ANALYTICS_KEY',
        SECRET 'ANALYTICS_SECRET',
        REGION 'eu-west-1',
        SCOPE 's3://analytics-bucket/'
    )
""")

# Verificar qual secret será usado para cada URL
print("Verificando secrets para diferentes buckets:\n")

for bucket in ['production-bucket', 'development-bucket', 'analytics-bucket']:
    url = f's3://{bucket}/data.parquet'
    result = con.execute(f"""
        SELECT name, scope FROM which_secret('{url}', 's3')
    """).fetchone()
    print(f"{bucket:20} → Secret: {result[0]}")

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# MinIO local
con.execute("""
    CREATE SECRET minio_local (
        TYPE s3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT 'localhost:9000',
        USE_SSL false,
        URL_STYLE 'path'
    )
""")

# MinIO remoto com SSL
con.execute("""
    CREATE SECRET minio_remote (
        TYPE s3,
        KEY_ID 'minio_access_key',
        SECRET 'minio_secret_key',
        ENDPOINT 'minio.example.com',
        USE_SSL true,
        URL_STYLE 'path',
        REGION 'us-east-1'
    )
""")

print("Secrets MinIO criados!")

# Exemplo de uso
# result = con.execute("""
#     SELECT * FROM 's3://my-bucket/data.parquet'
# """).df()

# Exemplo/Bloco 7
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

print("""
Métodos de Autenticação Azure:
┌──────────────────────────────────────────────────────────┐
│ 1. Connection String                                     │
│    - Método mais simples                                 │
│    - Contém todas as informações necessárias             │
│                                                          │
│ 2. Account Name + Account Key                           │
│    - Separação de nome e chave                          │
│    - Mais flexível                                       │
│                                                          │
│ 3. Service Principal (AAD)                              │
│    - Autenticação via Azure Active Directory            │
│    - Recomendado para produção                          │
│                                                          │
│ 4. Managed Identity                                      │
│    - Para recursos Azure (VMs, Functions)               │
│    - Sem credenciais explícitas                         │
│                                                          │
│ 5. Credential Chain                                      │
│    - Tenta múltiplos métodos automaticamente            │
│    - Mais conveniente para desenvolvimento              │
└──────────────────────────────────────────────────────────┘
""")

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Connection string completa
con.execute("""
    CREATE SECRET azure_conn (
        TYPE azure,
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=mystorageaccount;AccountKey=myaccountkey==;EndpointSuffix=core.windows.net'
    )
""")

print("Secret Azure com connection string criado!")

# Com escopo específico
con.execute("""
    CREATE SECRET azure_container (
        TYPE azure,
        CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=key==;EndpointSuffix=core.windows.net',
        SCOPE 'azure://mycontainer/'
    )
""")

print("Secret com escopo para container específico criado!")

# Exemplo/Bloco 9
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Account name e key separados
con.execute("""
    CREATE SECRET azure_key (
        TYPE azure,
        ACCOUNT_NAME 'mystorageaccount',
        ACCOUNT_KEY 'abcdefghijklmnopqrstuvwxyz0123456789+/=='
    )
""")

print("Secret Azure com account key criado!")

# Com endpoint customizado
con.execute("""
    CREATE SECRET azure_custom_endpoint (
        TYPE azure,
        ACCOUNT_NAME 'mystorageaccount',
        ACCOUNT_KEY 'mykey==',
        ENDPOINT 'https://mystorageaccount.blob.core.windows.net'
    )
""")

print("Secret com endpoint customizado criado!")

# Exemplo/Bloco 10
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Service Principal (Azure AD)
con.execute("""
    CREATE SECRET azure_sp (
        TYPE azure,
        PROVIDER service_principal,
        TENANT_ID '00000000-0000-0000-0000-000000000000',
        CLIENT_ID '11111111-1111-1111-1111-111111111111',
        CLIENT_SECRET 'client_secret_value',
        ACCOUNT_NAME 'mystorageaccount'
    )
""")

print("""
Secret com Service Principal criado!

Vantagens do Service Principal:
✓ Autenticação via Azure Active Directory
✓ Suporte a RBAC (Role-Based Access Control)
✓ Rotação de credenciais mais fácil
✓ Auditoria melhorada
✓ Recomendado para ambientes de produção
""")

# Exemplo/Bloco 11
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Managed Identity (para recursos Azure)
con.execute("""
    CREATE SECRET azure_managed (
        TYPE azure,
        PROVIDER managed_identity,
        ACCOUNT_NAME 'mystorageaccount'
    )
""")

print("""
Secret com Managed Identity criado!

Uso:
- Para VMs Azure
- Para Azure Functions
- Para Azure Container Instances
- Não requer credenciais explícitas
- Usa identidade atribuída ao recurso Azure
""")

# Exemplo/Bloco 12
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Credential chain
con.execute("""
    CREATE SECRET azure_chain (
        TYPE azure,
        PROVIDER credential_chain,
        ACCOUNT_NAME 'mystorageaccount',
        CHAIN 'env;managed_identity;cli;service_principal'
    )
""")

print("""
Azure Credential Chain configurada!

Ordem de busca:
1. env               → Variáveis de ambiente
2. managed_identity  → Managed Identity do recurso Azure
3. cli               → Azure CLI (az login)
4. service_principal → Service Principal configurado

Útil para:
✓ Desenvolvimento local (usa Azure CLI)
✓ Produção (usa Managed Identity)
✓ CI/CD (usa Service Principal)
""")

# Exemplo/Bloco 13
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Service Account com arquivo JSON
con.execute("""
    CREATE SECRET gcs_sa_file (
        TYPE gcs,
        KEY_ID 'service-account@project.iam.gserviceaccount.com',
        SECRET '/path/to/service-account-key.json'
    )
""")

print("""
Secret GCS com Service Account criado!

Arquivo JSON contém:
- type: "service_account"
- project_id
- private_key_id
- private_key
- client_email
- client_id
- auth_uri, token_uri

Para criar:
gcloud iam service-accounts keys create key.json \\
  --iam-account=sa-name@project.iam.gserviceaccount.com
""")

# Exemplo/Bloco 14
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Service Account com JSON inline (útil para CI/CD)
service_account_json = """{
  "type": "service_account",
  "project_id": "my-project",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
  "client_email": "service-account@project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}"""

con.execute(f"""
    CREATE SECRET gcs_sa_json (
        TYPE gcs,
        KEY_ID 'service-account@project.iam.gserviceaccount.com',
        SECRET '{service_account_json}'
    )
""")

print("Secret GCS com JSON inline criado!")

# Exemplo/Bloco 15
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Access token (temporário)
con.execute("""
    CREATE SECRET gcs_token (
        TYPE gcs,
        ACCESS_TOKEN 'ya29.c.Kl6iB...'
    )
""")

print("""
Secret GCS com access token criado!

IMPORTANTE:
- Access tokens expiram (geralmente 1 hora)
- Use apenas para testes ou sessões curtas
- Para uso prolongado, use Service Account

Para gerar access token:
gcloud auth application-default print-access-token
""")

# Exemplo/Bloco 16
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Application Default Credentials (ADC)
con.execute("""
    CREATE SECRET gcs_adc (
        TYPE gcs,
        PROVIDER credential_chain
    )
""")

print("""
Secret GCS com ADC criado!

Application Default Credentials busca credenciais na ordem:
1. GOOGLE_APPLICATION_CREDENTIALS environment variable
2. gcloud auth application-default login
3. Compute Engine metadata service
4. App Engine metadata service
5. Cloud Run metadata service

Setup ADC:
gcloud auth application-default login
""")

# Exemplo/Bloco 17
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Diferentes service accounts para diferentes buckets
con.execute("""
    CREATE SECRET gcs_prod (
        TYPE gcs,
        KEY_ID 'prod-sa@project.iam.gserviceaccount.com',
        SECRET '/path/to/prod-key.json',
        SCOPE 'gs://production-bucket/'
    )
""")

con.execute("""
    CREATE SECRET gcs_dev (
        TYPE gcs,
        KEY_ID 'dev-sa@project.iam.gserviceaccount.com',
        SECRET '/path/to/dev-key.json',
        SCOPE 'gs://development-bucket/'
    )
""")

print("Secrets GCS com escopos diferentes criados!")

# Verificar qual secret será usado
prod_secret = con.execute("""
    SELECT name FROM which_secret('gs://production-bucket/file.parquet', 'gcs')
""").fetchone()

dev_secret = con.execute("""
    SELECT name FROM which_secret('gs://development-bucket/file.parquet', 'gcs')
""").fetchone()

print(f"\nProduction bucket usa: {prod_secret[0]}")
print(f"Development bucket usa: {dev_secret[0]}")

# Exemplo/Bloco 18
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Service Account Impersonation
con.execute("""
    CREATE SECRET gcs_impersonate (
        TYPE gcs,
        KEY_ID 'source-sa@project.iam.gserviceaccount.com',
        SECRET '/path/to/source-key.json',
        IMPERSONATE_SERVICE_ACCOUNT 'target-sa@project.iam.gserviceaccount.com'
    )
""")

print("""
Secret GCS com impersonation criado!

Use cases:
- Delegação de privilégios
- Cross-project access
- Separação de ambientes

Requer:
- source-sa precisa ter permissão iam.serviceAccountTokenCreator
  no target-sa
""")

# Exemplo/Bloco 19
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# R2 secret básico
con.execute("""
    CREATE SECRET r2_basic (
        TYPE r2,
        KEY_ID 'r2_access_key_id',
        SECRET 'r2_secret_access_key',
        ACCOUNT_ID 'cloudflare_account_id'
    )
""")

print("""
Secret Cloudflare R2 criado!

Para obter credenciais:
1. Acesse Cloudflare Dashboard
2. R2 > Manage R2 API Tokens
3. Create API Token
4. Copie Access Key ID e Secret Access Key
5. Account ID está no dashboard
""")

# Exemplo/Bloco 20
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# R2 com endpoint explícito
con.execute("""
    CREATE SECRET r2_custom (
        TYPE r2,
        KEY_ID 'access_key',
        SECRET 'secret_key',
        ACCOUNT_ID 'account_id',
        ENDPOINT 'https://account_id.r2.cloudflarestorage.com'
    )
""")

print("Secret R2 com endpoint customizado criado!")

# Exemplo/Bloco 21
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# R2 com diferentes tokens para diferentes buckets
con.execute("""
    CREATE SECRET r2_public (
        TYPE r2,
        KEY_ID 'public_key',
        SECRET 'public_secret',
        ACCOUNT_ID 'account_id',
        SCOPE 'r2://public-data/'
    )
""")

con.execute("""
    CREATE SECRET r2_private (
        TYPE r2,
        KEY_ID 'private_key',
        SECRET 'private_secret',
        ACCOUNT_ID 'account_id',
        SCOPE 'r2://private-data/'
    )
""")

print("Secrets R2 com escopos diferentes criados!")

# Exemplo/Bloco 22
import duckdb

print("""
Cloudflare R2 vs AWS S3:
┌──────────────────────────┬─────────────────┬─────────────────┐
│ Característica           │ R2              │ S3              │
├──────────────────────────┼─────────────────┼─────────────────┤
│ API Compatível           │ S3 API          │ S3 API nativo   │
│ Egress Fees              │ Zero            │ Pagos           │
│ Global Distribution      │ Sim             │ Regional        │
│ URL Style                │ Path-style      │ Vhost/Path      │
│ Authentication           │ Access Key/Secret│ IAM, Keys, etc. │
│ Credential Chain         │ Não             │ Sim             │
└──────────────────────────┴─────────────────┴─────────────────┘

Quando usar R2:
✓ Alto volume de downloads (sem egress fees)
✓ Distribuição global de dados
✓ Compatibilidade com S3 API
✓ Alternativa econômica ao S3

Quando usar S3:
✓ Integração profunda com AWS
✓ Necessita features específicas AWS
✓ Usa IAM roles e policies complexas
✓ Workloads já em AWS
""")

# Exemplo/Bloco 23
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# S3 com configurações de performance
con.execute("""
    CREATE SECRET s3_optimized (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        REGION 'us-east-1',
        TIMEOUT 120000,  -- 2 minutos
        RETRIES 5
    )
""")

print("""
Configurações de Performance:

TIMEOUT (milissegundos):
- Padrão: 30000 (30 segundos)
- Aumentar para arquivos grandes
- Considerar latência da rede

RETRIES:
- Padrão: 3
- Aumentar para redes instáveis
- Pode aumentar tempo total de operação

Recomendações:
┌──────────────────────┬─────────────┬──────────┐
│ Cenário              │ Timeout (ms)│ Retries  │
├──────────────────────┼─────────────┼──────────┤
│ Arquivos pequenos    │    30000    │    3     │
│ Arquivos médios      │    60000    │    3     │
│ Arquivos grandes     │   120000    │    5     │
│ Rede instável        │    90000    │    7     │
│ Produção crítica     │   180000    │   10     │
└──────────────────────┴─────────────┴──────────┘
""")

# Exemplo/Bloco 24
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# S3 com SSL desabilitado (apenas desenvolvimento local)
con.execute("""
    CREATE SECRET s3_no_ssl (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        ENDPOINT 'localhost:9000',
        USE_SSL false
    )
""")

# S3 com SSL habilitado (produção)
con.execute("""
    CREATE SECRET s3_ssl (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        ENDPOINT 's3.amazonaws.com',
        USE_SSL true
    )
""")

print("""
SSL/TLS Configuration:

USE_SSL = false:
❌ Apenas para desenvolvimento local
❌ Dados não criptografados em trânsito
✓ Menor overhead (MinIO local)

USE_SSL = true:
✓ Obrigatório para produção
✓ Dados criptografados
✓ Compliance (GDPR, HIPAA, etc.)
✓ Man-in-the-middle protection
""")

# Exemplo/Bloco 25
# 1. Crie secrets para S3 em 3 regiões diferentes (us-east-1, eu-west-1, ap-southeast-1)
# 2. Use SCOPEs para direcionar cada região a um bucket específico
# 3. Verifique com which_secret() qual secret seria usado para cada bucket
# 4. Liste todos os secrets e compare as configurações

# Sua solução aqui

# Exemplo/Bloco 26
# 1. Crie um secret Azure com connection string
# 2. Crie um secret Azure com account key
# 3. Crie um secret Azure com credential chain
# 4. Liste todos e compare os providers
# 5. Delete todos os secrets Azure

# Sua solução aqui

# Exemplo/Bloco 27
# 1. Crie um secret GCS com service account para bucket de produção
# 2. Crie um secret GCS com service account para bucket de staging
# 3. Use SCOPE para separar os ambientes
# 4. Verifique qual secret seria usado para cada ambiente
# 5. Teste criar um secret duplicado com IF NOT EXISTS

# Sua solução aqui

# Exemplo/Bloco 28
# 1. Crie secrets para S3, Azure, GCS e R2 com nomes padronizados
# 2. Liste todos usando duckdb_secrets()
# 3. Crie uma tabela comparativa com os campos: name, type, provider, scope
# 4. Identifique quais suportam credential_chain
# 5. Delete todos os secrets criados

# Sua solução aqui

