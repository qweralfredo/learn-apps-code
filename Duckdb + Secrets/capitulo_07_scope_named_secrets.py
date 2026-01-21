# -*- coding: utf-8 -*-
"""
Secrets-07-scope-named-secrets
"""

# Secrets-07-scope-named-secrets
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

print("""
SCOPE em DuckDB Secrets:
════════════════════════════════════════════════════════

O que é:
────────
SCOPE define para quais URLs/paths um secret se aplica.
É um pattern matching de prefixo de URL.

Funcionamento:
──────────────
Quando você faz uma query para 's3://bucket1/file.parquet',
DuckDB procura o secret com SCOPE que melhor casa com a URL.

Matching:
─────────
1. Procura exact match
2. Procura longest prefix match
3. Usa secret sem SCOPE como fallback

Exemplos:
─────────
""")

# Secret sem SCOPE (aplica a todos os S3)
con.execute("""
    CREATE SECRET s3_default (
        TYPE s3,
        KEY_ID 'default_key',
        SECRET 'default_secret'
    )
""")

# Secret com SCOPE específico
con.execute("""
    CREATE SECRET s3_bucket1 (
        TYPE s3,
        KEY_ID 'bucket1_key',
        SECRET 'bucket1_secret',
        SCOPE 's3://bucket1/'
    )
""")

con.execute("""
    CREATE SECRET s3_bucket2 (
        TYPE s3,
        KEY_ID 'bucket2_key',
        SECRET 'bucket2_secret',
        SCOPE 's3://bucket2/'
    )
""")

# Verificar SCOPEs
secrets = con.execute("""
    SELECT name, scope
    FROM duckdb_secrets()
    WHERE type = 's3'
    ORDER BY name
""").df()

print("\nSecrets criados:")
print(secrets)

print("""
Matching de URLs:
─────────────────
's3://bucket1/file.parquet'  → s3_bucket1
's3://bucket2/data.csv'      → s3_bucket2
's3://bucket3/other.parquet' → s3_default (fallback)
's3://bucket1/deep/nested'   → s3_bucket1 (prefix match)
""")

con.close()

# Exemplo/Bloco 2
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Hierarquia de SCOPEs
con.execute("""
    CREATE SECRET s3_all (
        TYPE s3,
        KEY_ID 'all',
        SECRET 'all'
    )
""")

con.execute("""
    CREATE SECRET s3_prod_bucket (
        TYPE s3,
        KEY_ID 'prod',
        SECRET 'prod',
        SCOPE 's3://production-bucket/'
    )
""")

con.execute("""
    CREATE SECRET s3_prod_subfolder (
        TYPE s3,
        KEY_ID 'sensitive',
        SECRET 'sensitive',
        SCOPE 's3://production-bucket/sensitive-data/'
    )
""")

print("""
Hierarquia de SCOPEs:
════════════════════════════════════════════════════════

Secrets criados:
────────────────
1. s3_all                → SCOPE: (nenhum)
2. s3_prod_bucket        → SCOPE: s3://production-bucket/
3. s3_prod_subfolder     → SCOPE: s3://production-bucket/sensitive-data/

Matching (longest prefix wins):
────────────────────────────────
URL                                              → Secret usado
────────────────────────────────────────────────────────────────
s3://other-bucket/file.parquet                   → s3_all
s3://production-bucket/public/data.csv           → s3_prod_bucket
s3://production-bucket/sensitive-data/pii.parquet → s3_prod_subfolder
s3://production-bucket/sensitive-data/sub/file    → s3_prod_subfolder

Regras:
───────
✓ SCOPE mais específico (mais longo) tem prioridade
✓ Matching é por prefixo, não por regex
✓ Trailing slash recomendado mas não obrigatório
✓ Case-sensitive
""")

# Verificar matching
urls = [
    's3://other-bucket/file.parquet',
    's3://production-bucket/public/data.csv',
    's3://production-bucket/sensitive-data/pii.parquet'
]

print("\nVerificação com which_secret():\n")
for url in urls:
    result = con.execute(f"""
        SELECT name
        FROM which_secret('{url}', 's3')
    """).fetchone()

    secret_name = result[0] if result else 'None'
    print(f"{url:55} → {secret_name}")

con.close()

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar múltiplos secrets
con.execute("""
    CREATE SECRET s3_bucket_a (
        TYPE s3,
        KEY_ID 'key_a',
        SECRET 'secret_a',
        SCOPE 's3://bucket-a/'
    )
""")

con.execute("""
    CREATE SECRET s3_bucket_b (
        TYPE s3,
        KEY_ID 'key_b',
        SECRET 'secret_b',
        SCOPE 's3://bucket-b/'
    )
""")

# which_secret() básico
print("which_secret(url, type):\n")

result = con.execute("""
    SELECT * FROM which_secret('s3://bucket-a/file.parquet', 's3')
""").df()

print("Para 's3://bucket-a/file.parquet':")
print(result[['name', 'type', 'scope']])

print("""
which_secret() function:
════════════════════════════════════════════════════════

Sintaxe:
────────
which_secret(path: VARCHAR, type: VARCHAR) → TABLE

Parâmetros:
───────────
- path: URL ou path para verificar
- type: Tipo de secret (s3, azure, gcs, etc.)

Retorna:
────────
Tabela com informações do secret que será usado:
- name: Nome do secret
- type: Tipo do secret
- provider: Provider usado
- scope: SCOPE do secret
- ... outros campos

Uso:
────
✓ Debug: verificar qual secret será usado
✓ Testing: validar configuração de SCOPEs
✓ Documentation: listar mapeamentos URL → secret
""")

con.close()

# Exemplo/Bloco 4
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Setup complexo
con.execute("CREATE SECRET s3_default (TYPE s3, KEY_ID 'default', SECRET 'default')")
con.execute("CREATE SECRET s3_prod (TYPE s3, KEY_ID 'prod', SECRET 'prod', SCOPE 's3://prod/')")
con.execute("CREATE SECRET s3_analytics (TYPE s3, KEY_ID 'analytics', SECRET 'analytics', SCOPE 's3://prod/analytics/')")

# Testar múltiplas URLs
test_urls = [
    's3://dev-bucket/data.parquet',
    's3://prod/logs/2024/01/data.parquet',
    's3://prod/analytics/reports/summary.parquet',
    's3://prod/analytics/raw/events.parquet'
]

print("Verificação de Secrets para Múltiplas URLs:")
print("=" * 70)

for url in test_urls:
    result = con.execute(f"""
        SELECT name, scope
        FROM which_secret('{url}', 's3')
    """).fetchone()

    if result:
        secret_name, scope = result
        scope_display = scope if scope else '(global)'
        print(f"\nURL: {url}")
        print(f"  → Secret: {secret_name}")
        print(f"  → Scope: {scope_display}")
    else:
        print(f"\nURL: {url}")
        print(f"  → Nenhum secret encontrado!")

print("""
Análise:
────────
- s3://dev-bucket/           → s3_default (fallback global)
- s3://prod/logs/            → s3_prod (match 's3://prod/')
- s3://prod/analytics/*      → s3_analytics (longest match)

Hierarquia aplicada:
1. s3_analytics: 's3://prod/analytics/' (mais específico)
2. s3_prod: 's3://prod/' (intermediário)
3. s3_default: (nenhum scope) (fallback)
""")

con.close()

# Exemplo/Bloco 5
import duckdb

def diagnose_secret_configuration(con, urls, secret_type='s3'):
    """
    Diagnosticar configuração de secrets para múltiplas URLs
    """
    print(f"\nDiagnóstico de Secrets ({secret_type}):")
    print("=" * 80)

    # Listar todos os secrets do tipo
    all_secrets = con.execute(f"""
        SELECT name, scope
        FROM duckdb_secrets()
        WHERE type = '{secret_type}'
        ORDER BY LENGTH(scope) DESC, name
    """).df()

    print(f"\nSecrets configurados ({len(all_secrets)}):")
    for _, row in all_secrets.iterrows():
        scope = row['scope'] if row['scope'] else '(global)'
        print(f"  - {row['name']:20} → {scope}")

    # Testar cada URL
    print(f"\nTeste de URLs:")
    for url in urls:
        try:
            result = con.execute(f"""
                SELECT name, scope
                FROM which_secret('{url}', '{secret_type}')
            """).fetchone()

            if result:
                name, scope = result
                scope_display = scope if scope else '(global)'
                print(f"  ✓ {url:50} → {name} [{scope_display}]")
            else:
                print(f"  ✗ {url:50} → NENHUM SECRET")
        except Exception as e:
            print(f"  ✗ {url:50} → ERRO: {e}")

# Exemplo de uso
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Configurar secrets
con.execute("CREATE SECRET s3_public (TYPE s3, KEY_ID 'pub', SECRET 'pub', SCOPE 's3://public-data/')")
con.execute("CREATE SECRET s3_private (TYPE s3, KEY_ID 'priv', SECRET 'priv', SCOPE 's3://private-data/')")
con.execute("CREATE SECRET s3_backup (TYPE s3, KEY_ID 'backup', SECRET 'backup')")

# URLs para testar
urls_to_test = [
    's3://public-data/dataset.parquet',
    's3://private-data/sensitive.parquet',
    's3://unknown-bucket/file.parquet',
    's3://public-data/subfolder/nested.parquet'
]

diagnose_secret_configuration(con, urls_to_test, 's3')

con.close()

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

print("""
Cenário: Múltiplos Buckets S3 com Diferentes Credenciais
════════════════════════════════════════════════════════
""")

# Bucket público (credenciais de read-only)
con.execute("""
    CREATE SECRET s3_public_datasets (
        TYPE s3,
        KEY_ID 'AKIAPUBLIC',
        SECRET 'public_secret',
        REGION 'us-east-1',
        SCOPE 's3://company-public-datasets/'
    )
""")

# Bucket de analytics (credenciais de read/write)
con.execute("""
    CREATE SECRET s3_analytics (
        TYPE s3,
        KEY_ID 'AKIAANALYTICS',
        SECRET 'analytics_secret',
        REGION 'us-west-2',
        SCOPE 's3://company-analytics/'
    )
""")

# Bucket de ML (credenciais específicas com GPU)
con.execute("""
    CREATE SECRET s3_ml_models (
        TYPE s3,
        KEY_ID 'AKIAML',
        SECRET 'ml_secret',
        REGION 'eu-west-1',
        SCOPE 's3://company-ml-models/'
    )
""")

# Bucket de logs (write-only)
con.execute("""
    CREATE SECRET s3_logs (
        TYPE s3,
        KEY_ID 'AKIALOGS',
        SECRET 'logs_secret',
        REGION 'us-east-1',
        SCOPE 's3://company-logs/'
    )
""")

# Listar configuração
secrets = con.execute("""
    SELECT name, scope
    FROM duckdb_secrets()
    WHERE type = 's3'
    ORDER BY name
""").df()

print("\nConfiguração de Secrets:")
for _, row in secrets.iterrows():
    print(f"  {row['name']:25} → {row['scope']}")

print("""
Vantagens desta Abordagem:
──────────────────────────
✓ Privilégios mínimos (least privilege)
✓ Separação de concerns
✓ Diferentes regiões otimizadas
✓ Isolamento de credenciais
✓ Facilita rotação por bucket
✓ Auditoria granular

Queries Exemplo:
────────────────
-- Ler dados públicos
SELECT * FROM 's3://company-public-datasets/census/2020.parquet';

-- Análise
SELECT * FROM 's3://company-analytics/reports/monthly.parquet';

-- ML inference
SELECT * FROM 's3://company-ml-models/prod/model-v2.parquet';

-- Logs
COPY (SELECT * FROM events) TO 's3://company-logs/2024/01/events.parquet';
""")

con.close()

# Exemplo/Bloco 7
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

print("""
Cenário: Cross-Account S3 Access
════════════════════════════════════════════════════════

Empresa com múltiplas AWS accounts:
- Account A (111111111111): Production
- Account B (222222222222): Analytics
- Account C (333333333333): Partner
""")

# Account A - Production
con.execute("""
    CREATE SECRET s3_account_a (
        TYPE s3,
        KEY_ID 'AKIAACCOUNTA',
        SECRET 'account_a_secret',
        REGION 'us-east-1',
        SCOPE 's3://prod-account-a/'
    )
""")

# Account B - Analytics (com assume role)
con.execute("""
    CREATE SECRET s3_account_b (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'sts',
        ROLE_ARN 'arn:aws:iam::222222222222:role/AnalyticsRole',
        REGION 'us-west-2',
        SCOPE 's3://analytics-account-b/'
    )
""")

# Account C - Partner (read-only)
con.execute("""
    CREATE SECRET s3_account_c (
        TYPE s3,
        KEY_ID 'AKIAACCOUNTC',
        SECRET 'account_c_secret',
        REGION 'eu-west-1',
        SCOPE 's3://partner-account-c/'
    )
""")

print("""
Setup de Cross-Account:
───────────────────────

Account A → Account B (Assume Role):
1. No Account B, criar role com trust policy:
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": {"AWS": "arn:aws:iam::111111111111:user/duckdb-user"},
       "Action": "sts:AssumeRole"
     }]
   }

2. Attachar policy ao role:
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": ["s3:GetObject", "s3:ListBucket"],
       "Resource": [
         "arn:aws:s3:::analytics-account-b",
         "arn:aws:s3:::analytics-account-b/*"
       ]
     }]
   }

3. No Account A, dar permissão de AssumeRole ao usuário

4. Usar no DuckDB com STS credential chain

Query Cross-Account:
────────────────────
-- Account A
SELECT * FROM 's3://prod-account-a/data.parquet';

-- Account B (via assume role)
SELECT * FROM 's3://analytics-account-b/reports.parquet';

-- Account C
SELECT * FROM 's3://partner-account-c/shared.parquet';

-- JOIN entre accounts!
SELECT
    a.user_id,
    a.transactions,
    b.analytics_score
FROM 's3://prod-account-a/users.parquet' a
JOIN 's3://analytics-account-b/scores.parquet' b
  ON a.user_id = b.user_id;
""")

con.close()

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL azure; LOAD azure;")

print("""
Cenário: Multi-Cloud Storage
════════════════════════════════════════════════════════

Dados distribuídos em múltiplos cloud providers
""")

# AWS S3
con.execute("""
    CREATE SECRET aws_main (
        TYPE s3,
        KEY_ID 'AKIAAWS',
        SECRET 'aws_secret',
        REGION 'us-east-1',
        SCOPE 's3://company-data-aws/'
    )
""")

# Cloudflare R2
con.execute("""
    CREATE SECRET cloudflare_cdn (
        TYPE r2,
        KEY_ID 'r2_key',
        SECRET 'r2_secret',
        ACCOUNT_ID 'cf_account_id',
        SCOPE 'r2://company-cdn/'
    )
""")

# Google Cloud Storage
con.execute("""
    CREATE SECRET gcs_archive (
        TYPE gcs,
        PROVIDER credential_chain,
        SCOPE 'gs://company-archive-gcs/'
    )
""")

# Azure Blob Storage
con.execute("""
    CREATE SECRET azure_backup (
        TYPE azure,
        PROVIDER managed_identity,
        ACCOUNT_NAME 'companybackup',
        SCOPE 'azure://backups/'
    )
""")

secrets = con.execute("""
    SELECT name, type, scope
    FROM duckdb_secrets()
    ORDER BY type, name
""").df()

print("\nConfiguração Multi-Cloud:")
print(secrets.to_string(index=False))

print("""
Queries Multi-Cloud:
────────────────────
-- AWS S3 (production data)
SELECT * FROM 's3://company-data-aws/sales/2024.parquet';

-- Cloudflare R2 (CDN assets)
SELECT * FROM 'r2://company-cdn/images/catalog.parquet';

-- GCS (long-term archive)
SELECT * FROM 'gs://company-archive-gcs/historical/2020.parquet';

-- Azure (backups)
SELECT * FROM 'azure://backups/daily/backup-2024-01-20.parquet';

-- Cross-cloud analytics!
SELECT
    'AWS' as source, COUNT(*) as records
FROM 's3://company-data-aws/sales/2024.parquet'
UNION ALL
SELECT
    'GCS' as source, COUNT(*) as records
FROM 'gs://company-archive-gcs/historical/2020.parquet'
UNION ALL
SELECT
    'Azure' as source, COUNT(*) as records
FROM 'azure://backups/daily/backup-2024-01-20.parquet';

Vantagens Multi-Cloud:
──────────────────────
✓ Vendor lock-in mitigation
✓ Cost optimization (use cheapest for each use case)
✓ Geographic distribution
✓ Compliance (data residency requirements)
✓ Disaster recovery
✓ Best-of-breed services
""")

con.close()

# Exemplo/Bloco 9
import duckdb

print("""
Estratégias de Naming para Secrets:
════════════════════════════════════════════════════════

1. Por Ambiente:
   ─────────────
   - dev_s3_analytics
   - staging_s3_analytics
   - prod_s3_analytics

2. Por Projeto/Team:
   ──────────────────
   - s3_marketing_data
   - s3_engineering_logs
   - s3_finance_reports

3. Por Região:
   ───────────
   - s3_us_east_1
   - s3_eu_west_1
   - s3_ap_southeast_1

4. Por Função:
   ───────────
   - s3_readonly_public
   - s3_readwrite_analytics
   - s3_writeonly_logs

5. Hierárquico:
   ────────────
   - company_aws_s3_prod_analytics
   - company_gcp_gcs_dev_ml
   - company_azure_blob_staging_backup

Recomendações:
──────────────
✓ Usar snake_case
✓ Incluir provider no nome (s3_, gcs_, azure_)
✓ Ser descritivo mas conciso
✓ Consistente em todo o projeto
✓ Documentar convenção no README
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Exemplo de naming bem estruturado
secrets_config = [
    # Ambiente → Provider → Projeto → Região
    ("prod_s3_analytics_us", "s3://prod-analytics-us/", "us-east-1"),
    ("prod_s3_analytics_eu", "s3://prod-analytics-eu/", "eu-west-1"),
    ("staging_s3_analytics", "s3://staging-analytics/", "us-west-2"),
    ("dev_s3_analytics", "s3://dev-analytics/", "us-west-2"),
]

for name, scope, region in secrets_config:
    con.execute(f"""
        CREATE SECRET {name} (
            TYPE s3,
            KEY_ID 'key',
            SECRET 'secret',
            REGION '{region}',
            SCOPE '{scope}'
        )
    """)
    print(f"✓ Created: {name:30} → {scope}")

con.close()

# Exemplo/Bloco 10
import duckdb
import pandas as pd

def create_secret_inventory(con):
    """
    Criar inventário de secrets para documentação
    """
    secrets = con.execute("""
        SELECT
            name,
            type,
            provider,
            scope,
            persistent,
            storage
        FROM duckdb_secrets()
        ORDER BY type, name
    """).df()

    return secrets

def export_secret_documentation(secrets_df, filename='secrets_inventory.md'):
    """
    Exportar documentação de secrets
    """
    with open(filename, 'w') as f:
        f.write("# DuckDB Secrets Inventory\n\n")
        f.write(f"**Total Secrets:** {len(secrets_df)}\n\n")

        # Agrupar por tipo
        for secret_type in secrets_df['type'].unique():
            type_secrets = secrets_df[secrets_df['type'] == secret_type]
            f.write(f"## {secret_type.upper()} Secrets\n\n")

            for _, secret in type_secrets.iterrows():
                f.write(f"### {secret['name']}\n\n")
                f.write(f"- **Type:** {secret['type']}\n")
                f.write(f"- **Provider:** {secret['provider']}\n")
                f.write(f"- **Scope:** {secret['scope'] if secret['scope'] else '(global)'}\n")
                f.write(f"- **Persistent:** {secret['persistent']}\n")
                f.write(f"- **Storage:** {secret['storage']}\n\n")

    print(f"✓ Documentation exported to {filename}")

# Exemplo de uso
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar alguns secrets
con.execute("CREATE SECRET prod_s3_data (TYPE s3, KEY_ID 'k', SECRET 's', SCOPE 's3://prod-data/')")
con.execute("CREATE SECRET dev_s3_data (TYPE s3, KEY_ID 'k', SECRET 's', SCOPE 's3://dev-data/')")
con.execute("CREATE SECRET api_auth (TYPE http, BEARER_TOKEN 'token')")

# Gerar inventário
inventory = create_secret_inventory(con)
print("\nSecret Inventory:")
print(inventory[['name', 'type', 'scope']])

# Exportar documentação
export_secret_documentation(inventory)

print("""
Secret Management Best Practices:
══════════════════════════════════════════════════════

1. Documentation:
   ──────────────
   ✓ Manter inventário atualizado
   ✓ Documentar purpose de cada secret
   ✓ Documentar SCOPEs e seus significados
   ✓ Incluir owner/team responsável

2. Lifecycle Management:
   ─────────────────────
   ✓ Rotação regular de credenciais
   ✓ Remoção de secrets não utilizados
   ✓ Versionamento de configurações
   ✓ Backup de persistent secrets

3. Security:
   ─────────
   ✓ Princípio de privilégio mínimo
   ✓ Separação por ambiente
   ✓ Auditoria de acesso
   ✓ Secrets em variáveis de ambiente (não hardcoded)

4. Testing:
   ────────
   ✓ Validar SCOPEs com which_secret()
   ✓ Testar em staging antes de prod
   ✓ Automated tests para configuração
   ✓ Rollback plan

5. Monitoring:
   ───────────
   ✓ Alertas para credenciais expiradas
   ✓ Log de uso de secrets
   ✓ Detecção de secrets vazados (git-secrets, trufflehog)
   ✓ Regular security audits
""")

con.close()

# Exemplo/Bloco 11
# 1. Crie 5 secrets S3 com hierarquia de SCOPEs:
#    - Um global (sem SCOPE)
#    - Um para 's3://data/'
#    - Um para 's3://data/prod/'
#    - Um para 's3://data/prod/sensitive/'
#    - Um para bucket diferente 's3://logs/'
# 2. Teste 10 URLs diferentes com which_secret()
# 3. Documente o matching de cada URL
# 4. Explique a hierarquia aplicada

# Sua solução aqui

# Exemplo/Bloco 12
# 1. Crie secrets para 3 ambientes (dev, staging, prod)
# 2. Cada ambiente deve ter:
#    - S3 secret com SCOPE específico
#    - PostgreSQL secret
#    - HTTP secret para API
# 3. Use naming convention consistente
# 4. Documente a estratégia de naming
# 5. Crie função para switch entre ambientes

# Sua solução aqui

# Exemplo/Bloco 13
# 1. Configure secrets para:
#    - AWS S3 (2 buckets diferentes)
#    - Azure Blob Storage (1 container)
#    - GCS (1 bucket)
# 2. Use SCOPEs apropriados
# 3. Teste which_secret() para cada provider
# 4. Crie query que une dados de todos os providers
# 5. Documente vantagens e challenges

# Sua solução aqui

# Exemplo/Bloco 14
# 1. Crie 10+ secrets variados (diferentes types, SCOPEs)
# 2. Implemente função para gerar inventário completo
# 3. Exporte para Markdown com formatação
# 4. Inclua recommendations e warnings
# 5. Adicione validation checks (duplicate SCOPEs, etc.)

# Sua solução aqui

