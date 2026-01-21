# -*- coding: utf-8 -*-
"""
Capítulo 3: Configuração de Credenciais AWS
============================================

IMPORTANTE: Este script usa credenciais MOCADAS para demonstração.
NÃO use credenciais reais AWS neste código!

Autor: Curso DuckDB + S3
"""

import duckdb
import os

if os.name == 'nt':
    os.system('chcp 65001 > nul')

print("=" * 70)
print("CAPÍTULO 3: CONFIGURAÇÃO DE CREDENCIAIS AWS")
print("=" * 70)

print("""
⚠️  AVISO IMPORTANTE ⚠️
Este script usa credenciais MOCADAS apenas para demonstração.
Para uso real com S3:
1. Configure suas credenciais AWS via AWS CLI (aws configure)
2. Ou use variáveis de ambiente (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
3. Ou use IAM Roles (em EC2/Lambda)
""")

conn = duckdb.connect(':memory:')
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

# =============================================================================
# 1. MÉTODO 1: PROVIDER CONFIG (Configuração Manual)
# =============================================================================
print("\n1. Método 1: Provider CONFIG (configuração manual)")
print("-" * 70)

print("""
Sintaxe:
CREATE SECRET my_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'sua_access_key',
    SECRET 'sua_secret_key',
    REGION 'us-east-1'
);
""")

# Exemplo com credenciais mocadas (NÃO FUNCIONAIS)
try:
    conn.execute("""
        CREATE SECRET mock_s3_secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID 'AKIAIOSFODNN7EXAMPLE',
            SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            REGION 'us-east-1'
        )
    """)
    print("✓ Secret 'mock_s3_secret' criado (credenciais mocadas)")
except Exception as e:
    print(f"Nota: {e}")

# =============================================================================
# 2. MÉTODO 2: PROVIDER CREDENTIAL_CHAIN (Automático)
# =============================================================================
print("\n2. Método 2: Provider CREDENTIAL_CHAIN (automático)")
print("-" * 70)

print("""
Este método busca credenciais em múltiplas fontes:
1. Arquivo ~/.aws/credentials
2. AWS STS
3. AWS SSO
4. Variáveis de ambiente
5. EC2 Instance Metadata
6. AWS credential process

Sintaxe:
CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain
);
""")

# =============================================================================
# 3. LISTAR SECRETS CRIADOS
# =============================================================================
print("\n3. Listar secrets configurados")
print("-" * 70)

try:
    result = conn.execute("""
        SELECT name, type, provider, scope, persist
        FROM duckdb_secrets()
    """).fetchdf()

    if len(result) > 0:
        print("Secrets configurados:")
        print(result)
    else:
        print("Nenhum secret configurado ainda.")
except Exception as e:
    print(f"Erro ao listar secrets: {e}")

# =============================================================================
# 4. VERIFICAR QUAL SECRET SERÁ USADO
# =============================================================================
print("\n4. Função which_secret()")
print("-" * 70)

print("""
Use which_secret() para verificar qual secret será usado para um path:

SELECT which_secret('s3://meu-bucket/arquivo.parquet', 's3');
""")

# =============================================================================
# 5. SCOPE: CONTROLANDO ACESSO
# =============================================================================
print("\n5. Scope: Controlando onde secrets são usados")
print("-" * 70)

print("""
Exemplos de scope:

-- Global (todos os buckets S3)
CREATE SECRET global_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'key',
    SECRET 'secret',
    SCOPE 's3://'
);

-- Bucket específico
CREATE SECRET bucket_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'key',
    SECRET 'secret',
    SCOPE 's3://meu-bucket'
);

-- Path específico dentro do bucket
CREATE SECRET path_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'key',
    SECRET 'secret',
    SCOPE 's3://meu-bucket/dados-sensiveis'
);
""")

# Exemplo com múltiplos scopes
try:
    # Desenvolvimento
    conn.execute("""
        CREATE SECRET dev_mock (
            TYPE s3,
            PROVIDER config,
            KEY_ID 'AKIA_DEV_EXAMPLE',
            SECRET 'dev_secret_example',
            REGION 'us-east-1',
            SCOPE 's3://dev-bucket'
        )
    """)

    # Produção
    conn.execute("""
        CREATE SECRET prod_mock (
            TYPE s3,
            PROVIDER config,
            KEY_ID 'AKIA_PROD_EXAMPLE',
            SECRET 'prod_secret_example',
            REGION 'us-east-1',
            SCOPE 's3://prod-bucket'
        )
    """)

    print("✓ Secrets com scope criados (dev e prod)")
except Exception as e:
    print(f"Nota: {e}")

# Listar secrets com scope
try:
    result = conn.execute("""
        SELECT name, scope
        FROM duckdb_secrets()
        WHERE type = 's3'
    """).fetchdf()
    print("\nSecrets com scope:")
    print(result)
except:
    pass

# =============================================================================
# 6. DELETAR SECRETS
# =============================================================================
print("\n6. Deletar secrets")
print("-" * 70)

print("""
Para deletar um secret:
DROP SECRET nome_do_secret;

Para deletar secret persistente:
DROP PERSISTENT SECRET nome_do_secret;
""")

# =============================================================================
# 7. CREDENCIAIS TEMPORÁRIAS (Session Token)
# =============================================================================
print("\n7. Credenciais temporárias (Session Token)")
print("-" * 70)

print("""
Para usar credenciais temporárias do AWS STS:

CREATE SECRET temp_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'ASIAXXX...',
    SECRET 'secret...',
    SESSION_TOKEN 'FwoGZXIvYXdzEBYaDH...',
    REGION 'us-east-1'
);
""")

# =============================================================================
# 8. CONFIGURAÇÃO VIA VARIÁVEIS DE AMBIENTE
# =============================================================================
print("\n8. Configuração via variáveis de ambiente")
print("-" * 70)

print("""
No Windows (PowerShell):
$env:AWS_ACCESS_KEY_ID="sua_access_key"
$env:AWS_SECRET_ACCESS_KEY="sua_secret_key"
$env:AWS_DEFAULT_REGION="us-east-1"

No Linux/Mac:
export AWS_ACCESS_KEY_ID=sua_access_key
export AWS_SECRET_ACCESS_KEY=sua_secret_key
export AWS_DEFAULT_REGION=us-east-1

Depois no DuckDB:
CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain,
    CHAIN 'env'
);
""")

# =============================================================================
# 9. EXERCÍCIOS PRÁTICOS
# =============================================================================
print("\n9. Exercícios práticos")
print("-" * 70)

print("""
Exercícios para praticar:

1. Criar um secret com provider CONFIG (use credenciais mocadas)
2. Listar todos os secrets configurados
3. Criar secrets separados para dev e prod com scopes diferentes
4. Usar which_secret() para verificar qual secret seria usado
5. Deletar um secret criado

IMPORTANTE:
- Para testes reais com S3, configure suas credenciais AWS
- Nunca commite credenciais em código
- Use credential_chain em produção
- Sempre use scope para limitar acesso
""")

# Exercício 1 - Solução
print("\nExercício 1 - Solução:")
try:
    conn.execute("""
        CREATE OR REPLACE SECRET exercise_secret (
            TYPE s3,
            PROVIDER config,
            KEY_ID 'AKIAIOSFODNN7EXAMPLE',
            SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            REGION 'us-east-1'
        )
    """)
    print("✓ Secret 'exercise_secret' criado")
except Exception as e:
    print(f"Erro: {e}")

# Exercício 2 - Solução
print("\nExercício 2 - Solução:")
try:
    result = conn.execute("SELECT * FROM duckdb_secrets()").fetchdf()
    print(result)
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
✓ Configurar credenciais com provider CONFIG
✓ Usar credential_chain para autenticação automática
✓ Trabalhar com scopes para limitar acesso
✓ Gerenciar múltiplos secrets
✓ Usar session tokens para credenciais temporárias
✓ Configurar via variáveis de ambiente

⚠️  LEMBRE-SE:
- Nunca use credenciais reais em código-fonte
- Use secrets persistentes apenas em ambientes confiáveis
- Sempre defina scope para limitar acesso
- Prefira credential_chain em produção

Próximo capítulo: Leitura de dados do S3
""")

conn.close()
print("\n✓ Conexão fechada!")
