# -*- coding: utf-8 -*-
"""
capitulo_09_seguranca_best_practices
"""

# capitulo_09_seguranca_best_practices
import duckdb
import os

# Exemplo/Bloco 1
print("""
Princípios Fundamentais de Segurança para Secrets:
════════════════════════════════════════════════════════

1. Defense in Depth (Defesa em Profundidade)
   ──────────────────────────────────────────
   Múltiplas camadas de segurança:
   ✓ Encryption at rest (disco)
   ✓ Encryption in transit (rede)
   ✓ Access control (quem pode acessar)
   ✓ Audit logging (rastreamento)
   ✓ Secret rotation (atualização)

2. Principle of Least Privilege
   ─────────────────────────────
   ✓ Apenas permissões necessárias
   ✓ Read-only quando possível
   ✓ Scoped secrets (não global)
   ✓ Time-limited credentials

3. Separation of Duties
   ─────────────────────
   ✓ Dev não tem prod credentials
   ✓ Diferentes secrets por ambiente
   ✓ Diferentes secrets por função

4. Never Trust, Always Verify
   ───────────────────────────
   ✓ Validate all inputs
   ✓ Verify secret integrity
   ✓ Monitor for anomalies
   ✓ Regular security audits

5. Security by Design
   ───────────────────
   ✓ Segurança desde o início
   ✓ Não é afterthought
   ✓ Documented security practices
   ✓ Team training
""")

# Exemplo/Bloco 2
print("""
Threat Model para Secrets:
════════════════════════════════════════════════════════

Ameaças:
────────

1. Credential Exposure
   ───────────────────
   Risco: Credenciais em código, logs, git
   Impacto: Alto - Acesso não autorizado
   Mitigação:
   ✓ Nunca hardcode credentials
   ✓ Use variáveis de ambiente
   ✓ .gitignore para secrets
   ✓ Git secrets scanning

2. Man-in-the-Middle (MITM)
   ────────────────────────
   Risco: Interceptação de comunicação
   Impacto: Alto - Roubo de credenciais
   Mitigação:
   ✓ SSL/TLS sempre
   ✓ Certificate validation
   ✓ VPN/Private networks

3. Privilege Escalation
   ─────────────────────
   Risco: Usuário obtém mais permissões
   Impacto: Médio-Alto - Acesso indevido
   Mitigação:
   ✓ Least privilege
   ✓ IAM roles bem definidas
   ✓ Regular permission audits

4. Insider Threat
   ───────────────
   Risco: Usuário autorizado age maliciosamente
   Impacto: Alto - Difícil detectar
   Mitigação:
   ✓ Audit logging
   ✓ Separation of duties
   ✓ Anomaly detection
   ✓ Regular access reviews

5. Credential Leakage
   ───────────────────
   Risco: Secrets vazam para logs, monitoring
   Impacto: Alto - Exposição ampla
   Mitigação:
   ✓ Log sanitization
   ✓ Masked credentials in outputs
   ✓ Secure log storage

6. Stolen Credentials
   ───────────────────
   Risco: Phishing, malware, breach
   Impacto: Crítico - Acesso total
   Mitigação:
   ✓ MFA where possible
   ✓ Short-lived tokens
   ✓ Immediate rotation on breach
   ✓ IP whitelisting
""")

# Exemplo/Bloco 3
import duckdb
import os

print("""
NUNCA Faça Isso:
════════════════════════════════════════════════════════
""")

# ❌ MAL - Credenciais hardcoded
bad_example = """
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# ❌ NUNCA FAÇA ISSO!
con.execute(\"\"\"
    CREATE SECRET s3_bad (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    )
\"\"\")

# Problemas:
# - Credenciais visíveis no código
# - Vão para git
# - Aparecem em logs
# - Difícil rotacionar
"""

print(bad_example)

print("""
✓ BOM - Use Variáveis de Ambiente:
════════════════════════════════════════════════════════
""")

# ✓ BOM - Variáveis de ambiente
good_example = """
import duckdb
import os

# Ler de variáveis de ambiente
s3_key_id = os.getenv('AWS_ACCESS_KEY_ID')
s3_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

if not s3_key_id or not s3_secret:
    raise ValueError("AWS credentials not found in environment variables")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute(f\"\"\"
    CREATE SECRET s3_good (
        TYPE s3,
        KEY_ID '{s3_key_id}',
        SECRET '{s3_secret}'
    )
\"\"\")

# Vantagens:
# ✓ Credenciais não no código
# ✓ Não vai para git
# ✓ Fácil mudar por ambiente
# ✓ Pode usar secrets manager
"""

print(good_example)

# Exemplo prático
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Simulação (em produção, use variáveis reais)
os.environ['AWS_ACCESS_KEY_ID'] = 'demo_key'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'demo_secret'

s3_key = os.getenv('AWS_ACCESS_KEY_ID')
s3_secret = os.getenv('AWS_SECRET_ACCESS_KEY')

con.execute(f"""
    CREATE SECRET s3_env (
        TYPE s3,
        KEY_ID '{s3_key}',
        SECRET '{s3_secret}'
    )
""")

print("\n✓ Secret criado usando variáveis de ambiente")

# Limpar
con.execute("DROP SECRET s3_env")
con.close()

# Exemplo/Bloco 4
import os

print("""
Usando .env Files (Development):
════════════════════════════════════════════════════════
""")

# Criar .env file (exemplo)
env_content = """
# .env file
AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
AWS_REGION=us-east-1

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mydb
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword

AZURE_STORAGE_ACCOUNT=myaccount
AZURE_STORAGE_KEY=mykey
"""

print("Conteúdo do .env:")
print(env_content)

print("""
.gitignore (OBRIGATÓRIO):
─────────────────────────
# Secrets
.env
.env.*
*.secret
secrets/

# DuckDB
*.duckdb
*.duckdb.wal

# Python
__pycache__/
*.pyc
""")

# Exemplo de código que usa .env
code_example = """
# main.py
import duckdb
import os
from dotenv import load_dotenv  # pip install python-dotenv

# Carregar .env
load_dotenv()

# Usar variáveis
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute(f\"\"\"
    CREATE SECRET s3_from_env (
        TYPE s3,
        KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID')}',
        SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY')}',
        REGION '{os.getenv('AWS_REGION')}'
    )
\"\"\")

print("✓ Credentials loaded from .env")
"""

print("\nCódigo usando .env:")
print(code_example)

print("""
Best Practices para .env:
─────────────────────────
✓ .env no .gitignore
✓ .env.example commitado (sem valores reais)
✓ Documentar variáveis necessárias
✓ Validar que variáveis existem antes de usar
✗ Nunca commitar .env com credenciais reais
✗ Não usar .env em produção (use secrets manager)
""")

# Exemplo/Bloco 5
print("""
Integrando com Secrets Managers:
════════════════════════════════════════════════════════

1. AWS Secrets Manager:
   ────────────────────
""")

aws_example = """
import boto3
import json
import duckdb

def get_secret_from_aws(secret_name):
    client = boto3.client('secretsmanager', region_name='us-east-1')
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response['SecretString'])

# Obter credenciais do Secrets Manager
s3_creds = get_secret_from_aws('duckdb/s3-credentials')

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute(f\"\"\"
    CREATE SECRET s3_aws_sm (
        TYPE s3,
        KEY_ID '{s3_creds['access_key_id']}',
        SECRET '{s3_creds['secret_access_key']}',
        REGION '{s3_creds['region']}'
    )
\"\"\")

print("✓ Credentials from AWS Secrets Manager")
"""

print(aws_example)

print("""
2. Azure Key Vault:
   ────────────────
""")

azure_example = """
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import duckdb

def get_secret_from_azure(vault_url, secret_name):
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value

# Obter credenciais do Key Vault
vault_url = "https://my-keyvault.vault.azure.net/"
storage_key = get_secret_from_azure(vault_url, "storage-account-key")
storage_account = get_secret_from_azure(vault_url, "storage-account-name")

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

con.execute(f\"\"\"
    CREATE SECRET azure_kv (
        TYPE azure,
        ACCOUNT_NAME '{storage_account}',
        ACCOUNT_KEY '{storage_key}'
    )
\"\"\")

print("✓ Credentials from Azure Key Vault")
"""

print(azure_example)

print("""
3. HashiCorp Vault:
   ────────────────
""")

vault_example = """
import hvac
import duckdb

def get_secret_from_vault(vault_url, token, path):
    client = hvac.Client(url=vault_url, token=token)
    secret = client.secrets.kv.v2.read_secret_version(path=path)
    return secret['data']['data']

# Obter credenciais do Vault
vault_url = "http://localhost:8200"
vault_token = os.getenv('VAULT_TOKEN')
s3_creds = get_secret_from_vault(vault_url, vault_token, 'duckdb/s3')

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

con.execute(f\"\"\"
    CREATE SECRET s3_vault (
        TYPE s3,
        KEY_ID '{s3_creds['key_id']}',
        SECRET '{s3_creds['secret']}',
        REGION '{s3_creds['region']}'
    )
\"\"\")

print("✓ Credentials from HashiCorp Vault")
"""

print(vault_example)

print("""
Vantagens de Secrets Managers:
───────────────────────────────
✓ Centralização de secrets
✓ Rotação automática
✓ Audit trail completo
✓ Encryption at rest
✓ Access control granular
✓ Versionamento de secrets
✓ Compliance (SOC2, HIPAA, etc.)

Quando Usar:
────────────
✓ Produção (obrigatório)
✓ Staging
✓ Qualquer ambiente compartilhado
✗ Development local (overkill, use .env)
""")

# Exemplo/Bloco 6
import duckdb
from datetime import datetime, timedelta

print("""
Estratégia de Rotação de Secrets:
════════════════════════════════════════════════════════

Por que Rotacionar:
───────────────────
✓ Reduz janela de exposição
✓ Limita impacto de breach
✓ Compliance requirements
✓ Best practice de segurança

Frequência Recomendada:
───────────────────────
Produção:    30-90 dias
Staging:     60-120 dias
Development: Quando necessário
Breach:      Imediatamente!

High-value: 30 dias ou menos
Standard:   90 dias
Low-risk:   180 dias
""")

def rotate_s3_secret(con, secret_name, new_key_id, new_secret, new_region='us-east-1'):
    """
    Rotacionar secret S3
    """
    print(f"\n{'='*60}")
    print(f"Rotacionando secret: {secret_name}")
    print(f"{'='*60}")

    # 1. Verificar se secret existe
    existing = con.execute(f"""
        SELECT name, persistent
        FROM duckdb_secrets()
        WHERE name = '{secret_name}'
    """).fetchone()

    if not existing:
        print(f"✗ Secret '{secret_name}' não encontrado")
        return False

    is_persistent = existing[1]

    # 2. Backup do secret atual (metadata)
    print("1. Backup de metadata...")
    backup_time = datetime.now().isoformat()
    print(f"   Backup timestamp: {backup_time}")

    # 3. Drop secret antigo
    print("2. Removendo secret antigo...")
    drop_cmd = f"DROP {'PERSISTENT ' if is_persistent else ''}SECRET {secret_name}"
    con.execute(drop_cmd)
    print(f"   ✓ Secret removido")

    # 4. Criar novo secret com novas credenciais
    print("3. Criando novo secret...")
    create_cmd = f"""
        CREATE {'PERSISTENT ' if is_persistent else ''}SECRET {secret_name} (
            TYPE s3,
            KEY_ID '{new_key_id}',
            SECRET '{new_secret}',
            REGION '{new_region}'
        )
    """
    con.execute(create_cmd)
    print(f"   ✓ Novo secret criado")

    # 5. Validar
    print("4. Validando...")
    new_secret_info = con.execute(f"""
        SELECT name, type
        FROM duckdb_secrets()
        WHERE name = '{secret_name}'
    """).fetchone()

    if new_secret_info:
        print(f"   ✓ Secret '{new_secret_info[0]}' validado")
        print(f"\n✓ Rotação completada com sucesso!")
        return True
    else:
        print(f"   ✗ Erro na validação")
        return False

# Exemplo de uso
con = duckdb.connect('rotation_test.duckdb')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Criar secret inicial
con.execute("""
    CREATE PERSISTENT SECRET s3_to_rotate (
        TYPE s3,
        KEY_ID 'old_key_id',
        SECRET 'old_secret',
        REGION 'us-east-1'
    )
""")

print("Secret inicial criado")

# Rotacionar
rotate_s3_secret(
    con,
    secret_name='s3_to_rotate',
    new_key_id='new_key_id',
    new_secret='new_secret',
    new_region='us-west-2'
)

# Limpar
con.execute("DROP PERSISTENT SECRET s3_to_rotate")
con.close()

import os
if os.path.exists('rotation_test.duckdb'):
    os.remove('rotation_test.duckdb')

# Exemplo/Bloco 7
import duckdb

print("""
Zero-Downtime Rotation Strategy:
════════════════════════════════════════════════════════

Problema:
─────────
Durante rotação (DROP + CREATE), há período sem secret,
causando falhas em queries em execução.

Solução:
────────
Usar dual-secret pattern com SCOPEs diferentes.
""")

def zero_downtime_rotate(con, base_name, new_key_id, new_secret, scope):
    """
    Rotação sem downtime usando dual secrets
    """
    print(f"\n{'='*60}")
    print(f"Zero-Downtime Rotation: {base_name}")
    print(f"{'='*60}")

    old_name = f"{base_name}_old"
    new_name = f"{base_name}_new"

    # 1. Criar novo secret com SCOPE
    print("1. Criando novo secret...")
    con.execute(f"""
        CREATE SECRET {new_name} (
            TYPE s3,
            KEY_ID '{new_key_id}',
            SECRET '{new_secret}',
            SCOPE '{scope}'
        )
    """)
    print(f"   ✓ {new_name} criado")

    # 2. Testar novo secret
    print("2. Testando novo secret...")
    test_result = con.execute(f"""
        SELECT name FROM which_secret('{scope}file.parquet', 's3')
    """).fetchone()

    if test_result and test_result[0] == new_name:
        print(f"   ✓ Novo secret funcional")
    else:
        print(f"   ✗ Erro no teste")
        return False

    # 3. Remover secret antigo (se existir)
    print("3. Removendo secret antigo...")
    try:
        con.execute(f"DROP SECRET IF EXISTS {old_name}")
        con.execute(f"DROP SECRET IF EXISTS {base_name}")
        print(f"   ✓ Secret antigo removido")
    except:
        pass

    # 4. Renomear novo → base (via drop + create)
    print("4. Finalizando...")
    # DuckDB não tem RENAME, então recriamos
    con.execute(f"DROP SECRET {new_name}")
    con.execute(f"""
        CREATE SECRET {base_name} (
            TYPE s3,
            KEY_ID '{new_key_id}',
            SECRET '{new_secret}',
            SCOPE '{scope}'
        )
    """)
    print(f"   ✓ {base_name} atualizado")

    print(f"\n✓ Zero-downtime rotation completada!")
    return True

# Exemplo
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Secret inicial
con.execute("""
    CREATE SECRET s3_prod (
        TYPE s3,
        KEY_ID 'old_key',
        SECRET 'old_secret',
        SCOPE 's3://prod-bucket/'
    )
""")

# Rotacionar sem downtime
zero_downtime_rotate(
    con,
    base_name='s3_prod',
    new_key_id='new_key',
    new_secret='new_secret',
    scope='s3://prod-bucket/'
)

con.close()

# Exemplo/Bloco 8
import duckdb
from datetime import datetime, timedelta
import json
import os

class SecretRotationManager:
    """
    Gerenciador de rotação automática de secrets
    """

    def __init__(self, db_path, metadata_file='rotation_metadata.json'):
        self.db_path = db_path
        self.metadata_file = metadata_file
        self.load_metadata()

    def load_metadata(self):
        """Carregar metadata de rotações"""
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}

    def save_metadata(self):
        """Salvar metadata de rotações"""
        with open(self.metadata_file, 'w') as f:
            json.dump(self.metadata, f, indent=2)

    def needs_rotation(self, secret_name, max_age_days=90):
        """
        Verificar se secret precisa ser rotacionado
        """
        if secret_name not in self.metadata:
            return True, "Never rotated"

        last_rotation = datetime.fromisoformat(self.metadata[secret_name]['last_rotation'])
        age = (datetime.now() - last_rotation).days

        if age >= max_age_days:
            return True, f"Age: {age} days (max: {max_age_days})"
        else:
            return False, f"Age: {age} days (OK)"

    def record_rotation(self, secret_name):
        """Registrar rotação"""
        self.metadata[secret_name] = {
            'last_rotation': datetime.now().isoformat(),
            'rotation_count': self.metadata.get(secret_name, {}).get('rotation_count', 0) + 1
        }
        self.save_metadata()

    def rotate_if_needed(self, secret_name, new_credentials, max_age_days=90):
        """
        Rotacionar secret se necessário
        """
        needs_rot, reason = self.needs_rotation(secret_name, max_age_days)

        print(f"\n{'='*60}")
        print(f"Secret: {secret_name}")
        print(f"Needs rotation: {needs_rot} ({reason})")
        print(f"{'='*60}")

        if needs_rot:
            con = duckdb.connect(self.db_path)
            con.execute("INSTALL httpfs; LOAD httpfs;")

            # Rotacionar
            try:
                con.execute(f"DROP SECRET IF EXISTS {secret_name}")
                con.execute(f"""
                    CREATE PERSISTENT SECRET {secret_name} (
                        TYPE s3,
                        KEY_ID '{new_credentials['key_id']}',
                        SECRET '{new_credentials['secret']}',
                        REGION '{new_credentials.get('region', 'us-east-1')}'
                    )
                """)

                # Registrar
                self.record_rotation(secret_name)
                print(f"✓ Secret rotacionado com sucesso")

                con.close()
                return True

            except Exception as e:
                print(f"✗ Erro na rotação: {e}")
                con.close()
                return False
        else:
            print("✓ Rotação não necessária")
            return False

# Exemplo de uso
print("""
Automated Rotation Example:
═══════════════════════════════════════════════════════
""")

manager = SecretRotationManager('rotation_db.duckdb')

# Configurar secrets a rotacionar
secrets_config = {
    's3_production': {
        'key_id': 'PROD_NEW_KEY',
        'secret': 'prod_new_secret',
        'region': 'us-east-1'
    },
    's3_analytics': {
        'key_id': 'ANALYTICS_NEW_KEY',
        'secret': 'analytics_new_secret',
        'region': 'us-west-2'
    }
}

# Verificar e rotacionar cada secret
for secret_name, creds in secrets_config.items():
    manager.rotate_if_needed(secret_name, creds, max_age_days=90)

print(f"\nMetadata salva em: {manager.metadata_file}")

# Limpar
if os.path.exists('rotation_db.duckdb'):
    os.remove('rotation_db.duckdb')
if os.path.exists('rotation_metadata.json'):
    os.remove('rotation_metadata.json')

# Exemplo/Bloco 9
import duckdb

print("""
SSL/TLS para S3:
════════════════════════════════════════════════════════
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# SSL habilitado (recomendado para produção)
con.execute("""
    CREATE SECRET s3_ssl_enabled (
        TYPE s3,
        KEY_ID 'key',
        SECRET 'secret',
        ENDPOINT 's3.amazonaws.com',
        USE_SSL true,
        REGION 'us-east-1'
    )
""")

# SSL desabilitado (apenas desenvolvimento local)
con.execute("""
    CREATE SECRET s3_ssl_disabled (
        TYPE s3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT 'localhost:9000',
        USE_SSL false
    )
""")

print("""
SSL Configuration:
──────────────────

Production (AWS S3):
  USE_SSL = true (obrigatório)
  ENDPOINT = 's3.amazonaws.com' ou regional
  ✓ Dados criptografados em trânsito
  ✓ Certificate validation
  ✓ Protection against MITM

Development (MinIO local):
  USE_SSL = false (aceitável)
  ENDPOINT = 'localhost:9000'
  ✓ Mais rápido (sem overhead SSL)
  ⚠ Apenas em rede confiável

MinIO Produção:
  USE_SSL = true (obrigatório)
  ENDPOINT = 'minio.example.com'
  ✓ Mesmo nível de segurança que AWS

Best Practices:
───────────────
✓ Sempre USE_SSL = true em produção
✓ Validar certificates
✓ Usar TLS 1.2+ (não TLS 1.0/1.1)
✗ Nunca desabilitar SSL em produção
✗ Não usar self-signed certs sem validation
""")

con.close()

# Exemplo/Bloco 10
import duckdb

print("""
SSL/TLS para PostgreSQL:
════════════════════════════════════════════════════════
""")

con = duckdb.connect()
con.execute("INSTALL postgres_scanner; LOAD postgres_scanner;")

# SSL Modes do PostgreSQL
print("""
PostgreSQL SSL Modes:
─────────────────────

disable:
  Sem SSL
  ✗ Nunca use em produção

allow:
  Tenta SSL, aceita não-SSL
  ⚠ Vulnerável a downgrade attacks

prefer:
  Prefere SSL, aceita não-SSL
  ⚠ Padrão, mas não suficiente para produção

require:
  SSL obrigatório, sem validação de certificado
  ✓ Proteção contra eavesdropping
  ⚠ Vulnerável a MITM (sem cert validation)

verify-ca:
  SSL obrigatório + valida certificado CA
  ✓ Protege contra MITM
  ⚠ Não valida hostname

verify-full:
  SSL obrigatório + valida CA + hostname
  ✓ Segurança máxima
  ✓ Recomendado para produção
""")

# require (básico)
con.execute("""
    CREATE SECRET postgres_ssl_require (
        TYPE postgres,
        HOST 'postgres.example.com',
        PORT 5432,
        DATABASE 'mydb',
        USER 'user',
        PASSWORD 'password',
        SSLMODE 'require'
    )
""")

# verify-full (recomendado)
con.execute("""
    CREATE SECRET postgres_ssl_verify (
        TYPE postgres,
        HOST 'postgres.example.com',
        PORT 5432,
        DATABASE 'mydb',
        USER 'user',
        PASSWORD 'password',
        SSLMODE 'verify-full',
        SSLROOTCERT '/path/to/root.crt',
        SSLCERT '/path/to/client.crt',
        SSLKEY '/path/to/client.key'
    )
""")

print("""
Recomendações por Ambiente:
────────────────────────────

Development Local:
  SSLMODE = 'prefer' ou 'disable'
  (Se database local sem SSL)

Staging:
  SSLMODE = 'require' (mínimo)
  SSLMODE = 'verify-ca' (recomendado)

Production:
  SSLMODE = 'verify-full' (obrigatório)
  + Client certificates
  + Certificate rotation policy

Compliance (HIPAA, PCI-DSS):
  SSLMODE = 'verify-full'
  + Mutual TLS (mTLS)
  + Regular cert rotation
  + Audit logging
""")

con.close()

# Exemplo/Bloco 11
import duckdb

print("""
SSL/TLS para MySQL:
════════════════════════════════════════════════════════
""")

con = duckdb.connect()
con.execute("INSTALL mysql_scanner; LOAD mysql_scanner;")

print("""
MySQL SSL Modes:
────────────────

DISABLED:
  Sem SSL
  ✗ Nunca use em produção

PREFERRED:
  Tenta SSL, aceita não-SSL
  ⚠ Padrão, mas não suficiente

REQUIRED:
  SSL obrigatório, sem validação
  ✓ Proteção básica
  ⚠ Vulnerável a MITM

VERIFY_CA:
  SSL obrigatório + valida CA
  ✓ Protege contra MITM

VERIFY_IDENTITY:
  SSL obrigatório + valida CA + hostname
  ✓ Segurança máxima
""")

# REQUIRED (básico)
con.execute("""
    CREATE SECRET mysql_ssl_required (
        TYPE mysql,
        HOST 'mysql.example.com',
        PORT 3306,
        DATABASE 'mydb',
        USER 'user',
        PASSWORD 'password',
        SSL_MODE 'REQUIRED'
    )
""")

# VERIFY_CA (recomendado)
con.execute("""
    CREATE SECRET mysql_ssl_verify (
        TYPE mysql,
        HOST 'mysql.example.com',
        PORT 3306,
        DATABASE 'mydb',
        USER 'user',
        PASSWORD 'password',
        SSL_MODE 'VERIFY_CA',
        SSL_CA '/path/to/ca-cert.pem',
        SSL_CERT '/path/to/client-cert.pem',
        SSL_KEY '/path/to/client-key.pem'
    )
""")

print("""
Best Practices:
───────────────
✓ Production: VERIFY_CA ou VERIFY_IDENTITY
✓ Use client certificates quando possível
✓ Rotacionar certificates regularmente
✓ Monitorar expiration dates
✗ Não usar PREFERRED em produção
✗ Não compartilhar client certificates
""")

con.close()

# Exemplo/Bloco 12
import duckdb
from datetime import datetime
import json

class SecretAuditLogger:
    """
    Logger para auditoria de uso de secrets
    """

    def __init__(self, log_file='secret_audit.log'):
        self.log_file = log_file

    def log_secret_creation(self, secret_name, secret_type, user, scope=None):
        """Log de criação de secret"""
        self._log({
            'event': 'SECRET_CREATED',
            'timestamp': datetime.now().isoformat(),
            'secret_name': secret_name,
            'secret_type': secret_type,
            'scope': scope,
            'user': user
        })

    def log_secret_deletion(self, secret_name, user):
        """Log de deleção de secret"""
        self._log({
            'event': 'SECRET_DELETED',
            'timestamp': datetime.now().isoformat(),
            'secret_name': secret_name,
            'user': user
        })

    def log_secret_usage(self, secret_name, operation, resource):
        """Log de uso de secret"""
        self._log({
            'event': 'SECRET_USED',
            'timestamp': datetime.now().isoformat(),
            'secret_name': secret_name,
            'operation': operation,
            'resource': resource
        })

    def log_secret_rotation(self, secret_name, user):
        """Log de rotação de secret"""
        self._log({
            'event': 'SECRET_ROTATED',
            'timestamp': datetime.now().isoformat(),
            'secret_name': secret_name,
            'user': user
        })

    def _log(self, entry):
        """Escrever log"""
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')

    def get_audit_trail(self, secret_name=None):
        """Obter audit trail"""
        logs = []
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    entry = json.loads(line)
                    if secret_name is None or entry.get('secret_name') == secret_name:
                        logs.append(entry)
        except FileNotFoundError:
            pass
        return logs

# Exemplo de uso
print("""
Secret Audit Logging Example:
════════════════════════════════════════════════════════
""")

logger = SecretAuditLogger('secret_audit.log')

# Simular eventos
logger.log_secret_creation('s3_prod', 's3', 'admin', 's3://prod-bucket/')
logger.log_secret_usage('s3_prod', 'READ', 's3://prod-bucket/data.parquet')
logger.log_secret_rotation('s3_prod', 'admin')
logger.log_secret_deletion('s3_temp', 'admin')

# Ver audit trail
trail = logger.get_audit_trail()
print("\nAudit Trail:")
for entry in trail:
    print(f"  {entry['timestamp']} - {entry['event']} - {entry.get('secret_name', 'N/A')}")

# Trail para secret específico
s3_prod_trail = logger.get_audit_trail('s3_prod')
print(f"\nAudit Trail para 's3_prod': {len(s3_prod_trail)} eventos")

# Limpar
import os
if os.path.exists('secret_audit.log'):
    os.remove('secret_audit.log')

print("""
O que Auditar:
──────────────
✓ Criação de secrets
✓ Deleção de secrets
✓ Rotação de secrets
✓ Uso de secrets (queries)
✓ Falhas de autenticação
✓ Modificações de SCOPEs
✓ Export de dados com secrets

Retention:
──────────
- Logs: 90+ dias (compliance)
- Audit trail: 1+ ano
- Security events: 7+ anos

Alertas:
────────
⚠ Multiple failed authentications
⚠ Secret usado fora de horário normal
⚠ Secret acessando recursos não esperados
⚠ Secret não rotacionado há 90+ dias
⚠ Tentativa de access a secret sem permissão
""")

# Exemplo/Bloco 13
print("""
Compliance Frameworks:
════════════════════════════════════════════════════════

SOC 2:
──────
✓ Access controls
✓ Encryption at rest and in transit
✓ Audit logging
✓ Regular security reviews
✓ Incident response plan

Implementação com DuckDB:
- Persistent secrets com encryption
- SSL/TLS para todas as conexões
- Audit logging de todos os acessos
- Rotação regular de credenciais
- Documented security policies

HIPAA:
──────
✓ Encrypted data storage
✓ Encrypted data transmission
✓ Access controls
✓ Audit controls
✓ Integrity controls

Implementação com DuckDB:
- Secrets para PHI data sources
- verify-full SSL mode
- Detailed audit logs
- Restricted access to secrets
- Regular security audits

PCI-DSS:
────────
✓ Encryption of cardholder data
✓ Restrict access on need-to-know
✓ Unique IDs for access
✓ Track and monitor access
✓ Regular security testing

Implementação com DuckDB:
- Separate secrets para payment data
- Least privilege access
- Individual named secrets
- Comprehensive audit logging
- Quarterly secret rotation

GDPR:
─────
✓ Data protection by design
✓ Encryption
✓ Access controls
✓ Data breach notification
✓ Right to erasure

Implementação com DuckDB:
- Secrets com SCOPE limitado
- Encryption in transit e at rest
- Audit trail de data access
- Documented data flows
- Secure secret deletion

CCPA:
─────
✓ Consumer data protection
✓ Security measures
✓ Access controls
✓ Data breach response

Implementação com DuckDB:
- Controlled access to consumer data
- Encrypted connections
- Activity monitoring
- Incident response procedures
""")

# Exemplo/Bloco 14
print("""
Security Checklist for Production:
════════════════════════════════════════════════════════

Secrets Management:
───────────────────
☐ No hardcoded credentials
☐ Secrets in environment variables ou secrets manager
☐ .env files in .gitignore
☐ Different secrets per environment
☐ Persistent secrets para production
☐ SCOPE configured apropriadamente
☐ Named secrets com convention clara

Access Control:
───────────────
☐ Least privilege principle
☐ Read-only secrets quando possível
☐ Separate secrets por team/project
☐ IAM roles properly configured
☐ MFA enabled onde suportado
☐ Regular access reviews

Encryption:
───────────
☐ SSL/TLS habilitado (USE_SSL = true)
☐ SSLMODE = verify-full (PostgreSQL)
☐ SSL_MODE = VERIFY_CA (MySQL)
☐ Certificate validation enabled
☐ Client certificates quando necessário
☐ Full disk encryption no servidor
☐ Encryption at rest para persistent secrets

Rotation:
─────────
☐ Rotation policy definida
☐ Automated rotation implementada
☐ Rotation frequency apropriada
☐ Zero-downtime rotation strategy
☐ Rotation metadata tracked
☐ Expired secrets alerting

Monitoring & Audit:
───────────────────
☐ Comprehensive audit logging
☐ Log retention policy (90+ days)
☐ Monitoring de secret usage
☐ Alertas para anomalias
☐ Failed authentication tracking
☐ Regular log reviews
☐ SIEM integration

Incident Response:
──────────────────
☐ Incident response plan documentado
☐ Secret breach procedure
☐ Emergency rotation process
☐ Contact list atualizada
☐ Post-incident review process
☐ Lessons learned documentation

Documentation:
──────────────
☐ Secret inventory atualizado
☐ SCOPE documentation
☐ Security policies documentadas
☐ Rotation procedures documentadas
☐ Troubleshooting guides
☐ Runbooks para common tasks

Testing:
────────
☐ Regular security testing
☐ Penetration testing
☐ Secret scanning em CI/CD
☐ Rotation testing
☐ Disaster recovery drills
☐ Access control validation

Compliance:
───────────
☐ Framework requirements identificados
☐ Controls implementados
☐ Regular compliance audits
☐ Documentation para auditors
☐ Control effectiveness testing
☐ Remediation tracking
""")

# Exemplo/Bloco 15
# 1. Configure um ambiente com secrets seguros:
#    - Use variáveis de ambiente (não hardcode)
#    - Configure SSL/TLS
#    - Implemente least privilege
# 2. Crie .env file com secrets
# 3. Adicione .env ao .gitignore
# 4. Valide que secrets não estão expostos
# 5. Documente security decisions

# Sua solução aqui

# Exemplo/Bloco 16
# 1. Implemente automated rotation system
# 2. Configure rotation metadata tracking
# 3. Implemente zero-downtime rotation
# 4. Adicione validation pós-rotação
# 5. Configure alertas para rotações vencidas
# 6. Teste com múltiplos secrets

# Sua solução aqui

# Exemplo/Bloco 17
# 1. Implemente comprehensive audit logging
# 2. Log todos os eventos de secrets
# 3. Implemente audit trail queries
# 4. Configure retention policy
# 5. Adicione alerting para eventos suspeitos
# 6. Crie dashboard de audit metrics

# Sua solução aqui

# Exemplo/Bloco 18
# 1. Escolha um framework (SOC2, HIPAA, PCI-DSS)
# 2. Identifique requirements relevantes
# 3. Implemente controles necessários
# 4. Documente implementação
# 5. Crie evidências para auditoria
# 6. Valide compliance

# Sua solução aqui

