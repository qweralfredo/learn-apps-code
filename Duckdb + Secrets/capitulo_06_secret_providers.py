# -*- coding: utf-8 -*-
"""
capitulo_06_secret_providers
"""

# capitulo_06_secret_providers
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

print("""
Secret Providers no DuckDB:
┌──────────────────────────────────────────────────────────┐
│ 1. config                                                │
│    - Credenciais explícitas no CREATE SECRET            │
│    - Padrão quando não especificado                     │
│    - Mais simples, menos flexível                       │
│                                                          │
│ 2. credential_chain                                      │
│    - Tenta múltiplas fontes automaticamente             │
│    - Variáveis de ambiente → config files → IAM        │
│    - Recomendado para desenvolvimento                   │
│                                                          │
│ 3. managed_identity (Azure)                             │
│    - Usa identidade gerenciada do recurso Azure        │
│    - Sem credenciais explícitas                         │
│    - Recomendado para produção Azure                    │
│                                                          │
│ 4. service_principal (Azure)                            │
│    - Autentica via Azure Active Directory               │
│    - Tenant ID + Client ID + Client Secret              │
│    - Alternativa a managed_identity                     │
│                                                          │
│ 5. env (AWS)                                            │
│    - Apenas variáveis de ambiente                       │
│    - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY          │
│    - Simples, mas limitado                              │
└──────────────────────────────────────────────────────────┘
""")

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Exemplo: config provider (explícito)
con.execute("""
    CREATE SECRET s3_config (
        TYPE s3,
        PROVIDER config,
        KEY_ID 'explicit_key',
        SECRET 'explicit_secret'
    )
""")

# Listar providers disponíveis
secrets = con.execute("""
    SELECT name, type, provider
    FROM duckdb_secrets()
""").df()

print("\nSecrets criados:")
print(secrets)

con.close()

# Exemplo/Bloco 2
print("""
Guia de Seleção de Provider:
═════════════════════════════════════════════════════════

config:
───────
Usar quando:
  ✓ Desenvolvimento local
  ✓ Testes
  ✓ Credenciais já conhecidas
  ✓ Configuração simples

Não usar quando:
  ✗ Produção (credenciais hardcoded)
  ✗ Múltiplos ambientes
  ✗ CI/CD

credential_chain:
─────────────────
Usar quando:
  ✓ Desenvolvimento e produção
  ✓ Múltiplos ambientes
  ✓ Quer flexibilidade máxima
  ✓ AWS/GCP com IAM

Não usar quando:
  ✗ Precisa controle preciso da fonte
  ✗ Azure (use managed_identity)

managed_identity:
─────────────────
Usar quando:
  ✓ Azure VMs, Functions, AKS
  ✓ Produção Azure
  ✓ Segurança máxima
  ✓ Sem gestão de credenciais

Não usar quando:
  ✗ Fora da Azure
  ✗ Desenvolvimento local
  ✗ Recursos sem identidade gerenciada

service_principal:
──────────────────
Usar quando:
  ✓ Azure sem managed identity
  ✓ CI/CD pipelines Azure
  ✓ Cross-tenant scenarios
  ✓ Aplicações multi-tenant

Não usar quando:
  ✗ Managed identity disponível (preferir)
  ✗ Fora da Azure

Recomendações por Ambiente:
────────────────────────────

Development Local:
  → credential_chain ou config

CI/CD:
  → credential_chain (AWS/GCP)
  → service_principal (Azure)

Production AWS/GCP:
  → credential_chain com IAM roles

Production Azure:
  → managed_identity (preferido)
  → service_principal (alternativa)
""")

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Config provider (explícito)
con.execute("""
    CREATE SECRET s3_explicit (
        TYPE s3,
        PROVIDER config,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1'
    )
""")

# Config provider (implícito - padrão)
con.execute("""
    CREATE SECRET s3_implicit (
        TYPE s3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
        REGION 'us-east-1'
    )
""")

# Ambos são equivalentes
secrets = con.execute("""
    SELECT name, provider
    FROM duckdb_secrets()
    WHERE name LIKE 's3_%'
""").df()

print("Ambos usam provider 'config':")
print(secrets)

print("""
Config Provider:
┌──────────────────────────────────────────────────────┐
│ Características:                                     │
│   - Credenciais passadas explicitamente              │
│   - Padrão quando PROVIDER não especificado          │
│   - Sem busca automática de credenciais              │
│                                                      │
│ Vantagens:                                           │
│   ✓ Simples e direto                                │
│   ✓ Controle total                                  │
│   ✓ Funciona em qualquer ambiente                   │
│                                                      │
│ Desvantagens:                                        │
│   ✗ Credenciais hardcoded                           │
│   ✗ Menos seguro                                    │
│   ✗ Difícil gerenciar múltiplos ambientes           │
└──────────────────────────────────────────────────────┘
""")

con.close()

# Exemplo/Bloco 4
import duckdb
import os

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Credential chain completo
con.execute("""
    CREATE SECRET s3_chain (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'env;config;sts;instance_metadata'
    )
""")

print("""
AWS Credential Chain - Ordem de Busca:
════════════════════════════════════════════════════════

1. env (Variáveis de Ambiente)
   ────────────────────────────
   AWS_ACCESS_KEY_ID
   AWS_SECRET_ACCESS_KEY
   AWS_SESSION_TOKEN (opcional)
   AWS_REGION (opcional)

   Exemplo:
   export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
   export AWS_SECRET_ACCESS_KEY=wJalrXUtn...
   export AWS_REGION=us-east-1

2. config (AWS Config Files)
   ──────────────────────────
   ~/.aws/credentials
   ~/.aws/config

   Exemplo ~/.aws/credentials:
   [default]
   aws_access_key_id = AKIAIOSFODNN7EXAMPLE
   aws_secret_access_key = wJalrXUtn...

   [profile prod]
   aws_access_key_id = AKIAPROD...
   aws_secret_access_key = prodSecret...

3. sts (AWS Security Token Service)
   ─────────────────────────────────
   Assume Role via STS
   Credenciais temporárias
   Útil para cross-account access

   aws sts assume-role \\
     --role-arn arn:aws:iam::123456789012:role/MyRole \\
     --role-session-name my-session

4. instance_metadata (EC2 Instance Metadata)
   ──────────────────────────────────────────
   http://169.254.169.254/latest/meta-data/iam/security-credentials/
   IAM role attached ao EC2 instance
   Credenciais rotacionadas automaticamente
   Recomendado para produção EC2

Processo:
─────────
1. Tenta 'env' → se falhar...
2. Tenta 'config' → se falhar...
3. Tenta 'sts' → se falhar...
4. Tenta 'instance_metadata' → se falhar...
5. Erro: credenciais não encontradas

✓ Para ao encontrar credenciais válidas
✓ Caching automático de credenciais encontradas
""")

# Verificar provider
info = con.execute("""
    SELECT name, provider
    FROM duckdb_secrets()
    WHERE name = 's3_chain'
""").df()

print("\nSecret criado:")
print(info)

con.close()

# Exemplo/Bloco 5
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# GCS credential chain
con.execute("""
    CREATE SECRET gcs_chain (
        TYPE gcs,
        PROVIDER credential_chain
    )
""")

print("""
GCS Credential Chain - Application Default Credentials (ADC):
═══════════════════════════════════════════════════════════════

Ordem de Busca:
───────────────

1. GOOGLE_APPLICATION_CREDENTIALS
   ────────────────────────────────
   Variável de ambiente apontando para service account JSON

   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

   service-account.json:
   {
     "type": "service_account",
     "project_id": "my-project",
     "private_key_id": "...",
     "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
     "client_email": "sa@project.iam.gserviceaccount.com",
     ...
   }

2. gcloud CLI Configuration
   ─────────────────────────
   Credenciais do usuário logado via gcloud

   gcloud auth application-default login

   Salva em: ~/.config/gcloud/application_default_credentials.json

3. Compute Engine Metadata Server
   ────────────────────────────────
   http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/
   Service account attached à VM/GKE
   Credenciais rotacionadas automaticamente

4. App Engine Metadata Server
   ───────────────────────────
   Default service account do App Engine

5. Cloud Run Metadata Server
   ──────────────────────────
   Default service account do Cloud Run

Setup para Desenvolvimento:
────────────────────────────
# Método 1: gcloud CLI
gcloud auth application-default login

# Método 2: Service Account
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

Setup para Produção:
────────────────────────
# GCE VM
gcloud compute instances create my-vm \\
  --service-account=my-sa@project.iam.gserviceaccount.com \\
  --scopes=https://www.googleapis.com/auth/cloud-platform

# GKE
kubectl create serviceaccount my-sa
gcloud iam service-accounts add-iam-policy-binding \\
  my-sa@project.iam.gserviceaccount.com \\
  --role roles/iam.workloadIdentityUser \\
  --member "serviceAccount:project.svc.id.goog[namespace/my-sa]"
""")

con.close()

# Exemplo/Bloco 6
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

# Chain customizado - apenas env e config
con.execute("""
    CREATE SECRET s3_custom_chain (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'env;config'
    )
""")

print("Credential chain customizado criado (env → config)")

# Chain com ordem diferente
con.execute("""
    CREATE SECRET s3_config_first (
        TYPE s3,
        PROVIDER credential_chain,
        CHAIN 'config;env'
    )
""")

print("Chain com config primeiro (config → env)")

print("""
Customizando Credential Chain:
═══════════════════════════════════════════════════════

Opções AWS (separadas por ';'):
   env               → Variáveis de ambiente
   config            → ~/.aws/credentials
   sts               → AWS STS
   instance_metadata → EC2 instance metadata

Exemplos de Chains:

Development Local:
  CHAIN 'env;config'
  → Tenta variáveis de ambiente primeiro
  → Depois ~/.aws/credentials
  → Não tenta EC2 metadata (mais rápido)

Production EC2:
  CHAIN 'instance_metadata;config'
  → IAM role primeiro (preferido)
  → Fallback para config

CI/CD:
  CHAIN 'env'
  → Apenas variáveis de ambiente
  → Falha se não encontrar (comportamento desejado)

Cross-Account:
  CHAIN 'sts;config'
  → Assume role via STS
  → Fallback para credenciais diretas

Vantagens de Chain Customizado:
────────────────────────────────
✓ Performance - pula métodos não disponíveis
✓ Segurança - limita fontes de credenciais
✓ Previsibilidade - ordem explícita
✓ Debugging - mais fácil identificar fonte
""")

con.close()

# Exemplo/Bloco 7
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Managed identity (system-assigned)
con.execute("""
    CREATE SECRET azure_managed (
        TYPE azure,
        PROVIDER managed_identity,
        ACCOUNT_NAME 'mystorageaccount'
    )
""")

print("""
Azure System-Assigned Managed Identity:
════════════════════════════════════════════════════════

O que é:
────────
- Identidade criada automaticamente com o recurso Azure
- Lifecycle vinculado ao recurso
- Deletar recurso = deletar identidade
- Uma identidade por recurso

Recursos Suportados:
────────────────────
✓ Azure Virtual Machines
✓ Azure App Service
✓ Azure Functions
✓ Azure Container Instances
✓ Azure Kubernetes Service (AKS)
✓ Azure Data Factory
✓ Azure Logic Apps

Setup:

1. Habilitar Managed Identity na VM:
   ─────────────────────────────────
   az vm identity assign \\
     --name myVM \\
     --resource-group myRG

2. Dar permissões ao Storage Account:
   ──────────────────────────────────
   # Obter ID da managed identity
   PRINCIPAL_ID=$(az vm show \\
     --name myVM \\
     --resource-group myRG \\
     --query identity.principalId -o tsv)

   # Atribuir role
   az role assignment create \\
     --assignee $PRINCIPAL_ID \\
     --role "Storage Blob Data Contributor" \\
     --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>

3. Usar no DuckDB:
   ────────────────
   CREATE SECRET azure_managed (
       TYPE azure,
       PROVIDER managed_identity,
       ACCOUNT_NAME 'mystorageaccount'
   );

   SELECT * FROM 'azure://container/file.parquet';

Vantagens:
──────────
✓ Sem credenciais para gerenciar
✓ Rotação automática de tokens
✓ Auditoria via Azure AD
✓ Princípio de privilégio mínimo
✓ Mais seguro que account keys

Desvantagens:
─────────────
✗ Apenas recursos Azure
✗ Não funciona localmente
✗ Uma identidade por recurso
""")

con.close()

# Exemplo/Bloco 8
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# User-assigned managed identity
con.execute("""
    CREATE SECRET azure_user_assigned (
        TYPE azure,
        PROVIDER managed_identity,
        ACCOUNT_NAME 'mystorageaccount',
        CLIENT_ID '00000000-0000-0000-0000-000000000000'
    )
""")

print("""
Azure User-Assigned Managed Identity:
════════════════════════════════════════════════════════

O que é:
────────
- Identidade criada como recurso Azure independente
- Lifecycle independente de outros recursos
- Pode ser atribuída a múltiplos recursos
- Mais flexível que system-assigned

System-Assigned vs User-Assigned:
──────────────────────────────────

System-Assigned:
  - 1 identidade : 1 recurso
  - Lifecycle acoplado
  - Criação automática
  - Simples

User-Assigned:
  - 1 identidade : N recursos
  - Lifecycle independente
  - Criação manual
  - Mais flexível

Setup:

1. Criar User-Assigned Identity:
   ─────────────────────────────
   az identity create \\
     --name myIdentity \\
     --resource-group myRG

   # Obter Client ID
   CLIENT_ID=$(az identity show \\
     --name myIdentity \\
     --resource-group myRG \\
     --query clientId -o tsv)

2. Atribuir à VM:
   ──────────────
   az vm identity assign \\
     --name myVM \\
     --resource-group myRG \\
     --identities /subscriptions/<sub>/resourcegroups/<rg>/providers/Microsoft.ManagedIdentity/userAssignedIdentities/myIdentity

3. Dar permissões ao Storage:
   ──────────────────────────
   PRINCIPAL_ID=$(az identity show \\
     --name myIdentity \\
     --resource-group myRG \\
     --query principalId -o tsv)

   az role assignment create \\
     --assignee $PRINCIPAL_ID \\
     --role "Storage Blob Data Contributor" \\
     --scope /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>

4. Usar no DuckDB (com CLIENT_ID):
   ────────────────────────────────
   CREATE SECRET azure_user (
       TYPE azure,
       PROVIDER managed_identity,
       ACCOUNT_NAME 'mystorageaccount',
       CLIENT_ID '00000000-0000-0000-0000-000000000000'
   );

Casos de Uso:
─────────────
✓ Múltiplas VMs usando mesma identidade
✓ Identidade compartilhada por App Service + Functions
✓ Gerenciar lifecycle de identidade separadamente
✓ Ambientes com muitos recursos
""")

con.close()

# Exemplo/Bloco 9
import duckdb

con = duckdb.connect()
con.execute("INSTALL azure; LOAD azure;")

# Service principal
con.execute("""
    CREATE SECRET azure_sp (
        TYPE azure,
        PROVIDER service_principal,
        TENANT_ID '00000000-0000-0000-0000-000000000000',
        CLIENT_ID '11111111-1111-1111-1111-111111111111',
        CLIENT_SECRET 'client_secret_value_here',
        ACCOUNT_NAME 'mystorageaccount'
    )
""")

print("""
Azure Service Principal:
════════════════════════════════════════════════════════

O que é:
────────
- Identidade de aplicação no Azure Active Directory
- Similar a "service account" em outros clouds
- Credenciais: Tenant ID + Client ID + Client Secret
- Autenticação via OAuth 2.0

Setup Completo:

1. Criar Service Principal:
   ────────────────────────
   az ad sp create-for-rbac \\
     --name myDuckDBApp \\
     --role "Storage Blob Data Contributor" \\
     --scopes /subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<account>

   Output:
   {
     "appId": "11111111-1111-1111-1111-111111111111",
     "displayName": "myDuckDBApp",
     "password": "client_secret_here",
     "tenant": "00000000-0000-0000-0000-000000000000"
   }

   Mapeamento:
   - appId      → CLIENT_ID
   - password   → CLIENT_SECRET
   - tenant     → TENANT_ID

2. Usar no DuckDB:
   ────────────────
   CREATE SECRET azure_sp (
       TYPE azure,
       PROVIDER service_principal,
       TENANT_ID '00000000-0000-0000-0000-000000000000',
       CLIENT_ID '11111111-1111-1111-1111-111111111111',
       CLIENT_SECRET 'client_secret_here',
       ACCOUNT_NAME 'mystorageaccount'
   );

3. Testar:
   ────────
   SELECT * FROM 'azure://container/file.parquet';

Rotação de Client Secret:
──────────────────────────
# Criar novo secret
az ad sp credential reset \\
  --name 11111111-1111-1111-1111-111111111111

# Atualizar secret no DuckDB
DROP PERSISTENT SECRET azure_sp;
CREATE PERSISTENT SECRET azure_sp (
    TYPE azure,
    PROVIDER service_principal,
    TENANT_ID '...',
    CLIENT_ID '...',
    CLIENT_SECRET 'new_secret',
    ACCOUNT_NAME 'mystorageaccount'
);

Quando Usar:
────────────
✓ CI/CD pipelines
✓ Aplicações rodando fora da Azure
✓ Cross-tenant scenarios
✓ Múltiplas subscriptions
✓ Automação

Quando NÃO Usar:
─────────────────
✗ Recursos Azure (prefira managed_identity)
✗ Desenvolvimento local (prefira credential_chain)
""")

con.close()

# Exemplo/Bloco 10
import duckdb

print("""
Service Principal com Certificate (mais seguro):
════════════════════════════════════════════════════════

Vantagens sobre Client Secret:
───────────────────────────────
✓ Mais seguro que secrets
✓ Certificados podem ter validade curta
✓ Mais difícil de vazar acidentalmente
✓ Suporte a HSM (Hardware Security Module)

Setup:

1. Criar Certificate:
   ──────────────────
   openssl req -x509 -newkey rsa:2048 \\
     -keyout key.pem -out cert.pem \\
     -days 365 -nodes \\
     -subj "/CN=DuckDB App"

   # Converter para PFX
   openssl pkcs12 -export \\
     -out cert.pfx \\
     -inkey key.pem \\
     -in cert.pem

2. Upload Certificate to Azure AD:
   ────────────────────────────────
   az ad sp create-for-rbac \\
     --name myDuckDBApp \\
     --cert @cert.pem \\
     --create-cert

3. Usar no DuckDB:
   ────────────────
   CREATE SECRET azure_sp_cert (
       TYPE azure,
       PROVIDER service_principal,
       TENANT_ID '00000000-0000-0000-0000-000000000000',
       CLIENT_ID '11111111-1111-1111-1111-111111111111',
       CLIENT_CERTIFICATE '/path/to/cert.pem',
       ACCOUNT_NAME 'mystorageaccount'
   );

Nota: Verificar documentação DuckDB para suporte atual a certificates
""")

# Exemplo/Bloco 11
# 1. Configure variáveis de ambiente AWS
# 2. Crie um secret com credential_chain
# 3. Verifique qual provider foi usado
# 4. Teste com diferentes chains customizadas
# 5. Compare performance de chains diferentes

# Sua solução aqui

# Exemplo/Bloco 12
# 1. Crie secrets usando os 3 providers Azure:
#    - config
#    - service_principal
#    - managed_identity (se em ambiente Azure)
# 2. Liste todos e compare os providers
# 3. Documente casos de uso para cada um
# 4. Implemente rotação para service_principal

# Sua solução aqui

# Exemplo/Bloco 13
# 1. Configure credential_chain para development (env;config)
# 2. Configure service_principal para CI/CD
# 3. Configure managed_identity para production (se Azure)
# 4. Documente a estratégia de cada ambiente
# 5. Crie scripts de setup para cada ambiente

# Sua solução aqui

# Exemplo/Bloco 14
# 1. Crie um secret com credential_chain que falha
# 2. Implemente logging para debug
# 3. Teste cada método da chain individualmente
# 4. Documente erros comuns e soluções
# 5. Crie checklist de troubleshooting

# Sua solução aqui

