# -*- coding: utf-8 -*-
"""
capitulo_06_gerenciamento_secrets
"""

import duckdb
import os
import boto3
from botocore.exceptions import ClientError

# ==============================================================================
# SETUP MINIO
# ==============================================================================
print(f"--- Iniciando Capítulo 06: Gerenciamento de Secrets ---")

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "learn-duckdb-s3"
SECRET_NAME = "minio_secret"

# ==============================================================================
# EXEMPLOS DUCKDB
# ==============================================================================

# Usaremos um banco persistente para testar segredos persistentes
if os.path.exists("secrets_test.db"):
    os.remove("secrets_test.db")
    
con = duckdb.connect("secrets_test.db")
con.execute("INSTALL httpfs; LOAD httpfs;")

print("\n--- 1. Criando Secret Temporário (Session Scoped) ---")
# Por padrão CREATE SECRET cria TEMPORARY se não especificar PERSISTENT (dependendo da versão)
# Mas explicitamente: CREATE TEMP SECRET ou apenas CREATE SECRET em :memory:
con.execute(f"""
CREATE OR REPLACE SECRET {SECRET_NAME} (
    TYPE S3,
    KEY_ID '{MINIO_ACCESS_KEY}',
    SECRET '{MINIO_SECRET_KEY}',
    ENDPOINT '{MINIO_ENDPOINT.replace("http://", "")}',
    URL_STYLE 'path',
    USE_SSL 'false'
);
""")
print("Secret criado.")

print("\n--- 2. Inspecionando Secrets ---")
secrets = con.execute("SELECT * FROM duckdb_secrets()").df()
print(secrets)

print("\n--- 3. Testando o Secret ---")
# Para testar, precisamos que o bucket exista (boto3 setup implícito ou prévio)
# Vamos assumir que criamos no passo anterior ou apenas listar buckets se possível (MinIO root access)
try:
    # Tentar listar arquivos (requer secret funcionando)
    # Glob fail se bucket não existir, mas secret auth acontece antes
    con.execute(f"SELECT * FROM glob('s3://{BUCKET_NAME}/*') LIMIT 1")
    print("Acesso S3 bem sucedido!")
except Exception as e:
    print(f"Erro de acesso (esperado se bucket vazio ou erro config): {e}")

print("\n--- 4. Drop Secret ---")
con.execute(f"DROP SECRET {SECRET_NAME}")
print("Secret removido.")

count = con.execute("SELECT count(*) FROM duckdb_secrets()").fetchone()[0]
print(f"Secrets restantes: {count}")

print("\n--- 5. Persistent Secrets (Teórico) ---")
# CREATE PERSISTENT SECRET ... salvaria em ~/.duckdb/stored_secrets
# Evitamos executar para não poluir o ambiente do usuário com credenciais fake de MinIO
print("Comando: CREATE PERSISTENT SECRET my_store (...)")

con.close()
if os.path.exists("secrets_test.db"):
    os.remove("secrets_test.db")


