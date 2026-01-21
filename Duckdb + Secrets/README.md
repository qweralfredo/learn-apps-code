# DuckDB + Secrets: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **DuckDB + Secrets**.

## 📚 Estrutura

Cada capítulo possui:
- **arquivo.py**: Script Python executável
- **arquivo.ipynb**: Jupyter Notebook interativo

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install duckdb jupyter
```

Para usar com cloud storage:
```bash
# Instalar extensões necessárias dentro do DuckDB
# con.execute("INSTALL httpfs; LOAD httpfs;")
# con.execute("INSTALL azure; LOAD azure;")
```

## 📖 Capítulos Disponíveis

| Capítulo | Descrição | Arquivos |
|----------|-----------|----------|
| 01 | Introdução a Secrets | `capitulo_01_*.py/ipynb` |
| 02 | Tipos de Secrets | `capitulo_02_*.py/ipynb` |
| 03 | Cloud Storage Secrets | `capitulo_03_*.py/ipynb` |
| 04 | Database Secrets | `capitulo_04_*.py/ipynb` |
| 05 | Persistent Secrets | `capitulo_05_*.py/ipynb` |
| 06 | Secret Providers | `capitulo_06_*.py/ipynb` |
| 07 | Scope e Named Secrets | `capitulo_07_*.py/ipynb` |
| 08 | Secrets com Extensions | `capitulo_08_*.py/ipynb` |
| 09 | Segurança e Best Practices | `capitulo_09_*.py/ipynb` |
| 10 | Casos de Uso Avançados | `capitulo_10_*.py/ipynb` |

## 💡 Conceitos-Chave

### O que são Secrets?
Mecanismo unificado de gerenciamento de credenciais no DuckDB:
- Armazena credenciais de forma segura
- Uso automático baseado em URL
- Suporte a múltiplos backends (S3, Azure, GCS, etc)

### Tipos de Secrets
- **S3**: AWS S3 credentials
- **Azure**: Azure Blob Storage
- **GCS**: Google Cloud Storage
- **HTTP**: Bearer tokens
- **Database**: MySQL, PostgreSQL credentials

### Temporary vs Persistent
- **Temporary**: Existem apenas na sessão atual
- **Persistent**: Salvos em arquivo (DuckDB 0.10+)

## 🎯 Ordem Recomendada

1. **Cap 01**: Conceitos fundamentais
2. **Cap 02**: Tipos disponíveis
3. **Cap 03**: Cloud storage (S3, Azure, GCS)
4. **Cap 04**: Conexões database
5. **Cap 05**: Persistência de secrets
6. **Cap 06**: Secret providers
7. **Cap 07**: Scoping e named secrets
8. **Cap 08**: Integração com extensions
9. **Cap 09**: Segurança e melhores práticas
10. **Cap 10**: Casos de uso avançados

## 📝 Exemplos Práticos

### Criar Secret S3
```python
con.execute("""
    CREATE SECRET my_s3 (
        TYPE S3,
        KEY_ID 'AKIAIOSFODNN7EXAMPLE',
        SECRET 'wJalrXUtnFEMI...',
        REGION 'us-east-1'
    )
""")
```

### Listar Secrets
```python
secrets = con.execute("SELECT * FROM duckdb_secrets()").df()
print(secrets[['name', 'type', 'scope']])
```

### Verificar qual Secret é usado
```python
result = con.execute("""
    SELECT which_secret('s3://my-bucket/data.parquet')
""").fetchone()
print(f"Secret usado: {result[0]}")
```

### Persistent Secret
```python
con = duckdb.connect('my_db.db')
con.execute("""
    CREATE PERSISTENT SECRET my_persistent_s3 (
        TYPE S3,
        KEY_ID 'key',
        SECRET 'secret'
    )
""")
```

## ⚠️ Segurança

1. **NUNCA** commite credenciais reais no código
2. Use variáveis de ambiente para credenciais reais
3. Os exemplos usam credenciais MOCK para fins educacionais
4. Em produção, use secret providers (AWS Secrets Manager, etc)

## 🔒 Boas Práticas

### ✅ Bom
```python
# Usar secrets
con.execute("CREATE SECRET (...)")
con.sql("SELECT * FROM 's3://bucket/file.parquet'")
```

### ❌ Ruim
```python
# Expor credenciais diretamente
con.execute("SET s3_access_key_id = 'AKIAIO...'")
```

## 🐛 Troubleshooting

### Verificar secrets disponíveis
```python
con.sql("SELECT * FROM duckdb_secrets()").show()
```

### Remover secret
```python
con.execute("DROP SECRET secret_name")
```

### Secret não é usado automaticamente
Verifique o SCOPE - deve corresponder ao padrão da URL.

## 📚 Recursos Adicionais

- [DuckDB Secrets Manager](https://duckdb.org/docs/sql/statements/create_secret)
- [Cloud Storage Authentication](https://duckdb.org/docs/guides/import/s3_import)

---

**Curso**: DuckDB + Secrets
**Nível**: Intermediário
