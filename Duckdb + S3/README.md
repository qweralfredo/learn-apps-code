# DuckDB + S3: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **DuckDB + S3**.

## 📚 Estrutura

Cada capítulo possui:
- **arquivo.py**: Script Python executável com todos os exemplos e exercícios
- **arquivo.ipynb**: Jupyter Notebook interativo

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install duckdb jupyter
```

### Configurar AWS Credentials (Opcional)

Para testar com S3 real, configure suas credenciais:

```bash
# Linux/Mac
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"
export AWS_REGION="us-east-1"

# Windows PowerShell
$env:AWS_ACCESS_KEY_ID="your_key"
$env:AWS_SECRET_ACCESS_KEY="your_secret"
```

**IMPORTANTE**: Os scripts usam credenciais mock por padrão para fins educacionais.

## 📖 Capítulos Disponíveis

| Capítulo | Descrição | Arquivos |
|----------|-----------|----------|
| 01 | Introdução DuckDB e S3 | `capitulo_01_*.py/ipynb` |
| 02 | Instalação HTTPFS Extension | `capitulo_02_*.py/ipynb` |
| 03 | Configuração Credenciais AWS | `capitulo_03_*.py/ipynb` |
| 04 | Leitura de Dados S3 | `capitulo_04_*.py/ipynb` |
| 05 | Escrita de Dados S3 | `capitulo_05_*.py/ipynb` |
| 06 | Gerenciamento de Secrets | `capitulo_06_*.py/ipynb` |
| 07 | Trabalhando com Parquet no S3 | `capitulo_07_*.py/ipynb` |
| 08 | Padrões Avançados e Globbing | `capitulo_08_*.py/ipynb` |
| 09 | Integração com Cloud Services | `capitulo_09_*.py/ipynb` |
| 10 | Otimização e Boas Práticas | `capitulo_10_*.py/ipynb` |

## 💡 Conceitos-Chave

### HTTPFS Extension
- Acesso direto a arquivos remotos via HTTP/HTTPS
- Suporte nativo a S3, Azure, GCS
- Sem necessidade de download local

### Secrets Manager
- Gerenciamento seguro de credenciais AWS
- Sem expor keys em código ou logs
- Suporte a múltiplas credenciais

### Globbing Patterns
- Ler múltiplos arquivos com wildcards
- `s3://bucket/data/*.parquet`
- Filtros eficientes no lado do servidor

## 🎯 Ordem Recomendada

1. **Cap 01-02**: Setup e configuração HTTPFS
2. **Cap 03**: Configurar credenciais (mock ou real)
3. **Cap 04-05**: Operações básicas de leitura/escrita
4. **Cap 06**: Gerenciar secrets de forma segura
5. **Cap 07**: Otimizar com Parquet
6. **Cap 08-09**: Padrões avançados
7. **Cap 10**: Performance e otimizações

## ⚠️ Avisos Importantes

1. **Custos AWS**: Operações S3 podem gerar custos. Use buckets de teste.
2. **Credenciais**: NUNCA commite credenciais reais no código.
3. **Permissões**: Garanta que suas credenciais têm permissões adequadas.

## 📝 Exemplos Práticos

### Ler CSV do S3
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
df = con.sql("SELECT * FROM 's3://bucket/data.csv'").df()
```

### Ler Parquet com Glob
```python
df = con.sql("SELECT * FROM 's3://bucket/logs/*.parquet'").df()
```

### Usar Secrets
```python
con.execute("""
    CREATE SECRET my_s3 (
        TYPE S3,
        KEY_ID 'your_key',
        SECRET 'your_secret'
    )
""")
```

## 🐛 Troubleshooting

### Extension não carrega
```python
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
```

### Erro de credenciais
Verifique variáveis de ambiente ou use secrets manager.

## 📚 Recursos Adicionais

- [DuckDB HTTPFS](https://duckdb.org/docs/extensions/httpfs)
- [AWS S3 Documentation](https://aws.amazon.com/s3/)
- [DuckDB Secrets Manager](https://duckdb.org/docs/sql/statements/create_secret)

---

**Curso**: DuckDB + S3
**Nível**: Intermediário
