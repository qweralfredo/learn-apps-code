# DuckDB + Delta: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **DuckDB + Delta**.

## 📚 O que é Delta Lake?

Delta Lake é um formato de armazenamento open-source que traz confiabilidade e performance para data lakes:
- Transações ACID
- Time travel (versionamento)
- Schema evolution
- Upserts e deletes eficientes

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install duckdb jupyter
```

Dentro do DuckDB, instalar extensão Delta:
```python
con.execute("INSTALL delta;")
con.execute("LOAD delta;")
```

## 📖 Capítulos Disponíveis

| Capítulo | Descrição | Arquivos |
|----------|-----------|----------|
| 01 | Introdução ao DuckDB | `capitulo_01_*.py/ipynb` |
| 02 | Instalação e Configuração | `capitulo_02_*.py/ipynb` |
| 03 | Introdução à Extensão Delta | `capitulo_03_*.py/ipynb` |
| 04 | Leitura de Tabelas Delta | `capitulo_04_*.py/ipynb` |
| 05 | Trabalhando com Cloud Storage | `capitulo_05_*.py/ipynb` |
| 06 | Secrets e Autenticação | `capitulo_06_*.py/ipynb` |
| 07 | Otimizações e Performance | `capitulo_07_*.py/ipynb` |
| 08 | Particionamento e Data Skipping | `capitulo_08_*.py/ipynb` |
| 09 | Integração Python/Spark | `capitulo_09_*.py/ipynb` |
| 10 | Casos de Uso Práticos | `capitulo_10_*.py/ipynb` |

## 💡 Conceitos-Chave

### Delta Lake Features
- **ACID Transactions**: Garantia de consistência
- **Time Travel**: Acesso a versões históricas
- **Schema Evolution**: Mudanças seguras de schema
- **Compaction**: Otimização de arquivos pequenos

### DuckDB + Delta
- Leitura nativa de Delta tables
- Query direta sem conversão
- Suporte a particionamento
- Integração com cloud storage

## 🎯 Ordem Recomendada

1. **Cap 01-02**: Setup DuckDB e Delta extension
2. **Cap 03**: Conceitos Delta Lake
3. **Cap 04**: Leitura básica de Delta tables
4. **Cap 05**: Cloud storage (S3, Azure)
5. **Cap 06**: Autenticação segura
6. **Cap 07-08**: Otimizações e particionamento
7. **Cap 09**: Interoperabilidade com Spark
8. **Cap 10**: Casos de uso práticos

## 📝 Exemplos Práticos

### Ler Delta Table
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

df = con.execute("""
    SELECT * FROM delta_scan('path/to/delta_table')
""").df()
```

### Time Travel
```python
# Ler versão específica
df = con.execute("""
    SELECT * FROM delta_scan('delta_table', version = 5)
""").df()

# Ler em timestamp específico
df = con.execute("""
    SELECT * FROM delta_scan('delta_table',
        timestamp = '2024-01-01 00:00:00')
""").df()
```

### Metadados
```python
# Ver histórico de versões
history = con.execute("""
    SELECT * FROM delta_metadata('delta_table')
""").df()
```

## 🔧 Troubleshooting

### Extension não carrega
```python
con.execute("FORCE INSTALL delta;")
con.execute("LOAD delta;")
```

### Erro ao ler Delta table
Verifique se o diretório contém `_delta_log/`.

## 📚 Recursos Adicionais

- [Delta Lake Docs](https://docs.delta.io/)
- [DuckDB Delta Extension](https://github.com/duckdb/duckdb_delta)
- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

---

**Curso**: DuckDB + Delta
**Nível**: Intermediário a Avançado
