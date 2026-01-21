# DuckDB + Iceberg: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **DuckDB + Iceberg**.

## 📚 O que é Apache Iceberg?

Apache Iceberg é um formato de table aberto para dados analíticos:
- Table format (não storage format)
- Schema evolution sem reescrever dados
- Particionamento hidden (não aparece em queries)
- Time travel e snapshots
- Metadata tracking eficiente

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install duckdb jupyter
```

Instalar extensão Iceberg:
```python
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")
```

## 📖 Capítulos Disponíveis

| Capítulo | Descrição | Arquivos |
|----------|-----------|----------|
| 01 | Introdução ao Apache Iceberg | `capitulo_01_*.py/ipynb` |
| 02 | Instalação e Configuração | `capitulo_02_*.py/ipynb` |
| 03 | Leitura de Tabelas Iceberg | `capitulo_03_*.py/ipynb` |
| 04 | Metadados e Snapshots | `capitulo_04_*.py/ipynb` |
| 05 | Time Travel e Versionamento | `capitulo_05_*.py/ipynb` |
| 06 | Catálogos REST e Autenticação | `capitulo_06_*.py/ipynb` |
| 07 | Escrita de Dados | `capitulo_07_*.py/ipynb` |
| 08 | Particionamento e Organização | `capitulo_08_*.py/ipynb` |
| 09 | Integração Cloud Storage | `capitulo_09_*.py/ipynb` |
| 10 | Casos de Uso e Melhores Práticas | `capitulo_10_*.py/ipynb` |

## 💡 Conceitos-Chave

### Iceberg Architecture
- **Metadata Layer**: Tracking de snapshots e schemas
- **Manifest Files**: Lista de data files
- **Data Files**: Parquet, ORC, Avro
- **Catalog**: Metastore (REST, Hive, Glue)

### DuckDB + Iceberg
- Leitura nativa de Iceberg tables
- Suporte a REST catalogs
- Time travel com snapshots
- Query sem conversão

## 🎯 Ordem Recomendada

1. **Cap 01-02**: Fundamentos e setup
2. **Cap 03**: Leitura básica de Iceberg tables
3. **Cap 04**: Entender metadados e snapshots
4. **Cap 05**: Time travel
5. **Cap 06**: REST catalogs
6. **Cap 07**: Escrita de dados
7. **Cap 08**: Particionamento eficiente
8. **Cap 09**: Cloud integration
9. **Cap 10**: Melhores práticas

## 📝 Exemplos Práticos

### Ler Iceberg Table
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")

df = con.execute("""
    SELECT * FROM iceberg_scan('path/to/iceberg_table')
""").df()
```

### Time Travel com Snapshot
```python
# Ler snapshot específico
df = con.execute("""
    SELECT * FROM iceberg_scan('table',
        snapshot_id = 8744736658442914487)
""").df()
```

### Metadados
```python
# Ver snapshots disponíveis
snapshots = con.execute("""
    SELECT * FROM iceberg_snapshots('table')
""").df()

# Ver schema
schema = con.execute("""
    SELECT * FROM iceberg_schema('table')
""").df()
```

### REST Catalog
```python
con.execute("""
    CREATE SECRET iceberg_rest (
        TYPE ICEBERG,
        CATALOG 'rest',
        URI 'https://catalog.example.com',
        TOKEN 'my_token'
    )
""")

df = con.execute("""
    SELECT * FROM iceberg_scan('catalog.namespace.table')
""").df()
```

## 🔧 Troubleshooting

### Extension não disponível
```python
con.execute("FORCE INSTALL iceberg;")
con.execute("LOAD iceberg;")
```

### Erro ao acessar REST catalog
Verifique credenciais e URL do catalog.

## 📚 Recursos Adicionais

- [Apache Iceberg Docs](https://iceberg.apache.org/)
- [DuckDB Iceberg Extension](https://github.com/duckdb/duckdb_iceberg)
- [Iceberg Table Spec](https://iceberg.apache.org/spec/)

---

**Curso**: DuckDB + Iceberg
**Nível**: Intermediário a Avançado
