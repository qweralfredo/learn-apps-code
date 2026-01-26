# Capítulo 01: Introdução aos Open Table File Formats

## O que são Open Table File Formats?

Open Table File Formats são especificações abertas que definem como dados tabulares são armazenados e gerenciados em data lakes. Eles vão além de formatos de arquivo simples como Parquet ou CSV, adicionando camadas de metadados que permitem recursos avançados como transações ACID, time travel, schema evolution e otimizações de consulta.

## A Evolução do Armazenamento de Dados

### Era 1: Arquivos Simples (2000-2015)
- **CSV**: Simples, mas sem tipagem forte
- **Avro**: Esquema embutido, mas não otimizado para analytics
- **Parquet**: Colunar, eficiente, mas sem gerenciamento de transações

### Era 2: Formato Hive (2010-2018)
Apache Hive introduziu o conceito de "tabelas" sobre arquivos no HDFS:

```sql
-- Exemplo de tabela Hive-style no DuckDB
CREATE TABLE vendas (
    data DATE,
    produto VARCHAR,
    valor DECIMAL(10,2)
);

-- Estrutura de diretórios Hive
-- /data/vendas/ano=2024/mes=01/part-00000.parquet
-- /data/vendas/ano=2024/mes=02/part-00000.parquet
```

**Limitações**:
- Sem transações ACID completas
- Sem time travel
- Difícil gerenciar atualizações e deletes
- Metadados limitados

### Era 3: Table Formats Modernos (2019-presente)

Três grandes projetos surgiram para resolver essas limitações:

| Formato | Criador | Ano | Foco Principal |
|---------|---------|-----|----------------|
| **Delta Lake** | Databricks | 2019 | Simplicidade, Spark |
| **Apache Iceberg** | Netflix | 2018 | Performance, Múltiplas engines |
| **Apache Hudi** | Uber | 2019 | Streaming, Upserts |

## Principais Formatos e Suas Características

### 1. Delta Lake
**Origem**: Databricks  
**Foco**: Simplicidade e integração com Apache Spark

```sql
-- Instalando a extensão Delta no DuckDB
INSTALL delta;
LOAD delta;

-- Lendo uma tabela Delta
SELECT * FROM delta_scan('s3://meu-bucket/tabela-delta');

-- Time Travel
SELECT * FROM delta_scan('s3://meu-bucket/tabela-delta', version => 5);
```

**Características**:
- ✅ Transações ACID
- ✅ Time Travel (versioning)
- ✅ Schema Evolution
- ✅ Upserts e Deletes eficientes
- ✅ Auditoria completa (transaction log JSON)

### 2. Apache Iceberg
**Origem**: Netflix  
**Foco**: Performance e engine-agnostic

```sql
-- Instalando a extensão Iceberg no DuckDB
INSTALL iceberg;
LOAD iceberg;

-- Lendo uma tabela Iceberg
SELECT * FROM iceberg_scan('s3://meu-bucket/tabela-iceberg');

-- Consultando metadados
SELECT * FROM iceberg_metadata('s3://meu-bucket/tabela-iceberg', 'snapshots');
```

**Características**:
- ✅ Particionamento oculto (hidden partitioning)
- ✅ Schema evolution sem reescrita
- ✅ Time travel via snapshots
- ✅ Partition evolution
- ✅ Otimizado para múltiplas engines (Spark, Flink, Trino, DuckDB)

### 3. Apache Hudi
**Origem**: Uber  
**Foco**: Streaming e atualizações incrementais

```sql
-- DuckDB com Hudi (via Parquet scan)
SELECT * FROM read_parquet('s3://meu-bucket/hudi-table/*.parquet');

-- Filtros com Hudi partitioning
SELECT * FROM read_parquet('s3://bucket/hudi-table/*/*.parquet')
WHERE _hoodie_commit_time > '20240101000000';
```

**Características**:
- ✅ Copy-on-Write (CoW) e Merge-on-Read (MoR)
- ✅ Otimizado para streaming
- ✅ Índices avançados
- ✅ Compactação automática

## Formatos Emergentes (2024-2025)

### 4. Apache Paimon (ex-Flink Table Store)
**Foco**: Streaming em tempo real

```sql
-- Exemplo conceitual (suporte em desenvolvimento)
CREATE TABLE eventos_tempo_real (
    timestamp TIMESTAMP,
    usuario_id INTEGER,
    evento VARCHAR
) USING paimon
OPTIONS (
    'bucket' = '8',
    'changelog-producer' = 'input'
);
```

**Destaques**:
- Streaming lakehouse
- Integração nativa com Flink
- Atualizações de alta velocidade

### 5. Lance Format
**Foco**: AI, ML e Busca Vetorial

```python
# Exemplo com Python e DuckDB
import duckdb
import lancedb

# Lance é otimizado para embeddings e vetores
db = lancedb.connect("./lancedb")
table = db.create_table("embeddings",
    data=[
        {"id": 1, "vector": [0.1, 0.2, 0.3], "text": "exemplo 1"},
        {"id": 2, "vector": [0.4, 0.5, 0.6], "text": "exemplo 2"}
    ]
)

# Consulta vetorial
results = table.search([0.15, 0.25, 0.35]).limit(5).to_list()
```

**Destaques**:
- Formato colunar moderno em Rust
- Acesso aleatório ultra-rápido
- Busca vetorial nativa (essencial para LLMs)
- Otimizado para computer vision

### 6. DuckLake
**Foco**: Simplicidade single-node

```sql
-- DuckLake: Metadados em SQL em vez de JSON
CREATE TABLE produtos_ducklake AS
SELECT * FROM read_parquet('data/*.parquet');

-- Metadados armazenados no próprio DuckDB
CREATE TABLE _ducklake_metadata (
    table_name VARCHAR,
    version INTEGER,
    snapshot_id VARCHAR,
    created_at TIMESTAMP
);
```

**Destaques**:
- Proposta de 2025 do ecossistema DuckDB/MotherDuck
- Gerencia metadados via SQL (sem JSON/Avro complexo)
- Ideal para cargas menores ou locais

## Formatos de Interoperabilidade

### 7. Apache XTable (ex-OneTable)
**Tipo**: Camada de tradução entre formatos

```bash
# XTable permite ler/escrever em múltiplos formatos
# Escreve em Hudi, expõe como Delta e Iceberg automaticamente
java -jar xtable.jar --sourceFormat HUDI \
    --targetFormats DELTA,ICEBERG \
    --datasetPath s3://bucket/table
```

**Destaques**:
- Não é um formato, mas um tradutor
- Elimina vendor lock-in
- Suporta Delta ↔ Iceberg ↔ Hudi

### 8. Apache Kudu
**Tipo**: Storage engine híbrido

```sql
-- Kudu tem seu próprio storage (não usa Parquet no S3)
-- Integração via JDBC/ODBC
```

**Destaques**:
- Analytics rápido em dados que mudam rapidamente
- Storage proprietário (não S3/HDFS)
- Ponte entre HDFS (batch) e HBase (real-time)

### 9. Apache CarbonData
**Foco**: Queries ultra-rápidas com índices

```sql
-- CarbonData usa índices MDK (Multidimensional Key)
-- Evita full scan com filtros pesados
SELECT * FROM carbon_table
WHERE cidade = 'São Paulo' 
  AND categoria = 'Eletrônicos'
  AND data BETWEEN '2024-01-01' AND '2024-12-31';
```

**Destaques**:
- Índices multidimensionais
- Popular no ecossistema Huawei/Ásia
- Otimizado para filtros complexos

### 10. Vortex
**Status**: Formato experimental (2024-2025)

## Comparação Rápida

| Formato | Transações | Time Travel | Streaming | Busca Vetorial | Maturidade |
|---------|------------|-------------|-----------|----------------|------------|
| Delta Lake | ✅ | ✅ | ⚠️ | ❌ | Alta |
| Iceberg | ✅ | ✅ | ✅ | ❌ | Alta |
| Hudi | ✅ | ✅ | ✅✅ | ❌ | Alta |
| Paimon | ✅ | ✅ | ✅✅✅ | ❌ | Média |
| Lance | ⚠️ | ❌ | ❌ | ✅✅✅ | Baixa |
| DuckLake | ✅ | ⚠️ | ❌ | ❌ | Experimental |

## Por que usar DuckDB com esses formatos?

### Vantagens do DuckDB

1. **Zero-ETL**: Consulte diretamente sem cópias
2. **Performance**: Processamento colunar otimizado
3. **Portabilidade**: Roda em laptop, servidor ou cloud
4. **Simplicidade**: SQL padrão, sem clusters complexos
5. **Extensível**: Suporte via extensões

```sql
-- Exemplo: Analytics cross-format no DuckDB
WITH 
    delta_data AS (SELECT * FROM delta_scan('s3://bucket/delta-table')),
    iceberg_data AS (SELECT * FROM iceberg_scan('s3://bucket/iceberg-table'))
SELECT 
    d.categoria,
    COUNT(DISTINCT d.produto_id) as produtos_delta,
    COUNT(DISTINCT i.produto_id) as produtos_iceberg
FROM delta_data d
JOIN iceberg_data i ON d.categoria = i.categoria
GROUP BY d.categoria;
```

## Quando usar cada formato?

### Use Delta Lake se:
- Já usa Databricks ou Spark
- Quer simplicidade e maturidade
- Precisa de forte auditoria

### Use Iceberg se:
- Precisa de múltiplas engines (Trino, Flink, Spark, DuckDB)
- Quer performance máxima em leitura
- Tem schema evolution complexo

### Use Hudi se:
- Foca em streaming e CDC
- Precisa de upserts frequentes
- Usa Flink ou Kafka Streams

### Use Paimon se:
- Streaming lakehouse é crítico
- Já usa Flink extensivamente

### Use Lance se:
- Trabalha com embeddings/ML
- Precisa de busca vetorial
- Foca em computer vision ou LLMs

## Próximos Passos

No próximo capítulo, vamos explorar como DuckDB se posiciona como o query engine ideal para arquiteturas de lakehouse, permitindo analytics federado sobre múltiplos formatos.

## Recursos Adicionais

- [DuckDB Documentation](https://duckdb.org/docs/)
- [Delta Lake Specification](https://github.com/delta-io/delta)
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Apache Hudi Documentation](https://hudi.apache.org/)
- [LanceDB Documentation](https://lancedb.github.io/lancedb/)

---

**Exercício Prático**: Instale a extensão Delta no DuckDB e explore os metadados de uma tabela Delta de exemplo.

```sql
INSTALL delta;
LOAD delta;

-- Explore metadados
DESCRIBE SELECT * FROM delta_scan('s3://exemplo/tabela-delta');
```
