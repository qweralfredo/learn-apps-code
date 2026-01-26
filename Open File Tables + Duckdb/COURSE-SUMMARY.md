# 🎓 Curso Completo - DuckDB + Open Table Formats

## ✅ Status: 100% CONCLUÍDO

---

## 📚 Estrutura do Curso

### 📖 Parte 1: Teoria (10 Capítulos Markdown)

Localização: `C:\projetos\Cursos\Open File Tables + Duckdb\`

1. ✅ **[01-introducao-open-table-formats.md](../01-introducao-open-table-formats.md)**
   - História e evolução dos formatos
   - Comparação Delta Lake vs Iceberg vs Hudi vs Paimon
   - Decision matrix (quando usar cada formato)
   - 25+ páginas de conteúdo

2. ✅ **[02-duckdb-arquitetura-lakehouse.md](../02-duckdb-arquitetura-lakehouse.md)**
   - Medallion Architecture (Bronze → Silver → Gold)
   - Query Federation (múltiplas fontes)
   - Classe `MedallionPipeline` completa
   - Exemplos com MinIO S3

3. ✅ **[03-delta-lake-duckdb.md](../03-delta-lake-duckdb.md)**
   - ACID transactions explicadas
   - Time travel com `delta_scan(..., version=N)`
   - OPTIMIZE, VACUUM, Z-ordering
   - `DeltaManager` helper class

4. ✅ **[04-apache-iceberg-duckdb.md](../04-apache-iceberg-duckdb.md)**
   - Hidden partitioning (vs Hive partitioning)
   - Metadata tables (`iceberg_metadata()`)
   - Snapshot isolation
   - Schema evolution

5. ✅ **[05-apache-hudi-outros-formatos.md](../05-apache-hudi-outros-formatos.md)**
   - Hudi CoW (Copy-on-Write) vs MoR (Merge-on-Read)
   - Apache Paimon (Flink-native)
   - DuckLake, Vortex
   - Comparação performance

6. ✅ **[06-lance-format-machine-learning.md](../06-lance-format-machine-learning.md)**
   - Lance Format para ML/AI
   - Vector embeddings storage
   - KNN search nativo
   - RAG System architecture
   - Classe `RAGSystem`, `ImageVectorStore`

7. ✅ **[07-interoperabilidade-xtable.md](../07-interoperabilidade-xtable.md)**
   - Apache XTable (Incubating)
   - Conversão Delta ↔ Iceberg ↔ Hudi
   - Classe `XTableManager`
   - Multi-engine support (Spark, Trino, Flink)

8. ✅ **[08-formatos-especializados.md](../08-formatos-especializados.md)**
   - Apache Kudu (OLTP + OLAP)
   - CarbonData (indexação avançada)
   - Vortex (structured arrays)
   - Classe `MultiFormatDataPipeline`

9. ✅ **[09-performance-otimizacoes.md](../09-performance-otimizacoes.md)**
   - Benchmarks comparativos (10+ cenários)
   - Query optimization patterns
   - EXPLAIN ANALYZE debugging
   - Classe `FormatBenchmark`, `DuckDBMonitor`
   - Memory profiling, execution plans

10. ✅ **[10-casos-uso-projetos-praticos.md](../10-casos-uso-projetos-praticos.md)**
    - 4 Projetos Completos:
      - Medallion Lakehouse
      - RAG Knowledge Base
      - CDC Pipeline (PostgreSQL → Delta)
      - Multi-Cloud Data Mesh

**Total Teoria**: ~250 páginas de conteúdo técnico detalhado

---

### 🧪 Parte 2: Prática (10 Notebooks Jupyter)

Localização: `C:\projetos\Cursos\Open File Tables + Duckdb\code\notebooks\`

1. ✅ **[01-introducao.ipynb](notebooks/01-introducao.ipynb)**
   - Dataset: 100k orders
   - Benchmarks: Parquet vs Delta vs Iceberg
   - Visualizações matplotlib
   - Decision matrix interativa

2. ✅ **[02-lakehouse.ipynb](notebooks/02-lakehouse.ipynb)**
   - Implementação completa Medallion
   - MinIO S3 integration (boto3)
   - Bronze: raw data
   - Silver: cleaned, partitioned by date
   - Gold: business aggregates
   - Federation queries

3. ✅ **[03-delta-lake.ipynb](notebooks/03-delta-lake.ipynb)**
   - Time travel hands-on (10k + 5k + 3k rows)
   - Classe `DeltaManager`:
     - `create_table()`
     - `append_data()`
     - `read_version()`
     - `compare_versions()`
     - `show_history()`
   - OPTIMIZE, VACUUM examples

4. ✅ **[04-iceberg.ipynb](notebooks/04-iceberg.ipynb)**
   - Dataset IoT: 50k sensor readings
   - Hidden partitioning demo
   - Metadata exploration (glob `**/*.metadata.json`)
   - Classe `IcebergManager`:
     - `create_table()`
     - `query()`
     - `get_metadata()`
   - Partition pruning benchmarks

5. ✅ **[05-hudi-paimon.ipynb](notebooks/05-hudi-paimon.ipynb)**
   - Dataset: 10k user activity logs
   - CoW simulation (full rewrites)
   - Classe `HudiSimulator`:
     - `create_table()`
     - `upsert()`
     - `query()`
     - `history()`
   - Spark integration example
   - Paimon comparison matrix

6. ✅ **[06-lance-ml.ipynb](notebooks/06-lance-ml.ipynb)**
   - 1000 documentos com embeddings (384-dim)
   - LanceDB connection
   - KNN semantic search (top-k)
   - Classe `SimpleRAG`:
     - `retrieve()` - vector search
     - `augment()` - context injection
     - `generate()` - mock LLM
     - `query()` - full pipeline
   - Lance vs Parquet benchmarks (random access)

7. ✅ **[07-xtable.ipynb](notebooks/07-xtable.ipynb)**
   - 50k sales records
   - Delta → Iceberg conversion
   - Classe `XTableManager`:
     - `sync_delta_to_iceberg()`
     - `sync_iceberg_to_hudi()`
     - `validate_sync()`
   - Bidirectional sync example
   - Use cases: multi-engine, migration, vendor independence

8. ✅ **[08-specialized.ipynb](notebooks/08-specialized.ipynb)**
   - Kudu simulation (10k IoT readings)
   - OLTP (point lookups) vs OLAP (aggregations)
   - CarbonData: MDK index, Bloom filters
   - Vortex: numerical arrays (1M elements)
   - Format comparison matrix
   - Decision tree diagram

9. ✅ **[09-performance.ipynb](notebooks/09-performance.ipynb)**
   - Dataset: 1M records
   - Benchmarks:
     - CSV vs Parquet (none/snappy/gzip)
     - Write performance
     - Read performance (4 formats)
   - Query optimization:
     - EXPLAIN ANALYZE examples
     - Projection pushdown (speedup 2-5x)
     - Filter pushdown (speedup 3-10x)
   - Memory profiling (tracemalloc)
   - Best practices checklist

10. ✅ **[10-projects.ipynb](notebooks/10-projects.ipynb)**
    - **Projeto 1 - Medallion Lakehouse**: 100k orders → Bronze/Silver/Gold
    - **Projeto 2 - RAG System**: 8 documentos knowledge base + semantic search
    - **Projeto 3 - CDC Pipeline**: PostgreSQL → Delta Lake
    - **Projeto 4 - Multi-Cloud Federation**: S3 + Delta + PostgreSQL queries
    - Complete end-to-end scenarios

**Total Prática**: 10 notebooks executáveis com 100+ células de código

---

### 🐳 Parte 3: Infraestrutura (Docker Compose)

Localização: `C:\projetos\Cursos\Open File Tables + Duckdb\code\`

#### ✅ Serviços (8 containers, ~10GB RAM)

1. **Jupyter Lab** (`jupyter/datascience-notebook:latest`)
   - Port: 8888
   - Token: `duckdb123`
   - Python 3.11 + 50+ libraries
   - DuckDB, Delta Lake, Iceberg, LanceDB

2. **MinIO** (S3-compatible storage)
   - Ports: 9000 (API), 9001 (Console)
   - Credentials: minioadmin/minioadmin
   - Buckets: bronze, silver, gold, delta, iceberg, hudi, lance

3. **PostgreSQL** (Transactional database)
   - Port: 5432
   - Database: `demo_cdc`
   - User: duckdb/duckdb123
   - Pre-loaded: customers, orders, products, CDC triggers

4. **Spark Master**
   - Ports: 7077 (Spark), 8080 (UI)
   - Memory: auto
   - For Hudi/Paimon processing

5. **Spark Worker**
   - Memory: 2GB
   - Cores: 2
   - Connects to Master

6. **Flink JobManager**
   - Port: 8081 (Dashboard)
   - Memory: 1GB
   - For stream processing

7. **Flink TaskManager**
   - Memory: 2GB
   - Slots: 2
   - For Paimon examples

8. **Hive Metastore**
   - Port: 9083
   - For Iceberg catalog

#### ✅ Arquivos de Configuração

- **docker-compose.yml**: Orquestração completa
- **.env**: Variáveis de ambiente (MinIO, PostgreSQL, Spark, Flink)
- **requirements.txt**: 50+ Python packages
- **scripts/init-postgres.sql**: Dados demo + CDC triggers
- **scripts/test_environment.py**: Validação automatizada

#### ✅ Documentação

- **README.md**: Overview do projeto
- **QUICK-START.md**: Guia rápido (6 seções)
- **README-DOCKER.md**: Detalhes técnicos Docker

---

## 📊 Estatísticas do Curso

### Conteúdo
- **Markdown**: 10 capítulos, ~250 páginas
- **Notebooks**: 10 arquivos .ipynb, ~120 células
- **Código**: ~2500 linhas Python
- **Classes Helper**: 15+ (DeltaManager, IcebergManager, RAGSystem, XTableManager, etc.)

### Formatos Cobertos
1. ✅ Parquet (baseline)
2. ✅ Delta Lake (Databricks)
3. ✅ Apache Iceberg (Netflix)
4. ✅ Apache Hudi (Uber)
5. ✅ Apache Paimon (Alibaba)
6. ✅ Lance Format (LanceDB)
7. ✅ Apache Kudu (Cloudera)
8. ✅ CarbonData (Huawei)
9. ✅ Vortex (Array-focused)
10. ✅ DuckLake (DuckDB native)
11. ✅ XTable (Interoperability)

### Tecnologias
- **DuckDB**: v1.4.0+
- **Delta Lake**: deltalake 0.17+
- **Apache Iceberg**: pyiceberg 0.6+
- **LanceDB**: lancedb 0.6+
- **Spark**: 3.5.0
- **Flink**: 1.18.0
- **PostgreSQL**: 16
- **MinIO**: RELEASE.2024-01-01T00-00-00Z

### Datasets
- **Sintéticos**: 1.5M+ registros totais
- **Reais**: PostgreSQL CDC logs, vector embeddings
- **Variados**: Orders, IoT sensors, user activity, documents, images

---

## 🎯 O que você aprenderá

### 🟢 Nível Básico
- ✅ Conceitos de table formats
- ✅ Parquet vs columnar vs row formats
- ✅ DuckDB como engine analítica
- ✅ Leitura/escrita de arquivos

### 🟡 Nível Intermediário
- ✅ Delta Lake: ACID, time travel
- ✅ Apache Iceberg: metadata, partitioning
- ✅ Lakehouse Architecture (Medallion)
- ✅ S3/MinIO integration
- ✅ Query optimization patterns

### 🔴 Nível Avançado
- ✅ Hudi CoW/MoR strategies
- ✅ Lance Format: vector search, RAG
- ✅ XTable: format interoperability
- ✅ CDC pipelines (PostgreSQL → Delta)
- ✅ Multi-cloud federation
- ✅ Performance tuning (EXPLAIN ANALYZE)
- ✅ Specialized formats (Kudu, CarbonData, Vortex)

---

## 🚀 Como Usar Este Curso

### 📖 Opção 1: Apenas Teoria (sem Docker)

```bash
cd "C:\projetos\Cursos\Open File Tables + Duckdb"

# Ler capítulos na ordem:
# 01-introducao-open-table-formats.md
# 02-duckdb-arquitetura-lakehouse.md
# ... até 10-casos-uso-projetos-praticos.md
```

### 🧪 Opção 2: Teoria + Prática (com Docker)

```bash
cd "C:\projetos\Cursos\Open File Tables + Duckdb\code"

# 1. Iniciar ambiente
docker-compose up -d

# 2. Aguardar (~2 min)
docker-compose ps

# 3. Acessar Jupyter
# http://localhost:8888 (token: duckdb123)

# 4. Executar notebooks na ordem:
# notebooks/01-introducao.ipynb
# notebooks/02-lakehouse.ipynb
# ... até notebooks/10-projects.ipynb
```

### 🎓 Opção 3: Curso Completo (recomendado)

1. **Dia 1-3**: Ler capítulos 01-03 (teoria) + executar notebooks 01-03
2. **Dia 4-6**: Ler capítulos 04-06 + executar notebooks 04-06
3. **Dia 7-9**: Ler capítulos 07-09 + executar notebooks 07-09
4. **Dia 10**: Projetos completos (capítulo 10 + notebook 10)

**Total**: ~40 horas de estudo (10 dias x 4h/dia)

---

## ✅ Validação e Testes

### 1. Testar Ambiente Docker

```bash
cd code/
python scripts/test_environment.py
```

Espera-se:
```
✓ All imports successful
✓ DuckDB v1.4.0+ available
✓ MinIO accessible (9000, 9001)
✓ PostgreSQL accessible (5432)
✓ Spark Master accessible (8080)
✓ All 10 notebooks found
━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Environment validation PASSED
```

### 2. Executar Todos Notebooks

```bash
cd notebooks/
jupyter nbconvert --to notebook --execute *.ipynb --output-dir=executed/
```

### 3. Validar Outputs

Cada notebook deve produzir:
- ✅ Prints de status
- ✅ DataFrames pandas
- ✅ Visualizações matplotlib
- ✅ Benchmarks (tempo de execução)
- ✅ Sem erros (exit code 0)

---

## 📚 Recursos Adicionais

### Documentação Oficial
- **DuckDB**: https://duckdb.org/docs/
- **Delta Lake**: https://docs.delta.io/
- **Apache Iceberg**: https://iceberg.apache.org/docs/latest/
- **Apache Hudi**: https://hudi.apache.org/docs/overview
- **LanceDB**: https://lancedb.github.io/lancedb/

### Papers de Referência
- Delta Lake (VLDB 2020): Lakehouse Architecture
- Iceberg (VLDB 2020): Table Format Evolution
- Hudi (VLDB 2019): Incremental Processing at Uber

### Tutoriais Complementares
- DuckDB + Parquet: https://duckdb.org/docs/data/parquet
- Delta Lake Python API: https://delta-io.github.io/delta-rs/python/
- Iceberg Python (PyIceberg): https://py.iceberg.apache.org/

---

## 🎓 Certificação (Auto-Avaliação)

### ✅ Checklist de Aprendizado

#### Conceitos Teóricos
- [ ] Posso explicar a diferença entre Parquet, Delta, Iceberg, Hudi
- [ ] Entendo Medallion Architecture (Bronze/Silver/Gold)
- [ ] Sei quando usar cada formato (decision matrix)
- [ ] Compreendo ACID transactions em data lakes
- [ ] Entendo hidden partitioning vs Hive partitioning
- [ ] Conheço CoW vs MoR strategies
- [ ] Sei o que é XTable e para que serve

#### Prática
- [ ] Consigo ler/escrever Parquet com DuckDB
- [ ] Sei criar tabelas Delta Lake com time travel
- [ ] Posso query Iceberg tables com metadata
- [ ] Implementei uma pipeline Medallion completa
- [ ] Configurei MinIO (S3) e fiz queries remotas
- [ ] Criei um RAG system básico com Lance
- [ ] Integrei PostgreSQL + DuckDB (CDC)
- [ ] Otimizei queries (EXPLAIN ANALYZE)

#### Projetos
- [ ] Implementei Projeto 1 (Medallion Lakehouse)
- [ ] Implementei Projeto 2 (RAG System)
- [ ] Implementei Projeto 3 (CDC Pipeline)
- [ ] Implementei Projeto 4 (Multi-Cloud Federation)

**Meta**: 80%+ checklist = Curso concluído! 🎉

---

## 🤝 Contribuições

Este é um curso open-source. Contribuições são bem-vindas!

### Como Contribuir

1. **Issues**: Reportar erros, sugerir melhorias
2. **Pull Requests**: Corrigir typos, adicionar exemplos
3. **Notebooks**: Criar exercícios adicionais
4. **Documentação**: Traduzir para outros idiomas

### Roadmap Futuro

- [ ] Adicionar exercícios com soluções
- [ ] Criar vídeos explicativos
- [ ] Traduzir para inglês
- [ ] Adicionar testes automatizados
- [ ] Deploy em Kubernetes (exemplos)
- [ ] Integrar com DBT (data transformations)

---

## 📄 Licença

MIT License - Use livremente para aprender e ensinar!

---

## ✨ Créditos

**Curso criado por**: Alfredo Rodrigues  
**Data**: Janeiro 2025  
**Versão**: 1.0.0  

**Baseado em**:
- DuckDB Official Docs
- Delta Lake Guide (Databricks)
- Apache Iceberg Spec (Netflix)
- Lance Format (LanceDB)
- Apache XTable (Incubating)

**Agradecimentos**:
- DuckDB Team (in-process analytics engine)
- Delta Lake Contributors
- Apache Software Foundation (Iceberg, Hudi, Paimon)
- LanceDB Team

---

## 🎉 Happy Learning!

Esperamos que este curso seja útil na sua jornada com DuckDB e Open Table Formats!

**Dúvidas?** Abra uma issue no repositório.  
**Feedback?** Entre em contato via email/LinkedIn.

**Keep coding!** 🚀
