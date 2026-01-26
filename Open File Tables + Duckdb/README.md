# 🎓 Curso Completo: DuckDB + Open Table File Formats

[![Status](https://img.shields.io/badge/Status-100%25%20Completo-brightgreen)]()
[![Notebooks](https://img.shields.io/badge/Notebooks-10%2F10-blue)]()
[![Docker](https://img.shields.io/badge/Docker-8%20Services-blue)]()

> Curso abrangente sobre DuckDB integrado com formatos de tabela abertos: Delta Lake, Apache Iceberg, Hudi, Lance e mais!

---

## 📚 O que você vai aprender

### Fundamentos
- ✅ DuckDB como engine analítica in-process
- ✅ Arquitetura Lakehouse (Medallion: Bronze/Silver/Gold)
- ✅ Formatos de tabela: Parquet, Delta, Iceberg, Hudi, Paimon, Lance
- ✅ Query Federation (múltiplas fontes de dados)

### Práticas Avançadas
- ✅ ACID transactions em Data Lakes
- ✅ Time Travel e versionamento
- ✅ Hidden Partitioning e Schema Evolution
- ✅ Vector Search e RAG Systems (Lance Format)
- ✅ CDC Pipelines (PostgreSQL → Delta Lake)
- ✅ Format Interoperability (Apache XTable)

### Produção
- ✅ Performance tuning (EXPLAIN ANALYZE, pushdowns)
- ✅ Benchmarks comparativos (10+ cenários)
- ✅ Docker Compose completo (8 serviços)
- ✅ 4 Projetos End-to-End

---

## 📖 Estrutura do Curso

### Parte 1: Teoria (10 Capítulos - ~250 páginas)

| Cap | Título | Conteúdo |
|-----|--------|----------|
| [00](00-indice.md) | **Índice** | Visão geral do curso |
| [01](01-introducao-open-table-formats.md) | **Introdução** | História, comparação formatos, decision matrix |
| [02](02-duckdb-arquitetura-lakehouse.md) | **Lakehouse** | Medallion, Federation, `MedallionPipeline` |
| [03](03-delta-lake-duckdb.md) | **Delta Lake** | ACID, Time Travel, OPTIMIZE, `DeltaManager` |
| [04](04-apache-iceberg-duckdb.md) | **Iceberg** | Hidden partitioning, metadata, snapshots |
| [05](05-apache-hudi-outros-formatos.md) | **Hudi/Paimon** | CoW/MoR, streaming, DuckLake, Vortex |
| [06](06-lance-format-machine-learning.md) | **Lance ML** | Vector search, RAG, `RAGSystem`, embeddings |
| [07](07-interoperabilidade-xtable.md) | **XTable** | Delta↔Iceberg↔Hudi, `XTableManager` |
| [08](08-formatos-especializados.md) | **Specialized** | Kudu, CarbonData, Vortex |
| [09](09-performance-otimizacoes.md) | **Performance** | Benchmarks, tuning, `FormatBenchmark` |
| [10](10-casos-uso-projetos-praticos.md) | **Projetos** | 4 projetos completos end-to-end |

📄 **[COURSE-SUMMARY.md](COURSE-SUMMARY.md)** - Resumo completo do curso

---

### Parte 2: Prática (10 Notebooks Jupyter)

Localização: `code/notebooks/`

| # | Notebook | Dataset | Conceitos |
|---|----------|---------|-----------|
| 01 | [Introdução](code/notebooks/01-introducao.ipynb) | 100k orders | Parquet vs Delta vs Iceberg benchmarks |
| 02 | [Lakehouse](code/notebooks/02-lakehouse.ipynb) | 50k+50k | Medallion (Bronze→Silver→Gold) + MinIO |
| 03 | [Delta Lake](code/notebooks/03-delta-lake.ipynb) | 10k+5k+3k | Time travel, ACID, `DeltaManager` |
| 04 | [Iceberg](code/notebooks/04-iceberg.ipynb) | 50k IoT | Hidden partitioning, `IcebergManager` |
| 05 | [Hudi/Paimon](code/notebooks/05-hudi-paimon.ipynb) | 10k activity | CoW simulation, `HudiSimulator` |
| 06 | [Lance ML](code/notebooks/06-lance-ml.ipynb) | 1k docs+vectors | Vector search, RAG, `SimpleRAG` |
| 07 | [XTable](code/notebooks/07-xtable.ipynb) | 50k sales | Format conversion, `XTableManager` |
| 08 | [Specialized](code/notebooks/08-specialized.ipynb) | Varies | Kudu, CarbonData, Vortex comparison |
| 09 | [Performance](code/notebooks/09-performance.ipynb) | 1M records | Benchmarks, optimization patterns |
| 10 | [Projects](code/notebooks/10-projects.ipynb) | 4 projects | Medallion, RAG, CDC, Multi-Cloud |

📓 **Total**: 120+ células executáveis, 2500+ linhas de código Python

---

### Parte 3: Infraestrutura Docker

Localização: `code/`

#### 8 Serviços (docker-compose.yml)

| Serviço | Porta | Descrição |
|---------|-------|-----------|
| **Jupyter Lab** | 8888 | Notebooks + Python 3.11 + 50+ libs |
| **MinIO** | 9000/9001 | S3-compatible storage + console |
| **PostgreSQL** | 5432 | Transactional DB + CDC demo |
| **Spark Master** | 7077/8080 | Apache Spark para Hudi |
| **Spark Worker** | - | Worker com 2GB RAM |
| **Flink JobManager** | 8081 | Flink para Paimon |
| **Flink TaskManager** | - | TaskManager com 2GB RAM |
| **Hive Metastore** | 9083 | Iceberg catalog |

💾 **Total RAM**: ~10GB

📚 **Docs**:
- [README.md](code/README.md) - Overview
- [QUICK-START.md](code/QUICK-START.md) - Guia rápido
- [README-DOCKER.md](code/README-DOCKER.md) - Detalhes técnicos

---

## 🚀 Quick Start (3 passos)

### 1️⃣ Clonar Repositório

```bash
cd C:\projetos\Cursos
```

### 2️⃣ Iniciar Docker (opcional - apenas para notebooks)

```bash
cd "Open File Tables + Duckdb\code"
docker-compose up -d
```

**Aguardar ~2 minutos** para todos serviços iniciarem.

### 3️⃣ Acessar Jupyter Lab

Abrir no navegador: **http://localhost:8888**

Token: `duckdb123`

Navegar para `notebooks/` e executar na ordem (01 → 10).

---

## 📖 Apenas Teoria? (sem Docker)

Se você quer apenas ler o conteúdo teórico:

```bash
cd "Open File Tables + Duckdb"

# Ler capítulos na ordem:
# 00-indice.md
# 01-introducao-open-table-formats.md
# ...
# 10-casos-uso-projetos-praticos.md
```

Não precisa instalar nada! Apenas abrir os arquivos `.md` no seu editor favorito.

---

## 🎯 Roadmap de Aprendizado

### Opção 1: Rápido (1 semana)

- **Dia 1-2**: Capítulos 01-03 + Notebooks 01-03
- **Dia 3-4**: Capítulos 04-06 + Notebooks 04-06
- **Dia 5-6**: Capítulos 07-09 + Notebooks 07-09
- **Dia 7**: Capítulo 10 + Notebook 10 (Projetos)

### Opção 2: Completo (2 semanas)

- **Semana 1**: Teoria (capítulos 01-10)
- **Semana 2**: Prática (notebooks 01-10)

### Opção 3: Profundo (1 mês)

- **Semana 1-2**: Teoria + Exercícios extras
- **Semana 3**: Notebooks + Customizações
- **Semana 4**: Projetos próprios

---

## 🧪 Validação

### Testar Ambiente Docker

```bash
cd code/
python scripts/test_environment.py
```

Deve retornar:
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

### Executar Todos Notebooks

```bash
cd code/notebooks/
python run_all_notebooks.py
```

Gera relatório de execução em `executed/report_*.txt`.

---

## 📊 Estatísticas

### Conteúdo
- **Markdown**: 10 capítulos, ~250 páginas
- **Notebooks**: 10 arquivos, 120+ células
- **Código**: 2500+ linhas Python
- **Classes**: 15+ (DeltaManager, IcebergManager, RAGSystem, etc.)

### Formatos Cobertos (11)
1. Parquet (baseline)
2. Delta Lake
3. Apache Iceberg
4. Apache Hudi
5. Apache Paimon
6. Lance Format
7. Apache Kudu
8. CarbonData
9. Vortex
10. DuckLake
11. XTable (interop)

### Tecnologias
- **DuckDB** 1.4.0+
- **Delta Lake** (deltalake 0.17+)
- **Iceberg** (pyiceberg 0.6+)
- **LanceDB** 0.6+
- **Spark** 3.5.0
- **Flink** 1.18.0
- **PostgreSQL** 16
- **MinIO** 2024

---

## 🎓 Certificação (Auto-Avaliação)

### ✅ Checklist de Aprendizado

#### Teoria (10/10)
- [ ] Capítulo 01 - Introdução aos formatos
- [ ] Capítulo 02 - Arquitetura Lakehouse
- [ ] Capítulo 03 - Delta Lake
- [ ] Capítulo 04 - Apache Iceberg
- [ ] Capítulo 05 - Hudi/Paimon/outros
- [ ] Capítulo 06 - Lance Format (ML/AI)
- [ ] Capítulo 07 - Apache XTable
- [ ] Capítulo 08 - Formatos especializados
- [ ] Capítulo 09 - Performance
- [ ] Capítulo 10 - Projetos práticos

#### Prática (10/10)
- [ ] Notebook 01 - Comparação formatos
- [ ] Notebook 02 - Medallion Lakehouse
- [ ] Notebook 03 - Delta Lake hands-on
- [ ] Notebook 04 - Iceberg metadata
- [ ] Notebook 05 - Hudi CoW/MoR
- [ ] Notebook 06 - RAG System
- [ ] Notebook 07 - XTable conversions
- [ ] Notebook 08 - Specialized formats
- [ ] Notebook 09 - Benchmarks
- [ ] Notebook 10 - 4 projetos completos

#### Projetos (4/4)
- [ ] Projeto 1 - Medallion Lakehouse
- [ ] Projeto 2 - RAG Knowledge Base
- [ ] Projeto 3 - CDC Pipeline
- [ ] Projeto 4 - Multi-Cloud Federation

**Meta**: 80%+ = Curso concluído! 🎉

---

## 📚 Recursos Adicionais

### Documentação Oficial
- [DuckDB Docs](https://duckdb.org/docs/)
- [Delta Lake Guide](https://docs.delta.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Hudi](https://hudi.apache.org/)
- [LanceDB](https://lancedb.github.io/lancedb/)

### Papers
- [Delta Lake (VLDB 2020)](https://databricks.com/research/delta-lake) - Lakehouse Architecture
- [Iceberg (VLDB 2020)](https://www.vldb.org/pvldb/vol13/p3411-lei.pdf) - Netflix Table Format
- [Hudi (VLDB 2019)](https://eng.uber.com/hoodie/) - Uber Incremental Processing

### Tutoriais
- [DuckDB + Parquet](https://duckdb.org/docs/data/parquet)
- [Delta Lake Python API](https://delta-io.github.io/delta-rs/python/)
- [PyIceberg](https://py.iceberg.apache.org/)

---

## 🤝 Contribuindo

Contribuições são bem-vindas!

### Como Contribuir
1. Fork o repositório
2. Crie branch: `git checkout -b feature/melhoria`
3. Commit: `git commit -m "Add: nova feature"`
4. Push: `git push origin feature/melhoria`
5. Abra Pull Request

### Ideias
- [ ] Adicionar exercícios com soluções
- [ ] Criar vídeos explicativos
- [ ] Traduzir para inglês
- [ ] Adicionar mais exemplos práticos
- [ ] Integrar com DBT
- [ ] Deploy Kubernetes

---

## 📄 Licença

**MIT License** - Use livremente para aprender e ensinar!

---

## ✨ Créditos

**Curso criado por**: Alfredo Rodrigues  
**Data**: Janeiro 2025  
**Versão**: 1.0.0  

**Baseado em**:
- DuckDB Official Documentation
- Delta Lake Guide (Databricks)
- Apache Iceberg Specification (Netflix)
- Lance Format (LanceDB)
- Apache XTable (Incubating)

**Agradecimentos**:
- DuckDB Team
- Delta Lake Contributors
- Apache Software Foundation
- LanceDB Team

---

## 📞 Suporte

- **Issues**: [GitHub Issues](../../issues)
- **Discussões**: [GitHub Discussions](../../discussions)
- **Email**: [seu-email@example.com]

---

## 🎉 Happy Learning!

Esperamos que este curso acelere sua jornada com DuckDB e Open Table Formats!

**Keep coding!** 🚀

---

## 📑 Índice de Arquivos

```
Open File Tables + Duckdb/
├── 00-indice.md                          # Índice do curso
├── 01-introducao-open-table-formats.md   # Cap 1: Introdução
├── 02-duckdb-arquitetura-lakehouse.md    # Cap 2: Lakehouse
├── 03-delta-lake-duckdb.md               # Cap 3: Delta Lake
├── 04-apache-iceberg-duckdb.md           # Cap 4: Iceberg
├── 05-apache-hudi-outros-formatos.md     # Cap 5: Hudi/Paimon
├── 06-lance-format-machine-learning.md   # Cap 6: Lance ML
├── 07-interoperabilidade-xtable.md       # Cap 7: XTable
├── 08-formatos-especializados.md         # Cap 8: Specialized
├── 09-performance-otimizacoes.md         # Cap 9: Performance
├── 10-casos-uso-projetos-praticos.md     # Cap 10: Projetos
├── COURSE-SUMMARY.md                     # Resumo completo
├── README.md                             # Este arquivo
└── code/                                 # Código prático
    ├── docker-compose.yml                # Orquestração Docker
    ├── .env                              # Variáveis ambiente
    ├── requirements.txt                  # Dependencies Python
    ├── README.md                         # Overview código
    ├── QUICK-START.md                    # Guia rápido
    ├── README-DOCKER.md                  # Detalhes Docker
    ├── scripts/
    │   ├── init-postgres.sql             # PostgreSQL setup
    │   └── test_environment.py           # Validação ambiente
    └── notebooks/
        ├── helpers.py                    # Funções auxiliares
        ├── 01-introducao.ipynb           # Notebook 1
        ├── 02-lakehouse.ipynb            # Notebook 2
        ├── 03-delta-lake.ipynb           # Notebook 3
        ├── 04-iceberg.ipynb              # Notebook 4
        ├── 05-hudi-paimon.ipynb          # Notebook 5
        ├── 06-lance-ml.ipynb             # Notebook 6
        ├── 07-xtable.ipynb               # Notebook 7
        ├── 08-specialized.ipynb          # Notebook 8
        ├── 09-performance.ipynb          # Notebook 9
        ├── 10-projects.ipynb             # Notebook 10
        ├── README.md                     # Guia notebooks
        └── run_all_notebooks.py          # Executar todos
```

**Navegação rápida**:
- 📖 Teoria: Capítulos 00-10 (arquivos `.md`)
- 🧪 Prática: `code/notebooks/` (arquivos `.ipynb`)
- 🐳 Docker: `code/` (docker-compose.yml)
- 📊 Resumo: [COURSE-SUMMARY.md](COURSE-SUMMARY.md)
