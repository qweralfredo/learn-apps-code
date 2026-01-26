# Curso DuckDB + Open Table File Formats

## Índice do Curso

Este curso abrangente explora a integração do DuckDB com formatos de tabela abertos (Open Table File Formats), cobrindo desde fundamentos até casos de uso práticos em produção.

### 📚 Capítulos

1. **[Introdução aos Open Table File Formats](01-introducao-open-table-formats.md)**
   - História e evolução dos formatos de tabela
   - Comparação entre Delta Lake, Iceberg, Hudi e outros
   - Quando usar cada formato

2. **[DuckDB e Arquiteturas de Lakehouse](02-duckdb-arquitetura-lakehouse.md)**
   - O que é um Lakehouse
   - DuckDB como query engine ideal
   - Padrões de arquitetura (Medallion, etc.)

3. **[Delta Lake com DuckDB](03-delta-lake-duckdb.md)**
   - Instalação e configuração
   - Time Travel e versionamento
   - Exemplos práticos com Python e SQL

4. **[Apache Iceberg com DuckDB](04-apache-iceberg-duckdb.md)**
   - Hidden Partitioning
   - Partition Evolution
   - Metadados e snapshots

5. **[Apache Hudi e Outros Formatos](05-apache-hudi-outros-formatos.md)**
   - Apache Hudi (Copy-on-Write vs Merge-on-Read)
   - Apache Paimon (Streaming Lakehouse)
   - DuckLake e formatos emergentes

6. **[Lance Format e Machine Learning](06-lance-format-machine-learning.md)**
   - Busca vetorial com LanceDB
   - RAG (Retrieval-Augmented Generation)
   - Computer Vision e embeddings

7. **[Interoperabilidade com Apache XTable](07-interoperabilidade-xtable.md)**
   - Conversão entre formatos sem duplicar dados
   - Migração gradual entre formatos
   - Multi-cloud strategy

8. **[Formatos Especializados](08-formatos-especializados.md)**
   - Apache Kudu (Storage Engine Híbrido)
   - Apache CarbonData (Índices Multidimensionais)
   - Vortex e outros formatos experimentais

9. **[Performance e Otimizações](09-performance-otimizacoes.md)**
   - Benchmarks comparativos
   - Técnicas de tuning
   - Configurações de produção

10. **[Casos de Uso e Projetos Práticos](10-casos-uso-projetos-praticos.md)**
    - Analytics Platform com Medallion Architecture
    - Sistema RAG completo
    - Pipeline CDC (Change Data Capture)
    - Multi-Cloud Data Mesh

## 🎯 Público-Alvo

- **Data Engineers**: Construir pipelines modernos de dados
- **Data Analysts**: Analytics performático em data lakes
- **ML Engineers**: Integrar vetores e busca semântica
- **Data Architects**: Projetar arquiteturas de lakehouse

## 📋 Pré-requisitos

- Conhecimento básico de SQL
- Python intermediário
- Familiaridade com conceitos de data engineering
- Docker (opcional, para alguns exemplos)

## 🛠️ Setup Inicial

### Instalar DuckDB

```bash
# Via pip
pip install duckdb

# Verificar instalação
python -c "import duckdb; print(duckdb.__version__)"
```

### Instalar Dependências Adicionais

```bash
# Extensões principais
pip install duckdb pandas pyarrow

# Para Machine Learning (Capítulo 6)
pip install lancedb sentence-transformers

# Para visualizações
pip install matplotlib seaborn plotly
```

### Estrutura de Diretórios Sugerida

```
projeto/
├── data/
│   ├── bronze/      # Dados brutos
│   ├── silver/      # Dados limpos (Delta)
│   └── gold/        # Agregações (Iceberg)
├── notebooks/       # Jupyter notebooks
├── scripts/         # Scripts Python
└── config/          # Configurações
```

## 🚀 Como Usar Este Curso

### Estudo Sequencial (Recomendado)

Siga os capítulos em ordem:
1. Leia o conteúdo teórico
2. Execute os exemplos de código
3. Complete os exercícios práticos
4. Experimente variações dos exemplos

### Estudo por Tópico

Se você já tem conhecimento básico:
- **Delta Lake**: Cap. 3
- **Iceberg**: Cap. 4
- **Hudi**: Cap. 5
- **ML/AI**: Cap. 6
- **Performance**: Cap. 9

### Projetos Práticos

Para aprendizado hands-on, vá direto para:
- **Capítulo 10**: Projetos completos
- Depois, volte aos capítulos específicos conforme necessário

## 📊 Matriz de Comparação Rápida

| Formato | Maturidade | Performance | ACID | Time Travel | Multi-Engine | Use Case |
|---------|-----------|-------------|------|-------------|--------------|----------|
| **Delta Lake** | Alta | ⭐⭐⭐ | ✅ | ✅ | ⚠️ | Spark, Simplicidade |
| **Iceberg** | Alta | ⭐⭐⭐⭐ | ✅ | ✅ | ✅✅ | Multi-engine, Scale |
| **Hudi** | Alta | ⭐⭐⭐ | ✅ | ✅ | ⚠️ | Streaming, CDC |
| **Paimon** | Média | ⭐⭐⭐⭐ | ✅ | ✅ | ⚠️ | Real-time Streaming |
| **Lance** | Baixa | ⭐⭐⭐⭐ | ⚠️ | ✅ | ⚠️ | ML, Vector Search |
| **Parquet** | Alta | ⭐⭐⭐ | ❌ | ❌ | ✅✅ | Analytics básico |

## 🔗 Recursos Complementares

### Documentação Oficial
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Delta Lake Docs](https://delta.io/)
- [Apache Iceberg Docs](https://iceberg.apache.org/)
- [Apache Hudi Docs](https://hudi.apache.org/)
- [LanceDB Docs](https://lancedb.github.io/lancedb/)

### Comunidades
- [DuckDB Discord](https://discord.gg/duckdb)
- [Delta Lake Community](https://delta.io/community/)
- [Apache Iceberg Slack](https://apache-iceberg.slack.com/)

### Artigos e Blogs
- [DuckDB Blog](https://duckdb.org/news/)
- [Databricks Blog](https://databricks.com/blog)
- [Netflix Tech Blog](https://netflixtechblog.com/)

## 🤝 Contribuindo

Este curso é um material educacional aberto. Contribuições são bem-vindas:
- Correções de erros
- Novos exemplos práticos
- Casos de uso adicionais
- Otimizações de código

## 📝 Notas do Autor

Este curso foi criado com base em:
- Documentação oficial dos projetos
- Experiência prática em produção
- Contribuições da comunidade
- Melhores práticas da indústria

**Última atualização**: Janeiro 2026  
**Versão**: 1.0  
**DuckDB**: v1.4+

## 🎓 Certificação

Ao completar este curso, você estará preparado para:
- Projetar arquiteturas de lakehouse modernas
- Implementar pipelines de dados com DuckDB
- Escolher o formato adequado para cada caso de uso
- Otimizar performance em ambientes de produção
- Integrar ML/AI com data lakes

## 📬 Feedback

Se você encontrou este curso útil ou tem sugestões de melhoria, entre em contato através das issues do repositório ou comunidades listadas acima.

---

**Bons estudos e happy querying! 🦆**

