# Capítulo 10: Casos de Uso e Projetos Práticos

## Introdução

Neste capítulo final, vamos construir **projetos práticos completos** que demonstram o uso real de DuckDB com open table file formats. Cada projeto é inspirado em casos de uso reais de produção.

## Projeto 1: Analytics Platform com Medallion Architecture

### Objetivo

Construir um data lakehouse completo com arquitetura Medallion (Bronze-Silver-Gold) usando DuckDB e Delta Lake.

### Arquitetura

```
┌──────────────────────────────────────────────────┐
│         Data Sources (Raw)                       │
│  CSV, JSON, APIs, Logs                          │
└─────────────┬────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────┐
│  BRONZE Layer: Raw Data Ingestion               │
│  - Format: Parquet (fast writes)                │
│  - Partitioning: date                           │
│  - Schema: Flexible (schema-on-read)            │
└─────────────┬────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────┐
│  SILVER Layer: Cleaned & Enriched               │
│  - Format: Delta Lake (ACID, versioning)        │
│  - Quality: Validated, deduplicated             │
│  - Partitioning: year, month                    │
└─────────────┬────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────────────────┐
│  GOLD Layer: Business Aggregations              │
│  - Format: Iceberg (multi-engine)               │
│  - Optimized: Pre-aggregated                    │
│  - Purpose: BI, ML, Reporting                   │
└──────────────────────────────────────────────────┘
```

### Implementação Completa

```python
# medallion_lakehouse.py
import duckdb
import pandas as pd
import requests
from datetime import datetime, timedelta
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MedallionLakehouse:
    """
    Data Lakehouse com arquitetura Medallion usando DuckDB
    """
    
    def __init__(self, base_path='./lakehouse'):
        self.base_path = Path(base_path)
        self.bronze_path = self.base_path / 'bronze'
        self.silver_path = self.base_path / 'silver'
        self.gold_path = self.base_path / 'gold'
        
        # Criar estrutura de diretórios
        for path in [self.bronze_path, self.silver_path, self.gold_path]:
            path.mkdir(parents=True, exist_ok=True)
        
        # Conectar DuckDB
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; LOAD delta;")
        self.conn.execute("INSTALL iceberg; LOAD iceberg;")
        
        logger.info("Medallion Lakehouse inicializado")
    
    def ingest_to_bronze(self, source_url, table_name):
        """
        Bronze Layer: Ingestão de dados brutos
        """
        logger.info(f"Ingestão Bronze: {table_name}")
        
        # Simular ingestão de API
        # Em produção: requests.get(source_url)
        data = pd.DataFrame({
            'id': range(1000),
            'timestamp': pd.date_range('2024-01-01', periods=1000, freq='1H'),
            'user_id': [f'user_{i % 100}' for i in range(1000)],
            'event_type': pd.np.random.choice(['click', 'view', 'purchase'], 1000),
            'value': pd.np.random.rand(1000) * 100,
            'raw_data': ['{"extra": "data"}'] * 1000
        })
        
        # Adicionar metadados de ingestão
        data['_ingested_at'] = datetime.now()
        data['_source'] = source_url
        
        # Salvar como Parquet particionado por data
        bronze_table_path = self.bronze_path / table_name
        
        self.conn.register('bronze_data', data)
        self.conn.execute(f"""
            COPY (
                SELECT 
                    *,
                    YEAR(timestamp) as year,
                    MONTH(timestamp) as month,
                    DAY(timestamp) as day
                FROM bronze_data
            ) TO '{bronze_table_path}' 
            (FORMAT PARQUET, PARTITION_BY (year, month, day))
        """)
        
        logger.info(f"✓ Bronze: {len(data)} registros ingeridos")
        return bronze_table_path
    
    def transform_to_silver(self, bronze_table, silver_table):
        """
        Silver Layer: Limpeza e validação
        """
        logger.info(f"Transformação Silver: {silver_table}")
        
        bronze_path = self.bronze_path / bronze_table
        silver_table_path = self.silver_path / silver_table
        
        # Limpeza e validação
        self.conn.execute(f"""
            COPY (
                SELECT 
                    id,
                    timestamp,
                    LOWER(TRIM(user_id)) as user_id,
                    event_type,
                    ROUND(CAST(value AS DECIMAL(10,2)), 2) as value,
                    current_timestamp() as _processed_at
                FROM read_parquet('{bronze_path}/**/*.parquet')
                WHERE 
                    user_id IS NOT NULL
                    AND value >= 0
                    AND timestamp IS NOT NULL
                -- Deduplicação
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY id ORDER BY timestamp DESC
                ) = 1
            ) TO '{silver_table_path}'
            (FORMAT DELTA)
        """)
        
        # Contar registros processados
        count = self.conn.execute(f"""
            SELECT COUNT(*) FROM delta_scan('{silver_table_path}')
        """).fetchone()[0]
        
        logger.info(f"✓ Silver: {count} registros limpos")
        return silver_table_path
    
    def aggregate_to_gold(self, silver_table, gold_table):
        """
        Gold Layer: Agregações para analytics
        """
        logger.info(f"Agregação Gold: {gold_table}")
        
        silver_path = self.silver_path / silver_table
        gold_table_path = self.gold_path / gold_table
        
        # Agregações de negócio
        self.conn.execute(f"""
            COPY (
                SELECT 
                    DATE_TRUNC('hour', timestamp) as hour,
                    event_type,
                    COUNT(DISTINCT user_id) as unique_users,
                    COUNT(*) as total_events,
                    ROUND(SUM(value), 2) as total_value,
                    ROUND(AVG(value), 2) as avg_value,
                    ROUND(MIN(value), 2) as min_value,
                    ROUND(MAX(value), 2) as max_value,
                    current_timestamp() as _aggregated_at
                FROM delta_scan('{silver_path}')
                GROUP BY hour, event_type
            ) TO '{gold_table_path}'
            (FORMAT PARQUET)  -- Ou Iceberg quando disponível
        """)
        
        count = self.conn.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{gold_table_path}')
        """).fetchone()[0]
        
        logger.info(f"✓ Gold: {count} agregações criadas")
        return gold_table_path
    
    def run_pipeline(self, source_url, table_name):
        """Pipeline completo Bronze → Silver → Gold"""
        logger.info(f"=== Iniciando Pipeline: {table_name} ===")
        
        # Bronze
        bronze_path = self.ingest_to_bronze(source_url, table_name)
        
        # Silver
        silver_path = self.transform_to_silver(table_name, table_name)
        
        # Gold
        gold_path = self.aggregate_to_gold(table_name, f"{table_name}_agg")
        
        logger.info(f"=== Pipeline Completo: {table_name} ===\n")
        
        return {
            'bronze': bronze_path,
            'silver': silver_path,
            'gold': gold_path
        }
    
    def query_gold(self, query):
        """Query na camada Gold"""
        return self.conn.execute(query).fetchdf()

# Executar Pipeline
lakehouse = MedallionLakehouse('./demo_lakehouse')

# Ingestão e transformação
result = lakehouse.run_pipeline(
    source_url='https://api.example.com/events',
    table_name='user_events'
)

# Analytics na camada Gold
analytics = lakehouse.query_gold("""
    SELECT 
        event_type,
        SUM(total_events) as events,
        ROUND(AVG(avg_value), 2) as avg_value
    FROM read_parquet('./demo_lakehouse/gold/user_events_agg')
    GROUP BY event_type
    ORDER BY events DESC
""")

print("\n=== Analytics Dashboard ===")
print(analytics)
```

## Projeto 2: Sistema RAG (Retrieval-Augmented Generation)

### Objetivo

Construir um sistema RAG completo usando LanceDB (vetores) + DuckDB (analytics) para chatbot com memória.

### Implementação

```python
# rag_system.py
import lancedb
import duckdb
from sentence_transformers import SentenceTransformer
from typing import List, Dict
import numpy as np

class RAGKnowledgeBase:
    """
    Sistema RAG com LanceDB para vetores e DuckDB para analytics
    """
    
    def __init__(self, lance_path='./rag_vectors', duckdb_path='./rag_analytics.db'):
        # LanceDB para busca vetorial
        self.lance_db = lancedb.connect(lance_path)
        
        # DuckDB para analytics
        self.duck_conn = duckdb.connect(duckdb_path)
        
        # Modelo de embeddings
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        
        print("RAG System inicializado")
    
    def ingest_documents(self, documents: List[Dict]):
        """
        Ingere documentos com embeddings
        """
        print(f"Ingerindo {len(documents)} documentos...")
        
        # Gerar embeddings
        for doc in documents:
            doc['vector'] = self.model.encode(doc['content']).tolist()
            doc['ingested_at'] = duckdb.sql("SELECT current_timestamp()").fetchone()[0]
        
        # Armazenar no LanceDB
        self.lance_table = self.lance_db.create_table(
            "knowledge_base",
            data=documents,
            mode="overwrite"
        )
        
        # Criar índice para busca rápida
        self.lance_table.create_index(metric="cosine", num_partitions=256)
        
        # Também armazenar metadados no DuckDB para analytics
        import pandas as pd
        df = pd.DataFrame([{
            'doc_id': d['id'],
            'category': d.get('category', 'general'),
            'content_length': len(d['content']),
            'ingested_at': d['ingested_at']
        } for d in documents])
        
        self.duck_conn.execute("""
            CREATE TABLE IF NOT EXISTS documents_metadata (
                doc_id INTEGER,
                category VARCHAR,
                content_length INTEGER,
                ingested_at TIMESTAMP
            )
        """)
        
        self.duck_conn.register('metadata_df', df)
        self.duck_conn.execute("""
            INSERT INTO documents_metadata 
            SELECT * FROM metadata_df
        """)
        
        print(f"✓ {len(documents)} documentos indexados")
    
    def retrieve(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Busca documentos relevantes
        """
        query_vector = self.model.encode(query).tolist()
        results = self.lance_table.search(query_vector).limit(top_k).to_list()
        return results
    
    def log_query(self, query: str, num_results: int):
        """
        Log de queries para analytics
        """
        self.duck_conn.execute("""
            CREATE TABLE IF NOT EXISTS query_log (
                query VARCHAR,
                num_results INTEGER,
                timestamp TIMESTAMP
            )
        """)
        
        self.duck_conn.execute("""
            INSERT INTO query_log VALUES (?, ?, current_timestamp())
        """, [query, num_results])
    
    def analytics_dashboard(self):
        """
        Dashboard de analytics
        """
        # Top categorias
        top_categories = self.duck_conn.execute("""
            SELECT 
                category,
                COUNT(*) as docs,
                AVG(content_length) as avg_length
            FROM documents_metadata
            GROUP BY category
            ORDER BY docs DESC
        """).fetchdf()
        
        # Queries mais frequentes
        top_queries = self.duck_conn.execute("""
            SELECT 
                query,
                COUNT(*) as frequency,
                AVG(num_results) as avg_results
            FROM query_log
            GROUP BY query
            ORDER BY frequency DESC
            LIMIT 10
        """).fetchdf()
        
        return {
            'top_categories': top_categories,
            'top_queries': top_queries
        }
    
    def answer_question(self, question: str) -> Dict:
        """
        Pipeline RAG completo
        """
        # 1. Retrieve
        relevant_docs = self.retrieve(question, top_k=3)
        
        # 2. Log
        self.log_query(question, len(relevant_docs))
        
        # 3. Construir contexto
        context = "\n\n".join([
            f"[Documento {i+1}]\n{doc['content']}"
            for i, doc in enumerate(relevant_docs)
        ])
        
        # 4. Montar resposta (em produção, enviar para LLM)
        response = {
            'question': question,
            'context': context,
            'sources': [d['id'] for d in relevant_docs],
            'relevance_scores': [d['_distance'] for d in relevant_docs]
        }
        
        return response

# Exemplo de uso
rag = RAGKnowledgeBase()

# Base de conhecimento
knowledge = [
    {
        "id": 1,
        "title": "DuckDB Overview",
        "content": "DuckDB é um banco de dados analítico in-process otimizado para OLAP...",
        "category": "database"
    },
    {
        "id": 2,
        "title": "Delta Lake Introduction",
        "content": "Delta Lake é um formato de armazenamento open-source que adiciona ACID...",
        "category": "lakehouse"
    },
    {
        "id": 3,
        "title": "Apache Iceberg Features",
        "content": "Apache Iceberg é um table format com hidden partitioning e partition evolution...",
        "category": "lakehouse"
    },
]

# Ingerir
rag.ingest_documents(knowledge)

# Fazer perguntas
answer = rag.answer_question("Como funciona o DuckDB?")

print("\n=== Resposta RAG ===")
print(f"Pergunta: {answer['question']}")
print(f"\nContexto recuperado:\n{answer['context']}")
print(f"\nFontes: {answer['sources']}")

# Analytics
analytics = rag.analytics_dashboard()
print("\n=== Analytics ===")
print(analytics['top_categories'])
```

## Projeto 3: Pipeline de CDC (Change Data Capture)

### Objetivo

Processar mudanças incrementais de um banco transacional para data lakehouse.

### Implementação

```python
# cdc_pipeline.py
import duckdb
import pandas as pd
from datetime import datetime
from typing import List, Dict

class CDCPipeline:
    """
    Pipeline CDC: PostgreSQL → Bronze → Silver (Delta) → Gold (Iceberg)
    """
    
    def __init__(self, lakehouse_path='./cdc_lakehouse'):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; INSTALL postgres;")
        self.conn.execute("LOAD delta; LOAD postgres;")
        self.lakehouse_path = lakehouse_path
    
    def capture_changes(self, table_name: str, last_processed_id: int = 0):
        """
        Captura mudanças do banco transacional
        """
        # Simular leitura de CDC do PostgreSQL
        # Em produção: ler do WAL ou tabela de auditoria
        
        changes = pd.DataFrame({
            'id': range(last_processed_id + 1, last_processed_id + 101),
            'operation': pd.np.random.choice(['INSERT', 'UPDATE', 'DELETE'], 100),
            'user_id': [f'user_{i}' for i in range(100)],
            'amount': pd.np.random.rand(100) * 1000,
            'changed_at': pd.date_range('2024-01-01', periods=100, freq='1min')
        })
        
        return changes
    
    def apply_to_silver(self, changes: pd.DataFrame, table_name: str):
        """
        Aplica mudanças CDC na camada Silver (Delta Lake)
        """
        silver_path = f"{self.lakehouse_path}/silver/{table_name}"
        
        self.conn.register('changes', changes)
        
        # Processar por tipo de operação
        inserts = changes[changes['operation'] == 'INSERT']
        updates = changes[changes['operation'] == 'UPDATE']
        deletes = changes[changes['operation'] == 'DELETE']
        
        # INSERT: Append direto
        if len(inserts) > 0:
            self.conn.execute(f"""
                COPY (SELECT * FROM inserts)
                TO '{silver_path}'
                (FORMAT DELTA, MODE APPEND)
            """)
        
        # UPDATE: Reescrever (Delta não tem UPDATE nativo no DuckDB ainda)
        # Em produção: usar Spark para MERGE
        
        # DELETE: Marcar como deletado (soft delete)
        if len(deletes) > 0:
            # Implementar lógica de soft delete
            pass
        
        print(f"✓ CDC aplicado: {len(inserts)} INSERTs, "
              f"{len(updates)} UPDATEs, {len(deletes)} DELETEs")
    
    def run_incremental(self, table_name: str):
        """
        Processamento incremental contínuo
        """
        last_id = 0
        iterations = 5
        
        for i in range(iterations):
            print(f"\n--- Iteração {i+1} ---")
            
            # Capturar mudanças
            changes = self.capture_changes(table_name, last_id)
            
            # Aplicar na Silver
            self.apply_to_silver(changes, table_name)
            
            # Atualizar checkpoint
            last_id = changes['id'].max()
            
            print(f"Checkpoint: {last_id}")

# Executar pipeline CDC
cdc = CDCPipeline()
cdc.run_incremental('sales')
```

## Projeto 4: Multi-Cloud Data Mesh

### Objetivo

Federação de dados em múltiplas clouds (AWS, GCP, Azure) usando DuckDB.

### Arquitetura

```python
# multi_cloud_mesh.py
import duckdb
from typing import Dict

class MultiCloudDataMesh:
    """
    Data Mesh com federação multi-cloud
    """
    
    def __init__(self):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; INSTALL iceberg;")
        self.conn.execute("LOAD delta; LOAD iceberg;")
        
        self.setup_secrets()
    
    def setup_secrets(self):
        """Configurar credenciais multi-cloud"""
        # AWS
        self.conn.execute("""
            CREATE SECRET aws_prod (
                TYPE S3,
                KEY_ID 'AWS_KEY',
                SECRET 'AWS_SECRET',
                REGION 'us-east-1'
            )
        """)
        
        # GCP
        self.conn.execute("""
            CREATE SECRET gcp_analytics (
                TYPE GCS,
                KEY_ID 'GCP_CLIENT_EMAIL',
                SECRET 'GCP_PRIVATE_KEY'
            )
        """)
        
        # Azure
        self.conn.execute("""
            CREATE SECRET azure_backup (
                TYPE AZURE,
                CONNECTION_STRING 'DefaultEndpointsProtocol=https;...'
            )
        """)
    
    def federated_query(self):
        """
        Query federada cross-cloud
        """
        result = self.conn.execute("""
            WITH 
                aws_sales AS (
                    SELECT 'AWS' as cloud, *
                    FROM delta_scan('s3://prod-bucket/sales')
                ),
                gcp_products AS (
                    SELECT 'GCP' as cloud, *
                    FROM iceberg_scan('gs://analytics/products')
                ),
                azure_customers AS (
                    SELECT 'Azure' as cloud, *
                    FROM read_parquet('az://backup/customers.parquet')
                )
            SELECT 
                aws.cloud as sales_cloud,
                gcp.cloud as product_cloud,
                azure.cloud as customer_cloud,
                COUNT(*) as transactions,
                SUM(aws.amount) as revenue
            FROM aws_sales aws
            JOIN gcp_products gcp ON aws.product_id = gcp.id
            JOIN azure_customers azure ON aws.customer_id = azure.id
            GROUP BY aws.cloud, gcp.cloud, azure.cloud
        """).fetchdf()
        
        return result

# Executar
mesh = MultiCloudDataMesh()
result = mesh.federated_query()
print(result)
```

## Conclusão e Próximos Passos

### O que Aprendemos

Ao longo deste curso, exploramos:

1. **Fundamentos**: História e evolução dos table formats
2. **Formatos Principais**: Delta Lake, Iceberg, Hudi
3. **Formatos Especializados**: Lance, Kudu, CarbonData
4. **Interoperabilidade**: Apache XTable
5. **Performance**: Otimizações e tuning
6. **Projetos Práticos**: Casos de uso reais

### Recursos Adicionais

#### Documentação Oficial
- [DuckDB Docs](https://duckdb.org/docs/)
- [Delta Lake](https://delta.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Hudi](https://hudi.apache.org/)
- [LanceDB](https://lancedb.github.io/lancedb/)

#### Comunidades
- [DuckDB Discord](https://discord.gg/duckdb)
- [Delta Lake Slack](https://delta-users.slack.com/)
- [Apache Iceberg Slack](https://apache-iceberg.slack.com/)

#### Cursos e Tutoriais
- [DuckDB in Action (Manning)](https://www.manning.com/books/duckdb-in-action)
- [Data Engineering with DuckDB](https://www.udemy.com/course/data-engineering-with-duckdb/)

### Roadmap de Estudos

#### Iniciante → Intermediário (1-2 meses)
1. Dominar SQL no DuckDB
2. Entender Parquet e compressão
3. Aprender Delta Lake básico
4. Praticar com datasets reais

#### Intermediário → Avançado (2-4 meses)
1. Iceberg avançado (partition evolution)
2. Otimizações de performance
3. Arquiteturas de lakehouse
4. Integração com Spark/Flink

#### Avançado → Expert (4-6 meses)
1. Contribuir para projetos open-source
2. Arquitetar sistemas complexos
3. Tuning de performance em produção
4. Mentoria e evangelização

### Projetos Sugeridos

1. **Data Lake Pessoal**: Organize seus dados pessoais (finanças, saúde, etc.)
2. **Analytics Dashboard**: Dashboard com Streamlit + DuckDB + Delta
3. **ETL Pipeline**: Pipeline completo com Airflow + DuckDB
4. **RAG Chatbot**: Chatbot com memória usando Lance + DuckDB
5. **Time Series Platform**: Análise de séries temporais com CarbonData

### Certificações e Validação

Embora não existam certificações oficiais ainda, você pode:
- Contribuir para projetos open-source
- Publicar artigos técnicos
- Palestrar em meetups/conferências
- Criar cursos e tutoriais

---

## Agradecimentos

Obrigado por acompanhar este curso! O ecossistema de data lakehouse está em constante evolução, e esperamos que este conteúdo tenha fornecido uma base sólida para você explorar e contribuir com essa área empolgante.

**Happy Querying! 🦆**

---

**Exercício Final**:

Construa um projeto completo integrando múltiplos formatos:

```python
# Seu desafio: Criar um dashboard analítico que:
# 1. Ingere dados de múltiplas fontes
# 2. Processa com Medallion Architecture
# 3. Usa Delta (Silver) + Iceberg (Gold)
# 4. Implementa RAG para Q&A sobre os dados
# 5. Gera relatórios automáticos

# Boa sorte! 🚀
```
