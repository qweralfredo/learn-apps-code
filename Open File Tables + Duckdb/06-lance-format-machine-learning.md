# Capítulo 06: Lance Format e Machine Learning com DuckDB

## Introdução ao Lance Format

**Lance** é um formato colunar moderno, escrito em Rust, especificamente projetado para workloads de **Machine Learning**, **Computer Vision** e **Busca Vetorial**. É o formato base do **LanceDB**, um banco de dados vetorial de alta performance.

### Por que Lance?

Formatos tradicionais como Parquet são excelentes para analytics, mas têm limitações para ML/AI:

| Limitação Parquet | Solução Lance |
|-------------------|---------------|
| Acesso aleatório lento | **Random access ultra-rápido** (importante para treinamento) |
| Sem suporte vetorial nativo | **Busca vetorial integrada** (ANN - Approximate Nearest Neighbor) |
| Difícil versionar datasets | **Versionamento nativo** (como Git para dados) |
| Não otimizado para imagens | **Suporte nativo para tensors** e blobs binários |

### Características Principais

- ✅ **Fast Random Access**: Acesso a linhas individuais em microsegundos
- ✅ **Vector Search**: Índices IVF, PQ para busca de similaridade
- ✅ **Versioning**: Zero-copy versioning (snapshots baratos)
- ✅ **Multi-modal**: Texto, imagens, embeddings no mesmo dataset
- ✅ **Interoperável**: Funciona com Arrow, Pandas, DuckDB

## Instalando LanceDB

### Python

```bash
pip install lancedb duckdb pyarrow
```

### Verificar Instalação

```python
import lancedb
import duckdb
import pyarrow as pa

print("LanceDB version:", lancedb.__version__)
print("DuckDB version:", duckdb.__version__)
```

## Criando um Dataset Lance

### Exemplo Básico

```python
import lancedb
import pandas as pd
import numpy as np

# Conectar ao LanceDB (cria diretório local)
db = lancedb.connect("./lancedb_data")

# Criar dados de exemplo
data = pd.DataFrame({
    'id': range(1, 101),
    'text': [f'Documento {i}' for i in range(1, 101)],
    'category': np.random.choice(['A', 'B', 'C'], 100),
    'value': np.random.rand(100) * 1000,
    # Embeddings (vetores 128-dimensionais)
    'vector': [np.random.rand(128).tolist() for _ in range(100)]
})

# Criar tabela Lance
table = db.create_table("documentos", data=data, mode="overwrite")

print(f"Tabela criada com {table.count_rows()} registros")
```

### Com Metadados Estruturados

```python
import lancedb
import pyarrow as pa

# Schema explícito com tipos Arrow
schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("title", pa.string()),
    pa.field("published_date", pa.date32()),
    pa.field("embedding", pa.list_(pa.float32(), 384)),  # Vetor 384-d
    pa.field("metadata", pa.struct([
        pa.field("author", pa.string()),
        pa.field("tags", pa.list_(pa.string()))
    ]))
])

# Criar dados
data = [
    {
        "id": 1,
        "title": "Introdução ao ML",
        "published_date": "2024-01-15",
        "embedding": np.random.rand(384).tolist(),
        "metadata": {
            "author": "Alice",
            "tags": ["ML", "Python"]
        }
    },
    # ... mais registros
]

# Criar tabela
db = lancedb.connect("./lance_articles")
table = db.create_table("articles", data=data, schema=schema)
```

## Integrando Lance com DuckDB

### Leitura via Arrow

```python
import lancedb
import duckdb

# Conectar ao Lance
db = lancedb.connect("./lancedb_data")
table = db.open_table("documentos")

# Converter para Arrow Table
arrow_table = table.to_arrow()

# Consultar com DuckDB
conn = duckdb.connect()
conn.register('lance_docs', arrow_table)

result = conn.execute("""
    SELECT 
        category,
        COUNT(*) as docs,
        ROUND(AVG(value), 2) as avg_value,
        ROUND(SUM(value), 2) as total_value
    FROM lance_docs
    GROUP BY category
    ORDER BY docs DESC
""").fetchdf()

print(result)
```

### Pipeline Completo: Lance → DuckDB Analytics

```python
import lancedb
import duckdb
import pandas as pd
import numpy as np

class LanceDuckDBPipeline:
    def __init__(self, lance_path, duckdb_path=":memory:"):
        self.lance_db = lancedb.connect(lance_path)
        self.duck_conn = duckdb.connect(duckdb_path)
    
    def create_lance_dataset(self, table_name, df):
        """Cria dataset no Lance"""
        self.lance_db.create_table(table_name, data=df, mode="overwrite")
        print(f"Tabela '{table_name}' criada no Lance")
    
    def query_with_duckdb(self, table_name, query):
        """Consulta dados Lance usando DuckDB"""
        lance_table = self.lance_db.open_table(table_name)
        arrow_table = lance_table.to_arrow()
        
        self.duck_conn.register('lance_data', arrow_table)
        
        result = self.duck_conn.execute(query).fetchdf()
        return result
    
    def analytics_report(self, table_name):
        """Gera relatório analítico"""
        return self.query_with_duckdb(table_name, """
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT category) as categories,
                ROUND(AVG(value), 2) as avg_value,
                ROUND(MIN(value), 2) as min_value,
                ROUND(MAX(value), 2) as max_value,
                ROUND(STDDEV(value), 2) as stddev_value
            FROM lance_data
        """)

# Uso
pipeline = LanceDuckDBPipeline("./lance_analytics")

# Criar dataset
df = pd.DataFrame({
    'id': range(1, 1001),
    'category': np.random.choice(['Tech', 'Finance', 'Health'], 1000),
    'value': np.random.rand(1000) * 10000,
    'vector': [np.random.rand(128).tolist() for _ in range(1000)]
})

pipeline.create_lance_dataset("metrics", df)

# Analytics com DuckDB
report = pipeline.analytics_report("metrics")
print("\n=== Relatório Analítico ===")
print(report)

# Query customizada
custom = pipeline.query_with_duckdb("metrics", """
    SELECT 
        category,
        COUNT(*) as count,
        ROUND(AVG(value), 2) as avg,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median
    FROM lance_data
    GROUP BY category
    ORDER BY avg DESC
""")
print("\n=== Por Categoria ===")
print(custom)
```

## Busca Vetorial com Lance

### Conceito de Busca Vetorial

Busca vetorial (similarity search) é essencial para:
- **RAG (Retrieval-Augmented Generation)**: LLMs com contexto
- **Sistemas de Recomendação**: Produtos similares
- **Busca Semântica**: Encontrar documentos por significado
- **Computer Vision**: Busca por imagens similares

### Criando Embeddings

```python
import lancedb
import numpy as np
from sentence_transformers import SentenceTransformer

# Modelo para gerar embeddings de texto
model = SentenceTransformer('all-MiniLM-L6-v2')

# Dados de exemplo
documents = [
    {"id": 1, "text": "Machine learning com Python"},
    {"id": 2, "text": "Deep learning e redes neurais"},
    {"id": 3, "text": "Análise de dados com pandas"},
    {"id": 4, "text": "Visualização de dados"},
    {"id": 5, "text": "Processamento de linguagem natural"},
]

# Gerar embeddings
for doc in documents:
    doc['vector'] = model.encode(doc['text']).tolist()

# Criar tabela Lance
db = lancedb.connect("./lance_vectors")
table = db.create_table("documents", data=documents, mode="overwrite")

print(f"{len(documents)} documentos com embeddings armazenados")
```

### Busca por Similaridade

```python
# Query: encontrar documentos similares
query_text = "algoritmos de aprendizado de máquina"
query_vector = model.encode(query_text).tolist()

# Buscar top 3 mais similares
results = table.search(query_vector).limit(3).to_list()

print(f"Query: '{query_text}'\n")
print("Documentos mais similares:")
for i, result in enumerate(results, 1):
    print(f"{i}. {result['text']} (distance: {result['_distance']:.4f})")
```

**Output esperado**:
```
Query: 'algoritmos de aprendizado de máquina'

Documentos mais similares:
1. Machine learning com Python (distance: 0.2341)
2. Deep learning e redes neurais (distance: 0.4123)
3. Processamento de linguagem natural (distance: 0.5234)
```

### Indexação para Performance

```python
# Criar índice IVF-PQ para busca ultra-rápida
table.create_index(
    metric="cosine",  # ou "L2" para distância euclidiana
    num_partitions=256,
    num_sub_vectors=16
)

# Agora a busca é muito mais rápida em milhões de vetores
results = table.search(query_vector).limit(10).to_list()
```

## Caso de Uso: RAG (Retrieval-Augmented Generation)

### Pipeline RAG Completo

```python
import lancedb
import duckdb
from sentence_transformers import SentenceTransformer
import openai  # ou qualquer LLM

class RAGSystem:
    def __init__(self, lance_path):
        self.lance_db = lancedb.connect(lance_path)
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.duck_conn = duckdb.connect()
    
    def ingest_documents(self, documents):
        """Ingere documentos com embeddings"""
        for doc in documents:
            doc['vector'] = self.model.encode(doc['content']).tolist()
        
        self.table = self.lance_db.create_table(
            "knowledge_base", 
            data=documents, 
            mode="overwrite"
        )
        
        # Criar índice para busca rápida
        self.table.create_index(metric="cosine", num_partitions=256)
        
        print(f"{len(documents)} documentos indexados")
    
    def retrieve(self, query, top_k=5):
        """Busca documentos relevantes"""
        query_vector = self.model.encode(query).tolist()
        results = self.table.search(query_vector).limit(top_k).to_list()
        return results
    
    def analyze_with_duckdb(self, documents):
        """Análise dos documentos recuperados"""
        arrow_table = self.table.to_arrow()
        self.duck_conn.register('docs', arrow_table)
        
        stats = self.duck_conn.execute("""
            SELECT 
                category,
                COUNT(*) as doc_count,
                AVG(LENGTH(content)) as avg_length
            FROM docs
            GROUP BY category
        """).fetchdf()
        
        return stats
    
    def augmented_query(self, user_query):
        """Pipeline RAG completo"""
        # 1. Retrieve: Buscar documentos relevantes
        relevant_docs = self.retrieve(user_query, top_k=3)
        
        # 2. Construir contexto
        context = "\n\n".join([
            f"Documento {i+1}: {doc['content']}" 
            for i, doc in enumerate(relevant_docs)
        ])
        
        # 3. Augment: Combinar contexto com query
        prompt = f"""
        Baseado nos seguintes documentos:
        
        {context}
        
        Responda a pergunta: {user_query}
        """
        
        # 4. Generate: Enviar para LLM (exemplo conceitual)
        # response = openai.ChatCompletion.create(
        #     model="gpt-4",
        #     messages=[{"role": "user", "content": prompt}]
        # )
        
        return {
            'query': user_query,
            'retrieved_docs': relevant_docs,
            'prompt': prompt,
            # 'llm_response': response
        }

# Uso
rag = RAGSystem("./rag_knowledge")

# Ingerir base de conhecimento
knowledge = [
    {
        "id": 1, 
        "title": "Python Básico",
        "content": "Python é uma linguagem de programação de alto nível...",
        "category": "Programming"
    },
    {
        "id": 2,
        "title": "Machine Learning",
        "content": "Machine Learning é um subcampo da inteligência artificial...",
        "category": "AI"
    },
    # ... mais documentos
]

rag.ingest_documents(knowledge)

# Fazer query
result = rag.augmented_query("Como aprender Python?")
print("Documentos recuperados:")
for doc in result['retrieved_docs']:
    print(f"- {doc['title']}")
```

## Computer Vision com Lance

### Armazenar Imagens e Embeddings

```python
import lancedb
import numpy as np
from PIL import Image
import torch
from torchvision import models, transforms

class ImageVectorStore:
    def __init__(self, lance_path):
        self.db = lancedb.connect(lance_path)
        # Modelo pré-treinado para embeddings de imagens
        self.model = models.resnet50(pretrained=True)
        self.model.eval()
        
        self.preprocess = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(
                mean=[0.485, 0.456, 0.406],
                std=[0.229, 0.224, 0.225]
            )
        ])
    
    def image_to_embedding(self, image_path):
        """Converte imagem em vetor de embedding"""
        image = Image.open(image_path).convert('RGB')
        input_tensor = self.preprocess(image).unsqueeze(0)
        
        with torch.no_grad():
            embedding = self.model(input_tensor)
        
        return embedding.squeeze().numpy().tolist()
    
    def ingest_images(self, image_data):
        """Ingere imagens com metadados"""
        for item in image_data:
            item['embedding'] = self.image_to_embedding(item['path'])
        
        self.table = self.db.create_table(
            "images", 
            data=image_data, 
            mode="overwrite"
        )
        
        # Índice para busca rápida
        self.table.create_index(metric="L2", num_partitions=128)
        
        print(f"{len(image_data)} imagens indexadas")
    
    def find_similar_images(self, query_image_path, top_k=5):
        """Busca imagens similares"""
        query_embedding = self.image_to_embedding(query_image_path)
        results = self.table.search(query_embedding).limit(top_k).to_list()
        return results

# Uso
store = ImageVectorStore("./image_vectors")

images = [
    {"id": 1, "path": "cat1.jpg", "label": "cat"},
    {"id": 2, "path": "dog1.jpg", "label": "dog"},
    {"id": 3, "path": "cat2.jpg", "label": "cat"},
    # ... mais imagens
]

store.ingest_images(images)

# Buscar imagens similares
similar = store.find_similar_images("query_cat.jpg", top_k=3)
print("Imagens similares:")
for img in similar:
    print(f"- {img['path']} (label: {img['label']}, distance: {img['_distance']:.4f})")
```

## Versionamento de Datasets

Lance suporta versionamento nativo (como Git para dados):

```python
import lancedb
import pandas as pd

db = lancedb.connect("./versioned_data")

# Versão 1: Criar dataset inicial
v1_data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Carol'],
    'value': [100, 200, 300]
})

table = db.create_table("users", data=v1_data, mode="overwrite")
print(f"Versão 1: {table.count_rows()} registros")

# Versão 2: Adicionar dados
v2_data = pd.DataFrame({
    'id': [4, 5],
    'name': ['Dave', 'Eve'],
    'value': [400, 500]
})

table = db.open_table("users")
table.add(v2_data)
print(f"Versão 2: {table.count_rows()} registros")

# Versão 3: Atualizar valores
table.update(where="id = 1", values={"value": 150})
print(f"Versão 3: Alice atualizada")

# Listar versões
versions = table.list_versions()
print(f"\nTotal de versões: {len(versions)}")

# Voltar para versão anterior
table.checkout(version=1)
print(f"\nVersão 1 restaurada: {table.count_rows()} registros")
```

## Performance: Lance vs Parquet

### Benchmark Random Access

```python
import lancedb
import pyarrow.parquet as pq
import time
import numpy as np

# Criar dataset grande
n_rows = 1_000_000
data = {
    'id': range(n_rows),
    'vector': [np.random.rand(128).tolist() for _ in range(n_rows)]
}

# Lance
db = lancedb.connect("./benchmark_lance")
lance_table = db.create_table("test", data=data, mode="overwrite")

# Parquet
import pandas as pd
df = pd.DataFrame(data)
df.to_parquet("benchmark.parquet")

# Benchmark: Random access
indices = np.random.randint(0, n_rows, 1000)

# Lance
start = time.time()
for idx in indices:
    row = lance_table.to_arrow().slice(idx, 1)
lance_time = time.time() - start

# Parquet
start = time.time()
parquet_table = pq.read_table("benchmark.parquet")
for idx in indices:
    row = parquet_table.slice(idx, 1)
parquet_time = time.time() - start

print(f"Lance random access: {lance_time:.2f}s")
print(f"Parquet random access: {parquet_time:.2f}s")
print(f"Speedup: {parquet_time/lance_time:.2f}x")
```

## Limitações e Considerações

### Lance é ideal para:
- ✅ ML training pipelines (random access rápido)
- ✅ Vector search / RAG systems
- ✅ Multi-modal datasets (texto + imagens + vetores)
- ✅ Datasets que evoluem (versionamento)

### NÃO use Lance para:
- ❌ Analytics OLAP puro (use Parquet/Iceberg/Delta)
- ❌ Data warehousing tradicional
- ❌ Cargas que não precisam de vetores
- ❌ Ambientes sem Python/Rust

## Próximos Passos

No próximo capítulo, vamos explorar **Apache XTable**, a camada de interoperabilidade que permite converter entre Delta, Iceberg e Hudi.

## Recursos

- [LanceDB Documentation](https://lancedb.github.io/lancedb/)
- [Lance Format Spec](https://github.com/lancedb/lance)
- [Vector Search Guide](https://lancedb.github.io/lancedb/concepts/vector_search/)

---

**Exercício Prático**:

Crie um sistema de busca semântica simples:

```python
import lancedb
from sentence_transformers import SentenceTransformer

# Setup
db = lancedb.connect("./semantic_search")
model = SentenceTransformer('all-MiniLM-L6-v2')

# Dados
docs = [
    "Python é uma linguagem de programação",
    "Machine learning é fascinante",
    "DuckDB é um banco de dados analítico",
    "Análise de dados com pandas",
    "Deep learning com PyTorch"
]

# Criar embeddings e tabela
data = [
    {"id": i, "text": doc, "vector": model.encode(doc).tolist()}
    for i, doc in enumerate(docs)
]

table = db.create_table("docs", data=data, mode="overwrite")

# Buscar
query = "aprendizado de máquina"
query_vec = model.encode(query).tolist()
results = table.search(query_vec).limit(3).to_list()

print(f"Query: '{query}'\n")
for r in results:
    print(f"- {r['text']}")
```
