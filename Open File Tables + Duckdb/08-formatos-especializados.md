# Capítulo 08: Formatos Especializados

## Além dos "Big Three"

Enquanto Delta Lake, Iceberg e Hudi dominam o mercado de table formats, existem formatos especializados que resolvem problemas específicos com abordagens únicas. Neste capítulo, exploramos Apache Kudu, CarbonData e formatos emergentes.

## Apache Kudu: Storage Engine Híbrido

### Visão Geral

**Apache Kudu** é diferente dos outros formatos: não é uma camada sobre arquivos Parquet no S3/HDFS, mas sim um **storage engine completo** com seu próprio formato binário e gerenciamento de dados.

**Criado**: Cloudera (2015)  
**Status**: Apache Top-Level Project  
**Foco**: Fast analytics em dados que mudam rapidamente

### Arquitetura do Kudu

```
┌──────────────────────────────────────────────────┐
│              Kudu Architecture                    │
├──────────────────────────────────────────────────┤
│                                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │
│  │   Master    │  │   Master    │  │  Master  │ │
│  │   Server    │  │   Server    │  │  Server  │ │
│  └──────┬──────┘  └──────┬──────┘  └────┬─────┘ │
│         │                 │               │       │
│         └─────────────────┴───────────────┘       │
│                      │                            │
│         ┌────────────┴────────────┐               │
│         │                         │               │
│    ┌────▼─────┐              ┌───▼──────┐        │
│    │  Tablet  │              │  Tablet  │        │
│    │  Server  │              │  Server  │        │
│    │          │              │          │        │
│    │ ┌──────┐ │              │ ┌──────┐ │        │
│    │ │Tablet│ │              │ │Tablet│ │        │
│    │ └──────┘ │              │ └──────┘ │        │
│    └──────────┘              └──────────┘        │
└──────────────────────────────────────────────────┘
```

**Componentes**:
- **Master Servers**: Gerenciam metadados e coordenação
- **Tablet Servers**: Armazenam dados (tablets = partições)
- **Tablets**: Partições horizontais de dados

### Características Principais

| Característica | Descrição |
|----------------|-----------|
| **Fast Scans** | Otimizado para leitura analítica (como Parquet) |
| **Fast Random Access** | Acesso por chave rápido (como HBase) |
| **Updates/Deletes** | Mutabilidade eficiente |
| **Strong Consistency** | Transações consistentes |
| **No External Storage** | Storage próprio (não S3/HDFS) |

### Quando Usar Kudu?

✅ **Use Kudu para**:
- Analytics em tempo real com updates frequentes
- Workloads OLAP + OLTP híbridos
- Necessidade de strong consistency
- Infraestrutura on-premise (não cloud-native)

❌ **Não use Kudu para**:
- Dados imutáveis (use Parquet/Iceberg)
- Cloud-first (Kudu é complexo em cloud)
- Streaming puro (use Kafka)

### Integrando Kudu com DuckDB

**Nota**: DuckDB não tem conector Kudu direto. Use JDBC ou export para Parquet.

#### Via Parquet Export

```python
# No Impala/Spark (conectado ao Kudu)
# Exportar tabela Kudu para Parquet
CREATE TABLE vendas_parquet 
STORED AS PARQUET 
AS SELECT * FROM kudu_vendas;

# No DuckDB
import duckdb
conn = duckdb.connect()

# Ler Parquet exportado
result = conn.execute("""
    SELECT 
        categoria,
        COUNT(*) as vendas,
        SUM(valor) as receita
    FROM read_parquet('./vendas_from_kudu.parquet')
    GROUP BY categoria
""").fetchdf()

print(result)
```

#### Via JDBC (Impala Gateway)

```python
import duckdb
import jaydebeapi

# Conectar ao Impala (que acessa Kudu)
impala_conn = jaydebeapi.connect(
    'com.cloudera.impala.jdbc41.Driver',
    'jdbc:impala://impala-server:21050',
    ['username', 'password'],
    '/path/to/impala-jdbc.jar'
)

# Query Kudu via Impala
cursor = impala_conn.cursor()
cursor.execute("SELECT * FROM kudu_table")
data = cursor.fetchall()

# Carregar no DuckDB
conn = duckdb.connect()
conn.register('kudu_data', data)

result = conn.execute("SELECT COUNT(*) FROM kudu_data").fetchone()
print(f"Registros do Kudu: {result[0]}")
```

## Apache CarbonData: Índices Multidimensionais

### Visão Geral

**Apache CarbonData** é um formato indexado de armazenamento de dados projetado para **analytics ultra-rápidos** através de índices multidimensionais (MDK - Multi-Dimensional Key).

**Criado**: Huawei (2016)  
**Status**: Apache Top-Level Project  
**Foco**: Queries com filtros complexos

### Arquitetura CarbonData

```
CarbonData File Structure:
┌─────────────────────────────────────────────┐
│         CarbonData File (.carbondata)       │
├─────────────────────────────────────────────┤
│  Header                                     │
│  ├─ Schema                                  │
│  └─ Statistics                              │
├─────────────────────────────────────────────┤
│  Index (MDK)                                │
│  ├─ Min/Max values per column               │
│  ├─ Bloom filters                           │
│  └─ Inverted index                          │
├─────────────────────────────────────────────┤
│  Data Blocks (Columnar)                     │
│  ├─ Block 1 (compressed)                    │
│  ├─ Block 2 (compressed)                    │
│  └─ Block N (compressed)                    │
└─────────────────────────────────────────────┘
```

### Características Principais

| Feature | Descrição |
|---------|-----------|
| **MDK Index** | Índice multi-dimensional para pruning agressivo |
| **Dictionary Encoding** | Compressão e encoding inteligente |
| **Pre-aggregation** | Rollups pré-computados |
| **Time Series Optimization** | Otimizado para dados temporais |
| **Update/Delete** | Suporte via MERGE |

### Quando Usar CarbonData?

✅ **Use CarbonData para**:
- Queries com múltiplos filtros (WHERE com AND/OR)
- Análises de séries temporais
- Cardinalidade alta em várias dimensões
- Queries que retornam poucos registros

❌ **Não use CarbonData para**:
- Full table scans frequentes (use Parquet)
- Dados altamente desnormalizados
- Simplicidade é crítica

### Exemplo de Performance

```sql
-- Query com múltiplos filtros
SELECT * FROM vendas_carbon
WHERE 
    cidade = 'São Paulo' 
    AND categoria = 'Eletrônicos'
    AND data BETWEEN '2024-01-01' AND '2024-12-31'
    AND valor > 1000;

-- CarbonData usa índices MDK para pular blocos irrelevantes
-- Parquet teria que ler todos os blocos e filtrar
```

### Integrando CarbonData com DuckDB

#### Via Parquet Conversion

```python
# No Spark (com CarbonData)
# Converter CarbonData para Parquet
df = spark.read.format("carbondata") \
    .load("/data/vendas_carbon")

df.write.parquet("/data/vendas_parquet")

# No DuckDB
import duckdb
conn = duckdb.connect()

result = conn.execute("""
    SELECT 
        DATE_TRUNC('month', data) as mes,
        SUM(valor) as receita
    FROM read_parquet('/data/vendas_parquet/**/*.parquet')
    WHERE categoria = 'Eletrônicos'
    GROUP BY mes
    ORDER BY mes
""").fetchdf()

print(result)
```

#### Comparação: CarbonData vs Parquet

```python
import duckdb
import time

conn = duckdb.connect()

# Benchmark query com filtros
query = """
SELECT 
    categoria,
    cidade,
    COUNT(*) as vendas,
    SUM(valor) as receita
FROM {}
WHERE 
    data >= '2024-01-01'
    AND regiao = 'Sudeste'
    AND valor > 500
GROUP BY categoria, cidade
"""

# Parquet (sem índices especializados)
start = time.time()
parquet_result = conn.execute(
    query.format("read_parquet('./vendas_parquet/**/*.parquet')")
).fetchdf()
parquet_time = time.time() - start

# CarbonData (convertido, mas originalmente com índices)
# Nota: Índices são perdidos na conversão
start = time.time()
carbon_result = conn.execute(
    query.format("read_parquet('./vendas_from_carbon/**/*.parquet')")
).fetchdf()
carbon_time = time.time() - start

print(f"Parquet: {parquet_time:.2f}s")
print(f"CarbonData (original): {carbon_time:.2f}s (estimado 2-5x mais rápido)")
```

## Vortex: Next-Gen Columnar Format

### Visão Geral

**Vortex** é um formato experimental (2024) que visa ser o "sucessor" do Parquet com foco em:
- **Compressão moderna**: Algoritmos além de Snappy/ZSTD
- **Encoding adaptativo**: Escolhe automaticamente melhor encoding
- **Performance**: Até 10x mais rápido que Parquet em alguns casos

**Status**: Experimental, não recomendado para produção

### Conceitos do Vortex

```python
# Conceito Vortex (pseudocódigo)
vortex_file = {
    "metadata": {
        "schema": [...],
        "encoding_strategies": {
            "column_1": "dictionary + RLE",
            "column_2": "bit-packing + LZ4",
            "column_3": "delta + ZSTD"
        }
    },
    "statistics": {
        "row_groups": [...],
        "adaptive_compression_ratio": 0.15  # 15% do tamanho original
    },
    "data": [...]
}
```

### Quando Considerar Vortex?

⚠️ **Experimental - Apenas para Early Adopters**:
- Pesquisa e desenvolvimento
- Benchmarking de novos formatos
- Contribuir para o projeto

❌ **Não use em produção**:
- Ainda não estável
- Suporte limitado
- API pode mudar

## Comparação de Formatos Especializados

### Performance Comparison

| Formato | Write Speed | Read Speed | Compression | Updates | Use Case |
|---------|-------------|------------|-------------|---------|----------|
| **Kudu** | ⚡⚡⚡ | ⚡⚡⚡ | ⚡⚡ | ⚡⚡⚡ | Real-time OLAP+OLTP |
| **CarbonData** | ⚡⚡ | ⚡⚡⚡ (filtered) | ⚡⚡⚡ | ⚡⚡ | Multi-filter queries |
| **Parquet** | ⚡⚡ | ⚡⚡⚡ | ⚡⚡⚡ | ❌ | Standard analytics |
| **Vortex** | ⚡⚡ | ⚡⚡⚡⚡ (?) | ⚡⚡⚡⚡ (?) | ❌ | Experimental |

### Feature Matrix

| Feature | Kudu | CarbonData | Parquet | Vortex |
|---------|------|------------|---------|--------|
| **ACID Transactions** | ✅ | ✅ | ❌ | ❌ |
| **Indexes** | ✅ (PK) | ✅ (MDK) | ⚠️ (stats) | ❌ |
| **Cloud Native** | ❌ | ⚠️ | ✅ | ✅ |
| **DuckDB Support** | ⚠️ (via export) | ⚠️ (via export) | ✅✅ | ❌ |
| **Ecosystem** | Medium | Medium | Large | Tiny |
| **Maturity** | High | High | High | Low |

## Integração Prática: Pipeline Multi-Formato

```python
import duckdb
import pandas as pd
from typing import Dict, Any

class MultiFormatDataPipeline:
    """Pipeline que abstrai múltiplos formatos especializados"""
    
    def __init__(self):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; INSTALL iceberg;")
        self.conn.execute("LOAD delta; LOAD iceberg;")
    
    def read_format(self, format_type: str, path: str) -> pd.DataFrame:
        """Lê dados de diferentes formatos"""
        readers = {
            'parquet': lambda p: self.conn.execute(
                f"SELECT * FROM read_parquet('{p}')"
            ).fetchdf(),
            
            'delta': lambda p: self.conn.execute(
                f"SELECT * FROM delta_scan('{p}')"
            ).fetchdf(),
            
            'iceberg': lambda p: self.conn.execute(
                f"SELECT * FROM iceberg_scan('{p}')"
            ).fetchdf(),
            
            'kudu_export': lambda p: self.conn.execute(
                f"SELECT * FROM read_parquet('{p}')"  # Kudu exportado
            ).fetchdf(),
            
            'carbon_export': lambda p: self.conn.execute(
                f"SELECT * FROM read_parquet('{p}')"  # Carbon exportado
            ).fetchdf(),
        }
        
        if format_type not in readers:
            raise ValueError(f"Formato não suportado: {format_type}")
        
        return readers[format_type](path)
    
    def unified_analytics(self, sources: Dict[str, Dict[str, str]]) -> pd.DataFrame:
        """
        Analytics unificado sobre múltiplos formatos
        
        sources = {
            'vendas_delta': {'format': 'delta', 'path': './delta'},
            'vendas_kudu': {'format': 'kudu_export', 'path': './kudu.parquet'},
            'vendas_carbon': {'format': 'carbon_export', 'path': './carbon.parquet'}
        }
        """
        dfs = []
        
        for name, config in sources.items():
            df = self.read_format(config['format'], config['path'])
            df['source'] = name
            dfs.append(df)
        
        # Combinar todos
        combined = pd.concat(dfs, ignore_index=True)
        
        # Registrar no DuckDB para analytics
        self.conn.register('unified_data', combined)
        
        # Analytics
        result = self.conn.execute("""
            SELECT 
                source,
                COUNT(*) as registros,
                SUM(valor) as receita_total,
                AVG(valor) as ticket_medio
            FROM unified_data
            GROUP BY source
            ORDER BY receita_total DESC
        """).fetchdf()
        
        return result

# Uso
pipeline = MultiFormatDataPipeline()

sources = {
    'delta_lake': {
        'format': 'delta',
        'path': './data/vendas_delta'
    },
    'kudu_exported': {
        'format': 'kudu_export',
        'path': './data/kudu_export.parquet'
    },
    'carbon_exported': {
        'format': 'carbon_export',
        'path': './data/carbon_export.parquet'
    }
}

analytics = pipeline.unified_analytics(sources)
print("\n=== Analytics Cross-Format ===")
print(analytics)
```

## Recomendações por Cenário

### Decision Tree

```
Precisa de updates em tempo real?
├─ SIM
│  └─ Infraestrutura on-premise?
│     ├─ SIM → Apache Kudu
│     └─ NÃO → Delta Lake / Hudi
└─ NÃO
   └─ Queries com muitos filtros?
      ├─ SIM → CarbonData
      └─ NÃO → Parquet / Iceberg / Delta
```

### Por Indústria

| Indústria | Formato Recomendado | Razão |
|-----------|---------------------|-------|
| **Fintech** | Kudu + Iceberg | Real-time + Analytics |
| **E-commerce** | Delta Lake | Updates + Simplicidade |
| **IoT/Telemetria** | CarbonData | Time-series + Filtros |
| **Media/Streaming** | Iceberg | Multi-engine + Scale |
| **ML/AI** | Lance | Vector search |

## Próximos Passos

No próximo capítulo, vamos explorar performance e otimizações práticas para maximizar o desempenho do DuckDB com esses formatos.

## Recursos

- [Apache Kudu Docs](https://kudu.apache.org/)
- [Apache CarbonData Docs](https://carbondata.apache.org/)
- [Vortex GitHub](https://github.com/spiraldb/vortex)

---

**Exercício Prático**:

Compare performance de leitura entre Parquet nativo e exportação de outros formatos:

```python
import duckdb
import pandas as pd
import time

# Criar dados de teste
df = pd.DataFrame({
    'id': range(1000000),
    'categoria': ['Cat' + str(i % 100) for i in range(1000000)],
    'valor': [i * 1.5 for i in range(1000000)]
})

# Salvar como Parquet
df.to_parquet('./test_parquet.parquet')

# Benchmark
conn = duckdb.connect()

# Query com filtro
query = """
SELECT 
    categoria,
    COUNT(*) as count,
    AVG(valor) as avg_valor
FROM read_parquet('./test_parquet.parquet')
WHERE categoria IN ('Cat1', 'Cat2', 'Cat3')
GROUP BY categoria
"""

# Medir tempo
start = time.time()
result = conn.execute(query).fetchdf()
elapsed = time.time() - start

print(f"Query executada em {elapsed:.3f}s")
print(result)
```
