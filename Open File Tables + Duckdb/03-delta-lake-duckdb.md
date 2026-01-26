# Capítulo 03: Delta Lake com DuckDB

## Introdução ao Delta Lake

**Delta Lake** é um formato de armazenamento open-source criado pela Databricks que adiciona uma camada de gerenciamento transacional sobre arquivos Parquet. É o formato de tabela mais popular no ecossistema Spark e tem excelente suporte no DuckDB.

### Características Principais

- ✅ **Transações ACID**: Garante consistência mesmo com múltiplas operações
- ✅ **Time Travel**: Acesse versões históricas dos dados
- ✅ **Schema Evolution**: Modifique esquemas sem reescrever dados
- ✅ **Auditoria**: Transaction log completo em JSON
- ✅ **Unified Batch/Streaming**: Mesmo formato para ambos

## Instalando a Extensão Delta no DuckDB

### No SQL

```sql
-- Instalar a extensão
INSTALL delta;

-- Carregar a extensão na sessão
LOAD delta;

-- Verificar instalação
SELECT * FROM duckdb_extensions() WHERE extension_name = 'delta';
```

### No Python

```python
import duckdb

# Conectar e instalar
conn = duckdb.connect()
conn.execute("INSTALL delta")
conn.execute("LOAD delta")

# Verificar
result = conn.execute("""
    SELECT extension_name, loaded, installed 
    FROM duckdb_extensions() 
    WHERE extension_name = 'delta'
""").fetchall()
print(result)
```

### No CLI

```bash
# Iniciar DuckDB
duckdb

# Dentro do CLI
D INSTALL delta;
D LOAD delta;
```

## Estrutura de uma Tabela Delta Lake

Uma tabela Delta tem a seguinte estrutura:

```
tabela_delta/
├── _delta_log/
│   ├── 00000000000000000000.json    # Transaction log
│   ├── 00000000000000000001.json
│   ├── 00000000000000000002.json
│   └── 00000000000000000010.checkpoint.parquet  # Checkpoint
├── part-00000-xxx.snappy.parquet    # Dados
├── part-00001-xxx.snappy.parquet
└── part-00002-xxx.snappy.parquet
```

**Transaction Log** (`_delta_log/`):
- Arquivos JSON sequenciais
- Cada arquivo representa uma transação
- Contém metadados: schema, operações, estatísticas
- Checkpoints periódicos (Parquet) para otimização

## Lendo Tabelas Delta com DuckDB

### Leitura Básica

```sql
-- Sintaxe básica
SELECT * FROM delta_scan('path/to/delta_table');

-- Exemplo com contagem
SELECT COUNT(*) as total_registros
FROM delta_scan('./data/vendas_delta');

-- Filtros e agregações
SELECT 
    categoria,
    SUM(valor) as receita_total,
    COUNT(*) as num_vendas,
    AVG(valor) as ticket_medio
FROM delta_scan('./data/vendas_delta')
WHERE data >= '2024-01-01'
GROUP BY categoria
ORDER BY receita_total DESC;
```

### Leitura de Tabelas Remotas (S3)

```sql
-- Configurar credenciais AWS
CREATE SECRET aws_credentials (
    TYPE S3,
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    REGION 'us-east-1'
);

-- Ler tabela Delta no S3
SELECT * 
FROM delta_scan('s3://meu-bucket/lakehouse/vendas_delta')
LIMIT 10;

-- Query complexa
SELECT 
    DATE_TRUNC('month', data_venda) as mes,
    regiao,
    SUM(valor) as receita
FROM delta_scan('s3://analytics/vendas_delta')
WHERE data_venda BETWEEN '2024-01-01' AND '2024-12-31'
GROUP BY mes, regiao
HAVING SUM(valor) > 100000
ORDER BY mes, receita DESC;
```

### Python com Delta

```python
import duckdb
import pandas as pd

# Conectar e configurar
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")

# Ler tabela Delta
df = conn.execute("""
    SELECT * FROM delta_scan('./data/vendas_delta')
    WHERE valor > 1000
""").fetchdf()

print(f"Total de registros: {len(df)}")
print(df.head())

# Análise direta
analise = conn.execute("""
    SELECT 
        categoria,
        COUNT(*) as vendas,
        ROUND(AVG(valor), 2) as ticket_medio,
        ROUND(SUM(valor), 2) as receita_total
    FROM delta_scan('./data/vendas_delta')
    GROUP BY categoria
    ORDER BY receita_total DESC
""").fetchdf()

print("\n=== Análise por Categoria ===")
print(analise)
```

## Time Travel com Delta Lake

Uma das funcionalidades mais poderosas do Delta Lake é o **Time Travel**: a capacidade de consultar versões históricas dos dados.

### Consultar Versões Específicas

```sql
-- Versão mais recente (padrão)
SELECT COUNT(*) FROM delta_scan('./data/vendas_delta');

-- Versão específica (por número)
SELECT COUNT(*) 
FROM delta_scan('./data/vendas_delta', version => 5);

-- Versão específica (por timestamp)
SELECT COUNT(*) 
FROM delta_scan('./data/vendas_delta', 
    timestamp => '2024-01-15 10:30:00');

-- Comparar versões
WITH 
    versao_atual AS (
        SELECT categoria, SUM(valor) as valor_atual
        FROM delta_scan('./data/vendas_delta')
        GROUP BY categoria
    ),
    versao_anterior AS (
        SELECT categoria, SUM(valor) as valor_anterior
        FROM delta_scan('./data/vendas_delta', version => 10)
        GROUP BY categoria
    )
SELECT 
    va.categoria,
    va.valor_atual,
    vant.valor_anterior,
    va.valor_atual - vant.valor_anterior as diferenca,
    ROUND((va.valor_atual - vant.valor_anterior) / vant.valor_anterior * 100, 2) as pct_change
FROM versao_atual va
JOIN versao_anterior vant ON va.categoria = vant.categoria
ORDER BY ABS(diferenca) DESC;
```

### Auditoria com Time Travel

```python
import duckdb
from datetime import datetime, timedelta

class DeltaAuditor:
    def __init__(self, delta_path):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; LOAD delta;")
        self.delta_path = delta_path
    
    def compare_versions(self, version1, version2):
        """Compara duas versões da tabela"""
        query = f"""
        WITH 
            v1 AS (
                SELECT * FROM delta_scan('{self.delta_path}', version => {version1})
            ),
            v2 AS (
                SELECT * FROM delta_scan('{self.delta_path}', version => {version2})
            ),
            only_in_v1 AS (
                SELECT 'Removido' as status, * FROM v1
                EXCEPT
                SELECT 'Removido' as status, * FROM v2
            ),
            only_in_v2 AS (
                SELECT 'Adicionado' as status, * FROM v2
                EXCEPT
                SELECT 'Adicionado' as status, * FROM v1
            )
        SELECT * FROM only_in_v1
        UNION ALL
        SELECT * FROM only_in_v2
        """
        return self.conn.execute(query).fetchdf()
    
    def track_changes_over_time(self, id_column, start_version, end_version):
        """Rastreia mudanças de um registro específico"""
        changes = []
        for version in range(start_version, end_version + 1):
            count = self.conn.execute(f"""
                SELECT COUNT(*) 
                FROM delta_scan('{self.delta_path}', version => {version})
            """).fetchone()[0]
            changes.append({'version': version, 'count': count})
        
        return pd.DataFrame(changes)

# Uso
auditor = DeltaAuditor('./data/vendas_delta')
changes = auditor.compare_versions(5, 6)
print("Mudanças entre versões 5 e 6:")
print(changes)
```

## Criando Tabelas Delta

### Criar a partir de Parquet

```sql
-- Instalar extensão
INSTALL delta;
LOAD delta;

-- Copiar Parquet para Delta
COPY (
    SELECT * FROM read_parquet('data/vendas.parquet')
) TO 'data/vendas_delta' (FORMAT DELTA);

-- Verificar
SELECT COUNT(*) FROM delta_scan('data/vendas_delta');
```

### Criar a partir de CSV

```sql
-- Ler CSV e salvar como Delta
COPY (
    SELECT 
        CAST(id AS INTEGER) as id,
        CAST(data AS DATE) as data,
        produto,
        categoria,
        CAST(valor AS DECIMAL(10,2)) as valor,
        regiao
    FROM read_csv('data/vendas.csv', AUTO_DETECT=TRUE)
) TO 'data/vendas_delta' (FORMAT DELTA);
```

### Python: Pipeline Completo

```python
import duckdb
import pandas as pd
from datetime import datetime, timedelta

def create_delta_table_from_dataframe(df, output_path):
    """Cria uma tabela Delta a partir de um DataFrame"""
    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")
    
    # Registrar DataFrame temporariamente
    conn.register('temp_df', df)
    
    # Criar Delta
    conn.execute(f"""
        COPY (SELECT * FROM temp_df) 
        TO '{output_path}' 
        (FORMAT DELTA)
    """)
    
    print(f"Tabela Delta criada em: {output_path}")
    
    # Verificar
    count = conn.execute(f"""
        SELECT COUNT(*) FROM delta_scan('{output_path}')
    """).fetchone()[0]
    
    print(f"Total de registros: {count}")
    conn.close()

# Gerar dados de exemplo
dates = pd.date_range('2024-01-01', periods=1000)
df = pd.DataFrame({
    'id': range(1, 1001),
    'data': dates,
    'produto': ['Produto_' + str(i % 50) for i in range(1, 1001)],
    'categoria': ['Cat_' + str(i % 10) for i in range(1, 1001)],
    'valor': [100 + (i * 1.5) for i in range(1, 1001)],
    'regiao': ['Regiao_' + str(i % 5) for i in range(1, 1001)]
})

# Criar tabela Delta
create_delta_table_from_dataframe(df, './data/vendas_delta')
```

## Otimizações com Delta Lake

### 1. Particionamento

```sql
-- Criar tabela Delta particionada
COPY (
    SELECT 
        *,
        YEAR(data) as ano,
        MONTH(data) as mes
    FROM read_parquet('data/vendas.parquet')
) TO 'data/vendas_delta_partitioned' 
(FORMAT DELTA, PARTITION_BY (ano, mes));

-- Query com partition pruning
SELECT COUNT(*) 
FROM delta_scan('data/vendas_delta_partitioned')
WHERE ano = 2024 AND mes = 6;
-- DuckDB lê apenas a partição específica
```

### 2. Compression

```sql
-- Delta usa Snappy por padrão, mas você pode controlar via Parquet
COPY (
    SELECT * FROM read_csv('data/vendas.csv')
) TO 'data/vendas_delta_compressed' 
(FORMAT DELTA, COMPRESSION ZSTD);
-- Melhora compressão em ~30-40% vs Snappy
```

### 3. File Size Optimization

```python
import duckdb

def optimize_delta_table(delta_path, target_file_size_mb=128):
    """
    Otimiza tamanho de arquivos Delta
    (Nota: DuckDB não tem OPTIMIZE nativo, mas podemos reescrever)
    """
    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")
    
    # Ler todos os dados
    temp_path = f"{delta_path}_temp"
    
    conn.execute(f"""
        COPY (
            SELECT * FROM delta_scan('{delta_path}')
        ) TO '{temp_path}' 
        (FORMAT DELTA, ROW_GROUP_SIZE 100000)
    """)
    
    print(f"Tabela otimizada em: {temp_path}")
    print("Substitua a tabela original pelo temp se desejar")
    conn.close()

optimize_delta_table('./data/vendas_delta')
```

## Metadados e Estatísticas Delta

### Explorar Transaction Log

```python
import json
import os

def read_delta_log(delta_path):
    """Lê o transaction log Delta"""
    log_path = os.path.join(delta_path, '_delta_log')
    
    transactions = []
    for filename in sorted(os.listdir(log_path)):
        if filename.endswith('.json'):
            filepath = os.path.join(log_path, filename)
            with open(filepath, 'r') as f:
                for line in f:
                    transaction = json.loads(line)
                    transactions.append(transaction)
    
    return transactions

# Exemplo
transactions = read_delta_log('./data/vendas_delta')
print(f"Total de transações: {len(transactions)}")

# Ver primeira transação
if transactions:
    print("\nPrimeira transação:")
    print(json.dumps(transactions[0], indent=2))
```

### Estatísticas de Dados

```sql
-- Metadados básicos
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT categoria) as num_categorias,
    MIN(data) as data_inicio,
    MAX(data) as data_fim,
    ROUND(SUM(valor), 2) as receita_total,
    ROUND(AVG(valor), 2) as ticket_medio
FROM delta_scan('./data/vendas_delta');

-- Distribuição por categoria
SELECT 
    categoria,
    COUNT(*) as registros,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentual,
    ROUND(SUM(valor), 2) as receita
FROM delta_scan('./data/vendas_delta')
GROUP BY categoria
ORDER BY receita DESC;
```

## Padrões Avançados

### Pattern 1: Incremental Append

```python
import duckdb
import pandas as pd
from datetime import datetime

class DeltaIncrementalWriter:
    def __init__(self, delta_path):
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; LOAD delta;")
        self.delta_path = delta_path
    
    def append_data(self, new_data_df):
        """Adiciona novos dados à tabela Delta"""
        # Registrar novo DataFrame
        self.conn.register('new_data', new_data_df)
        
        # Verificar se tabela existe
        try:
            existing_count = self.conn.execute(f"""
                SELECT COUNT(*) FROM delta_scan('{self.delta_path}')
            """).fetchone()[0]
            
            # Tabela existe, fazer append via reescrita completa
            # (DuckDB não tem MERGE nativo ainda)
            self.conn.execute(f"""
                COPY (
                    SELECT * FROM delta_scan('{self.delta_path}')
                    UNION ALL
                    SELECT * FROM new_data
                ) TO '{self.delta_path}_temp' (FORMAT DELTA)
            """)
            
            print(f"Dados anexados. Registros anteriores: {existing_count}")
            print(f"Nova tabela em: {self.delta_path}_temp")
            
        except Exception as e:
            # Tabela não existe, criar nova
            self.conn.execute(f"""
                COPY (SELECT * FROM new_data) 
                TO '{self.delta_path}' (FORMAT DELTA)
            """)
            print(f"Nova tabela Delta criada em: {self.delta_path}")

# Uso
writer = DeltaIncrementalWriter('./data/vendas_delta')

# Novos dados
new_sales = pd.DataFrame({
    'id': [1001, 1002, 1003],
    'data': ['2024-06-01', '2024-06-02', '2024-06-03'],
    'produto': ['Produto_X', 'Produto_Y', 'Produto_Z'],
    'categoria': ['Cat_1', 'Cat_2', 'Cat_1'],
    'valor': [150.50, 200.00, 175.75],
    'regiao': ['Sul', 'Norte', 'Sudeste']
})

writer.append_data(new_sales)
```

### Pattern 2: Schema Evolution

```python
def evolve_delta_schema(delta_path):
    """Adiciona nova coluna ao schema Delta"""
    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta;")
    
    # Ler dados existentes e adicionar nova coluna
    conn.execute(f"""
        COPY (
            SELECT 
                *,
                CAST(NULL AS VARCHAR) as nova_coluna,
                current_timestamp() as data_atualizacao
            FROM delta_scan('{delta_path}')
        ) TO '{delta_path}_evolved' (FORMAT DELTA)
    """)
    
    print("Schema evoluído com sucesso!")
    print("Nova tabela em:", f"{delta_path}_evolved")
    conn.close()

evolve_delta_schema('./data/vendas_delta')
```

## Integração com Spark

Delta Lake é nativo do Spark. Você pode criar tabelas com Spark e ler com DuckDB:

```python
# No Spark (PySpark)
from pyspark.sql import SparkSession
from delta import *

# Criar sessão Spark com Delta
spark = (SparkSession.builder
    .appName("DeltaLake")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

# Escrever dados como Delta
df = spark.read.parquet("vendas.parquet")
df.write.format("delta").save("s3://bucket/vendas_delta")

# Depois, no DuckDB
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")

# Ler a mesma tabela
result = conn.execute("""
    SELECT * FROM delta_scan('s3://bucket/vendas_delta')
""").fetchdf()
```

## Limitações do Delta no DuckDB

1. **Somente leitura**: DuckDB não suporta escritas MERGE/UPDATE/DELETE nativamente
2. **Sem OPTIMIZE**: Não há comando OPTIMIZE (mas pode reescrever)
3. **Sem VACUUM**: Não remove arquivos antigos automaticamente
4. **Checkpoints**: Lê, mas não cria checkpoints

**Solução**: Use Spark para escritas complexas, DuckDB para leituras analíticas.

## Próximos Passos

No próximo capítulo, vamos explorar Apache Iceberg, o concorrente direto do Delta Lake com foco em performance multi-engine.

## Recursos

- [Delta Lake Official](https://delta.io/)
- [DuckDB Delta Extension](https://duckdb.org/docs/extensions/delta)
- [Delta Lake Spec](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

---

**Exercício Prático**:

Crie uma tabela Delta completa e explore time travel:

```python
import duckdb
import pandas as pd
from datetime import datetime, timedelta

# Setup
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")

# Dados iniciais
df1 = pd.DataFrame({
    'id': [1, 2, 3],
    'nome': ['Alice', 'Bob', 'Carol'],
    'valor': [100, 200, 300]
})

# Criar Delta (versão 0)
conn.execute("COPY (SELECT * FROM df1) TO './test_delta' (FORMAT DELTA)")

# Adicionar mais dados (versão 1)
df2 = pd.DataFrame({
    'id': [4, 5],
    'nome': ['Dave', 'Eve'],
    'valor': [400, 500]
})
# ... continuar com append

print("Explore as versões com time travel!")
```
