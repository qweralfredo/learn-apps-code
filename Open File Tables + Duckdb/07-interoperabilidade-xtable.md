# Capítulo 07: Interoperabilidade com Apache XTable

## O Problema da Fragmentação de Formatos

Imagine que você tem:
- **Equipe A**: Usa Delta Lake com Databricks
- **Equipe B**: Usa Iceberg com Trino
- **Equipe C**: Usa Hudi com Flink

Cada equipe precisa acessar dados das outras, mas os formatos são incompatíveis. Soluções tradicionais:

❌ **Replicar dados**: Caro, complexo, defasagem  
❌ **Converter offline**: Lento, duplicação de storage  
❌ **ETL constante**: Overhead operacional

✅ **Apache XTable**: Tradutor universal em tempo real

## Introdução ao Apache XTable (OneTable)

**Apache XTable** (anteriormente OneTable, da Onehouse) é uma camada de **interoperabilidade** que permite ler e escrever em múltiplos formatos simultaneamente **sem duplicar dados**.

### Como Funciona?

```
┌─────────────────────────────────────────────────┐
│           Escritor Nativo (ex: Spark)           │
│                                                 │
│    Escreve em Delta Lake                        │
└────────────────┬────────────────────────────────┘
                 │
                 ▼
        ┌─────────────────┐
        │  Dados Físicos  │  (Arquivos Parquet)
        │   (Imutáveis)   │
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────┐
        │  Apache XTable  │  (Sync Tool)
        └────────┬────────┘
                 │
        ┌────────┴────────┬───────────────┐
        │                 │               │
        ▼                 ▼               ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Delta        │  │ Iceberg      │  │ Hudi         │
│ Metadata     │  │ Metadata     │  │ Metadata     │
└──────────────┘  └──────────────┘  └──────────────┘
        │                 │               │
        ▼                 ▼               ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Leitores     │  │ Leitores     │  │ Leitores     │
│ Delta        │  │ Iceberg      │  │ Hudi         │
│ (Databricks) │  │ (Trino)      │  │ (Flink)      │
└──────────────┘  └──────────────┘  └──────────────┘
```

**Principais Vantagens**:
- ✅ **Zero duplicação**: Dados físicos únicos
- ✅ **Zero lock-in**: Troque de formato sem migração
- ✅ **Multi-engine**: Qualquer engine lê qualquer formato
- ✅ **Performance**: Apenas metadados são replicados

## Instalando XTable

### Via Docker (Recomendado)

```bash
# Pull da imagem oficial
docker pull apache/xtable:latest

# Rodar container
docker run -it \
  -v $(pwd)/data:/data \
  apache/xtable:latest \
  --help
```

### Via JAR

```bash
# Download
wget https://github.com/apache/incubator-xtable/releases/download/v0.1.0/xtable-utilities-0.1.0.jar

# Executar
java -jar xtable-utilities-0.1.0.jar --help
```

### Verificar Instalação

```bash
java -jar xtable-utilities-0.1.0.jar --version
```

## Configuração Básica

### Arquivo de Configuração (xtable-config.yaml)

```yaml
sourceFormat: DELTA  # Formato fonte: DELTA, ICEBERG, ou HUDI
targetFormats:
  - ICEBERG
  - HUDI

datasets:
  - tableBasePath: /data/vendas_delta
    tableName: vendas
    partitionSpec: data:DATE
```

### Converter Delta → Iceberg + Hudi

```bash
# Executar conversão
java -jar xtable-utilities.jar \
  --datasetConfig xtable-config.yaml \
  --once
```

**Resultado**:
```
/data/vendas_delta/
├── _delta_log/              # Delta metadata (original)
├── metadata/                # Iceberg metadata (gerado)
│   └── v1.metadata.json
├── .hoodie/                 # Hudi metadata (gerado)
│   └── hoodie.properties
└── data/                    # Arquivos Parquet (compartilhados)
```

## Uso com DuckDB

### Ler Formatos Convertidos

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")
conn.execute("INSTALL iceberg; LOAD iceberg;")

# Ler como Delta (formato original)
delta_df = conn.execute("""
    SELECT 'Delta' as formato, COUNT(*) as registros
    FROM delta_scan('/data/vendas_delta')
""").fetchdf()

# Ler como Iceberg (convertido via XTable)
iceberg_df = conn.execute("""
    SELECT 'Iceberg' as formato, COUNT(*) as registros
    FROM iceberg_scan('/data/vendas_delta')
""").fetchdf()

print("Mesmos dados, formatos diferentes:")
print(delta_df)
print(iceberg_df)
```

### Pipeline Completo com XTable

```python
import duckdb
import subprocess
import json

class XTableManager:
    def __init__(self, xtable_jar_path):
        self.jar_path = xtable_jar_path
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; LOAD delta;")
        self.conn.execute("INSTALL iceberg; LOAD iceberg;")
    
    def create_config(self, source_path, source_format, target_formats):
        """Cria configuração XTable"""
        config = {
            "sourceFormat": source_format.upper(),
            "targetFormats": [f.upper() for f in target_formats],
            "datasets": [{
                "tableBasePath": source_path,
                "tableName": "data"
            }]
        }
        
        with open("xtable-config.yaml", "w") as f:
            import yaml
            yaml.dump(config, f)
        
        return "xtable-config.yaml"
    
    def sync(self, config_path):
        """Executa sincronização XTable"""
        cmd = [
            "java", "-jar", self.jar_path,
            "--datasetConfig", config_path,
            "--once"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"XTable sync failed: {result.stderr}")
        
        print("Sync completed successfully")
        return result.stdout
    
    def query_multi_format(self, base_path):
        """Consulta mesmos dados em múltiplos formatos"""
        results = {}
        
        # Delta
        try:
            results['delta'] = self.conn.execute(f"""
                SELECT COUNT(*) FROM delta_scan('{base_path}')
            """).fetchone()[0]
        except:
            results['delta'] = None
        
        # Iceberg
        try:
            results['iceberg'] = self.conn.execute(f"""
                SELECT COUNT(*) FROM iceberg_scan('{base_path}')
            """).fetchone()[0]
        except:
            results['iceberg'] = None
        
        return results

# Uso
manager = XTableManager("./xtable-utilities.jar")

# 1. Criar configuração
config_file = manager.create_config(
    source_path="/data/vendas_delta",
    source_format="DELTA",
    target_formats=["ICEBERG", "HUDI"]
)

# 2. Sincronizar
manager.sync(config_file)

# 3. Consultar em múltiplos formatos
results = manager.query_multi_format("/data/vendas_delta")
print("Registros por formato:")
print(json.dumps(results, indent=2))
```

## Sincronização Contínua

### Modo Watch (Incremental)

```bash
# Sincronização contínua (verifica mudanças a cada minuto)
java -jar xtable-utilities.jar \
  --datasetConfig xtable-config.yaml \
  --continuous \
  --refreshInterval 60
```

### Python Wrapper para Sync Automático

```python
import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class XTableAutoSync(FileSystemEventHandler):
    def __init__(self, config_path, jar_path):
        self.config_path = config_path
        self.jar_path = jar_path
        self.last_sync = 0
        self.cooldown = 60  # segundos
    
    def on_modified(self, event):
        """Sincroniza quando arquivos mudam"""
        current_time = time.time()
        
        # Evitar syncs muito frequentes
        if current_time - self.last_sync < self.cooldown:
            return
        
        if event.src_path.endswith('.json') or event.src_path.endswith('.parquet'):
            print(f"Mudança detectada: {event.src_path}")
            self.sync()
            self.last_sync = current_time
    
    def sync(self):
        """Executa sync XTable"""
        cmd = [
            "java", "-jar", self.jar_path,
            "--datasetConfig", self.config_path,
            "--once"
        ]
        
        print("Sincronizando XTable...")
        subprocess.run(cmd, check=True)
        print("Sync completo!")

# Setup observer
observer = Observer()
handler = XTableAutoSync(
    config_path="xtable-config.yaml",
    jar_path="./xtable-utilities.jar"
)

observer.schedule(handler, path="/data/vendas_delta", recursive=True)
observer.start()

print("Observando mudanças... (Ctrl+C para parar)")
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
```

## Casos de Uso Reais

### Caso 1: Migração Gradual

Migrar de Delta para Iceberg sem downtime:

```python
import duckdb
import subprocess

# Fase 1: Setup inicial
# Criar metadados Iceberg dos dados Delta existentes
subprocess.run([
    "java", "-jar", "xtable-utilities.jar",
    "--datasetConfig", "migration-config.yaml",
    "--once"
])

# Fase 2: Período de transição
# Aplicações antigas usam Delta, novas usam Iceberg
conn = duckdb.connect()
conn.execute("INSTALL delta; INSTALL iceberg;")
conn.execute("LOAD delta; LOAD iceberg;")

# Legacy app (Delta)
delta_result = conn.execute("""
    SELECT * FROM delta_scan('/data/vendas')
    WHERE data >= '2024-01-01'
""").fetchdf()

# New app (Iceberg)
iceberg_result = conn.execute("""
    SELECT * FROM iceberg_scan('/data/vendas')
    WHERE data >= '2024-01-01'
""").fetchdf()

# Ambos retornam os mesmos dados!

# Fase 3: Eventualmente, migrar completamente para Iceberg
```

### Caso 2: Multi-Cloud Strategy

```yaml
# xtable-multicloud-config.yaml
sourceFormat: DELTA
targetFormats:
  - ICEBERG

datasets:
  # AWS (Delta nativo)
  - tableBasePath: s3://aws-bucket/vendas_delta
    tableName: vendas
  
  # GCP (ler via Iceberg)
  # Metadados Iceberg gerados via XTable
```

```python
import duckdb

conn = duckdb.connect()
conn.execute("INSTALL delta; INSTALL iceberg;")
conn.execute("LOAD delta; LOAD iceberg;")

# Aplicação AWS: Lê Delta
aws_query = """
    SELECT * FROM delta_scan('s3://aws-bucket/vendas_delta')
"""

# Aplicação GCP: Lê Iceberg (mesmos dados físicos no S3!)
gcp_query = """
    SELECT * FROM iceberg_scan('s3://aws-bucket/vendas_delta')
"""

# Dados compartilhados, formatos diferentes
```

### Caso 3: Vendor Neutrality

```python
class VendorNeutralLakehouse:
    """
    Abstração sobre múltiplos formatos
    Garante que você nunca fica preso a um vendor
    """
    def __init__(self, base_path):
        self.base_path = base_path
        self.conn = duckdb.connect()
        self.conn.execute("INSTALL delta; INSTALL iceberg;")
        self.conn.execute("LOAD delta; LOAD iceberg;")
    
    def query(self, sql, preferred_format='iceberg'):
        """
        Executa query no formato preferido
        Falls back automaticamente se não disponível
        """
        formats_to_try = [
            ('iceberg', f'iceberg_scan(\'{self.base_path}\')'),
            ('delta', f'delta_scan(\'{self.base_path}\')'),
            ('parquet', f'read_parquet(\'{self.base_path}/**/*.parquet\')')
        ]
        
        # Tentar formato preferido primeiro
        formats_to_try.sort(key=lambda x: x[0] != preferred_format)
        
        for format_name, scan_func in formats_to_try:
            try:
                query_with_scan = sql.replace('{{TABLE}}', scan_func)
                result = self.conn.execute(query_with_scan).fetchdf()
                print(f"Query executada via {format_name}")
                return result
            except Exception as e:
                print(f"Formato {format_name} não disponível, tentando próximo...")
                continue
        
        raise Exception("Nenhum formato disponível!")

# Uso
lakehouse = VendorNeutralLakehouse('/data/vendas')

# Query abstrata
result = lakehouse.query("""
    SELECT 
        categoria,
        SUM(valor) as receita
    FROM {{TABLE}}
    WHERE data >= '2024-01-01'
    GROUP BY categoria
""", preferred_format='iceberg')

print(result)
```

## Limitações e Considerações

### Limitações do XTable

1. **One-way Sync (em algumas operações)**: Escreva em um formato, leia em outros
2. **Latência**: Sync não é instantâneo (geralmente segundos a minutos)
3. **Feature Parity**: Nem todas as features se traduzem perfeitamente
4. **Overhead de Metadados**: Armazena metadados duplicados (mas não dados)

### Compatibilidade de Features

| Feature | Delta → Iceberg | Iceberg → Delta | Hudi → Delta/Iceberg |
|---------|-----------------|-----------------|---------------------|
| **Schema** | ✅ | ✅ | ✅ |
| **Partitioning** | ✅ | ⚠️ (simples) | ✅ |
| **Time Travel** | ✅ | ✅ | ⚠️ |
| **Hidden Partitioning** | ❌ | N/A | ❌ |
| **Partition Evolution** | ❌ | ✅ | ❌ |

### Quando NÃO Usar XTable

❌ **Não use XTable se**:
- Precisa de latência sub-segundo entre formatos
- Features avançadas específicas de um formato são críticas
- Simplicidade operacional é prioridade máxima
- Todos os stakeholders podem padronizar em um formato

## Alternativas

### 1. Trino Universal Connector

Trino pode ler múltiplos formatos nativamente (sem XTable):

```sql
-- Em Trino
SELECT * FROM delta.db.table
UNION ALL
SELECT * FROM iceberg.db.table;
```

### 2. Dremio Lakehouse Platform

Dremio oferece abstração sobre múltiplos formatos.

### 3. AWS Glue Data Catalog

AWS Glue pode catalogar múltiplos formatos e abstrair diferenças.

## Próximos Passos

No próximo capítulo, vamos explorar formatos especializados como Apache Kudu, CarbonData e Vortex para casos de uso específicos.

## Recursos

- [Apache XTable GitHub](https://github.com/apache/incubator-xtable)
- [XTable Documentation](https://xtable.apache.org/)
- [Onehouse Blog](https://www.onehouse.ai/blog)

---

**Exercício Prático**:

Configure XTable para converter Delta → Iceberg:

```bash
# 1. Criar tabela Delta (usando Spark ou Python)
# Ver capítulo 3

# 2. Criar config XTable
cat > xtable-config.yaml << EOF
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  - tableBasePath: ./data/vendas_delta
    tableName: vendas
EOF

# 3. Executar sync
java -jar xtable-utilities.jar \
  --datasetConfig xtable-config.yaml \
  --once

# 4. Ler com DuckDB
python << EOF
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")
result = conn.execute("""
    SELECT COUNT(*) FROM iceberg_scan('./data/vendas_delta')
""").fetchone()
print(f"Registros: {result[0]}")
EOF
```
