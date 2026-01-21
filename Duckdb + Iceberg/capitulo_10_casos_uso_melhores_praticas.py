# -*- coding: utf-8 -*-
"""
capitulo_10_casos_uso_melhores_praticas
"""

# capitulo_10_casos_uso_melhores_praticas
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
import pandas as pd

class LakehouseAnalytics:
    def __init__(self, catalog_config):
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext
        self.con.execute("LOAD httpfs")

        # Configurar catálogo
        self.con.execute(f"""
            CREATE SECRET cat_secret (
                TYPE iceberg,
                CLIENT_ID '{catalog_config['client_id']}',
                CLIENT_SECRET '{catalog_config['client_secret']}',
                OAUTH2_SERVER_URI '{catalog_config['oauth_uri']}'
            )
        """)

        self.con.execute(f"""
            ATTACH '{catalog_config['warehouse']}' AS lakehouse (
                TYPE iceberg,
                SECRET cat_secret,
                ENDPOINT '{catalog_config['endpoint']}'
            )
        """)

    def daily_sales_report(self, date):
        """Relatório diário de vendas"""
        return self.con.execute(f"""
            SELECT
                product_category,
                count(*) as total_orders,
                sum(total_amount) as revenue,
                avg(total_amount) as avg_order_value,
                count(DISTINCT customer_id) as unique_customers
            FROM lakehouse.sales.orders
            WHERE order_date = '{date}'
            GROUP BY product_category
            ORDER BY revenue DESC
        """).df()

    def customer_segmentation(self, months_back=6):
        """Segmentação de clientes"""
        return self.con.execute(f"""
            WITH customer_stats AS (
                SELECT
                    customer_id,
                    count(*) as order_count,
                    sum(total_amount) as lifetime_value,
                    max(order_date) as last_order_date,
                    current_date - max(order_date) as days_since_last_order
                FROM lakehouse.sales.orders
                WHERE order_date >= current_date - INTERVAL '{months_back} months'
                GROUP BY customer_id
            )
            SELECT
                CASE
                    WHEN lifetime_value > 10000 AND days_since_last_order < 30
                        THEN 'VIP Active'
                    WHEN lifetime_value > 10000 AND days_since_last_order >= 30
                        THEN 'VIP At Risk'
                    WHEN order_count > 5 AND days_since_last_order < 60
                        THEN 'Regular'
                    ELSE 'Occasional'
                END as segment,
                count(*) as customer_count,
                avg(lifetime_value) as avg_ltv
            FROM customer_stats
            GROUP BY segment
            ORDER BY avg_ltv DESC
        """).df()

# Usar
analytics = LakehouseAnalytics(config)
report = analytics.daily_sales_report('2024-01-15')
print(report)

# Exemplo/Bloco 2
import duckdb
from datetime import datetime

class AuditedETLPipeline:
    def __init__(self, source_table, target_table):
        self.source = source_table
        self.target = target_table
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext
        self.con.execute("LOAD httpfs")

    def get_last_processed_snapshot(self):
        """Obtém ID do último snapshot processado"""
        # Ler de tabela de controle ou arquivo
        return None  # Implementar lógica de controle

    def process_incremental(self):
        """Processa apenas dados novos"""
        last_snapshot = self.get_last_processed_snapshot()

        if last_snapshot:
            # Ler apenas mudanças desde último snapshot
            new_data = self.con.execute(f"""
                SELECT * FROM iceberg_scan('{self.source}')
                WHERE snapshot_id > {last_snapshot}
            """).df()
        else:
            # Primeira execução - processar tudo
            new_data = self.con.execute(f"""
                SELECT * FROM iceberg_scan('{self.source}')
            """).df()

        # Processar e carregar
        if len(new_data) > 0:
            processed = self.transform(new_data)
            self.load(processed)

            # Registrar snapshot processado
            current_snapshot = self.get_current_snapshot()
            self.save_checkpoint(current_snapshot)

        return len(new_data)

    def transform(self, data):
        """Transformações de negócio"""
        # Implementar lógica de transformação
        return data

    def load(self, data):
        """Carregar no destino"""
        self.con.execute(f"""
            INSERT INTO {self.target}
            SELECT * FROM data
        """)

    def get_current_snapshot(self):
        """Obtém snapshot atual"""
        result = self.con.execute(f"""
            SELECT snapshot_id
            FROM iceberg_snapshots('{self.source}')
            ORDER BY sequence_number DESC
            LIMIT 1
        """).fetchone()
        return result[0] if result else None

    def save_checkpoint(self, snapshot_id):
        """Salva checkpoint para recuperação"""
        # Implementar persistência do checkpoint
        pass

# Usar
pipeline = AuditedETLPipeline(
    source_table='s3://bucket/raw/events',
    target_table='s3://bucket/processed/events'
)
records_processed = pipeline.process_incremental()
print(f"Processados: {records_processed:,} registros")

# Exemplo/Bloco 3
import duckdb
import streamlit as st
from datetime import datetime, timedelta

class RealtimeDashboard:
    def __init__(self, table_path):
        self.table = table_path
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext
        self.con.execute("LOAD httpfs")

    def get_latest_metrics(self, hours_back=24):
        """Métricas das últimas N horas"""
        cutoff = datetime.now() - timedelta(hours=hours_back)

        return self.con.execute(f"""
            SELECT
                date_trunc('hour', event_timestamp) as hour,
                count(*) as total_events,
                count(DISTINCT user_id) as active_users,
                count(CASE WHEN event_type = 'purchase' THEN 1 END) as purchases,
                sum(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) as revenue
            FROM iceberg_scan('{self.table}')
            WHERE event_timestamp >= '{cutoff}'
            GROUP BY hour
            ORDER BY hour DESC
        """).df()

    def get_top_products(self, limit=10):
        """Produtos mais vendidos hoje"""
        return self.con.execute(f"""
            SELECT
                product_name,
                count(*) as sales_count,
                sum(amount) as total_revenue
            FROM iceberg_scan('{self.table}')
            WHERE event_type = 'purchase'
              AND event_date = current_date
            GROUP BY product_name
            ORDER BY sales_count DESC
            LIMIT {limit}
        """).df()

    def render(self):
        """Renderiza dashboard (exemplo com Streamlit)"""
        st.title('Real-time Analytics Dashboard')

        # Métricas
        metrics = self.get_latest_metrics(hours_back=24)

        # KPIs
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events", f"{metrics['total_events'].sum():,}")
        col2.metric("Active Users", f"{metrics['active_users'].sum():,}")
        col3.metric("Revenue", f"${metrics['revenue'].sum():,.2f}")

        # Gráfico
        st.line_chart(metrics.set_index('hour')[['total_events', 'purchases']])

        # Top produtos
        st.subheader('Top 10 Products Today')
        top_products = self.get_top_products()
        st.dataframe(top_products)

# Usar
# dashboard = RealtimeDashboard('s3://bucket/events')
# dashboard.render()

# Exemplo/Bloco 4
import duckdb
from datetime import datetime

class DataQualityMonitor:
    def __init__(self, table_path):
        self.table = table_path
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext

    def check_null_rates(self):
        """Verifica percentual de NULLs por coluna"""
        # Obter colunas
        columns = self.con.execute(f"""
            DESCRIBE SELECT * FROM iceberg_scan('{self.table}') LIMIT 1
        """).df()['column_name'].tolist()

        # Calcular null rates
        null_checks = []
        for col in columns:
            null_rate = self.con.execute(f"""
                SELECT
                    '{col}' as column_name,
                    count(*) as total_rows,
                    count({col}) as non_null_rows,
                    (count(*) - count({col}))::FLOAT / count(*) * 100 as null_pct
                FROM iceberg_scan('{self.table}')
            """).fetchone()
            null_checks.append(null_rate)

        return pd.DataFrame(null_checks, columns=['column', 'total', 'non_null', 'null_pct'])

    def check_duplicates(self, key_columns):
        """Verifica duplicatas"""
        key_list = ', '.join(key_columns)

        return self.con.execute(f"""
            SELECT
                count(*) as total_rows,
                count(DISTINCT ({key_list})) as unique_keys,
                count(*) - count(DISTINCT ({key_list})) as duplicate_count
            FROM iceberg_scan('{self.table}')
        """).fetchone()

    def check_freshness(self, timestamp_column, max_delay_hours=24):
        """Verifica frescor dos dados"""
        return self.con.execute(f"""
            SELECT
                max({timestamp_column}) as latest_timestamp,
                current_timestamp - max({timestamp_column}) as data_age,
                CASE
                    WHEN current_timestamp - max({timestamp_column}) > INTERVAL '{max_delay_hours} hours'
                    THEN 'STALE'
                    ELSE 'FRESH'
                END as status
            FROM iceberg_scan('{self.table}')
        """).fetchone()

    def run_all_checks(self):
        """Executa todas as verificações"""
        print(f"=== Data Quality Report ===")
        print(f"Table: {self.table}")
        print(f"Time: {datetime.now()}")
        print()

        # Null rates
        print("Null Rates:")
        nulls = self.check_null_rates()
        print(nulls[nulls['null_pct'] > 5])  # Mostrar colunas com >5% nulls
        print()

        # Duplicates
        print("Duplicates:")
        dups = self.check_duplicates(['id'])
        print(f"Total: {dups[0]:,}, Unique: {dups[1]:,}, Duplicates: {dups[2]:,}")
        print()

        # Freshness
        print("Data Freshness:")
        fresh = self.check_freshness('event_timestamp')
        print(f"Latest: {fresh[0]}, Age: {fresh[1]}, Status: {fresh[2]}")

# Usar
monitor = DataQualityMonitor('s3://bucket/events')
monitor.run_all_checks()

# Exemplo/Bloco 5
# ✅ BOM: Nomes descritivos e consistentes
catalog.sales.daily_orders
catalog.sales.monthly_revenue_summary
catalog.marketing.customer_segments

# ❌ RUIM: Nomes genéricos
catalog.default.table1
catalog.default.data
catalog.default.temp

# Exemplo/Bloco 6
# ✅ BOM: Schema bem definido com tipos apropriados
CREATE TABLE catalog.sales.orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    order_timestamp TIMESTAMP,
    total_amount DECIMAL(10, 2),
    status VARCHAR,
    items STRUCT(
        product_id BIGINT,
        quantity INTEGER,
        price DECIMAL(10, 2)
    )[]
);

# ❌ RUIM: Tudo como VARCHAR
CREATE TABLE catalog.sales.orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_date VARCHAR,
    total_amount VARCHAR,
    status VARCHAR
);

# Exemplo/Bloco 7
# ✅ BOM: Particionar por coluna frequentemente filtrada
# Com cardinalidade apropriada
PARTITIONED BY (date_trunc('day', order_date))

# ✅ BOM: Partition evolution
# Ano 1: day
# Ano 2: hour (dados cresceram)

# ❌ RUIM: Particionar por coluna única (ID)
PARTITIONED BY (customer_id)  # Alta cardinalidade!

# ❌ RUIM: Não particionar tabela grande
# (tabela de 1 TB sem partições)

# Exemplo/Bloco 8
# ✅ BOM: Filtros em colunas de partição
SELECT * FROM orders
WHERE order_date >= '2024-01-01'
  AND order_date < '2024-02-01';

# ✅ BOM: Projection pushdown
SELECT order_id, total_amount FROM orders;

# ❌ RUIM: Sem filtros de partição
SELECT * FROM orders
WHERE customer_name LIKE 'John%';

# ❌ RUIM: SELECT * desnecessário
SELECT * FROM orders;

# Exemplo/Bloco 9
import duckdb
import logging

logger = logging.getLogger(__name__)

class RobustIcebergReader:
    def __init__(self, table_path, max_retries=3):
        self.table = table_path
        self.max_retries = max_retries
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext

    def read_with_retry(self, query):
        """Lê com retry em caso de falha"""
        for attempt in range(self.max_retries):
            try:
                result = self.con.execute(query).df()
                return result
            except Exception as e:
                logger.warning(f"Tentativa {attempt + 1} falhou: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Todas as tentativas falharam")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def safe_read(self, filter_date=None):
        """Leitura com tratamento de erro completo"""
        try:
            query = f"SELECT * FROM iceberg_scan('{self.table}')"
            if filter_date:
                query += f" WHERE event_date = '{filter_date}'"

            return self.read_with_retry(query)

        except Exception as e:
            logger.error(f"Erro ao ler tabela: {e}")
            # Retornar DataFrame vazio ou propagar erro
            return pd.DataFrame()

# Exemplo/Bloco 10
import duckdb
from datetime import datetime
import json

import importlib.util


def has_module(name):
    return importlib.util.find_spec(name) is not None

def safe_install_ext(con, ext_name):
    try:
        con.execute(f"INSTALL {ext_name}")
        con.execute(f"LOAD {ext_name}")
        return True
    except Exception as e:
        print(f"Warning: Failed to install/load {ext_name} extension: {e}")
        return False


class IcebergMonitoring:
    def __init__(self, table_path):
        self.table = table_path
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext

    def collect_metrics(self):
        """Coleta métricas da tabela"""
        metrics = {}

        # File stats
        file_stats = self.con.execute(f"""
            SELECT
                count(*) as file_count,
                sum(record_count) as total_records,
                sum(file_size_in_bytes) / 1024 / 1024 / 1024 as size_gb,
                avg(file_size_in_bytes) / 1024 / 1024 as avg_file_mb
            FROM iceberg_metadata('{self.table}')
            WHERE status = 'EXISTING'
        """).fetchone()

        metrics['files'] = {
            'count': file_stats[0],
            'total_records': file_stats[1],
            'size_gb': round(file_stats[2], 2),
            'avg_file_mb': round(file_stats[3], 2)
        }

        # Snapshot stats
        snapshot_count = self.con.execute(f"""
            SELECT count(*) FROM iceberg_snapshots('{self.table}')
        """).fetchone()[0]

        metrics['snapshots'] = {
            'count': snapshot_count
        }

        # Timestamp
        metrics['collected_at'] = datetime.now().isoformat()

        return metrics

    def save_metrics(self, output_file):
        """Salva métricas em arquivo"""
        metrics = self.collect_metrics()
        with open(output_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        print(f"Métricas salvas em {output_file}")

# Usar
monitor = IcebergMonitoring('s3://bucket/sales')
monitor.save_metrics('metrics.json')

# Exemplo/Bloco 11
# Diagnóstico
1. Use EXPLAIN ANALYZE para identificar gargalo
2. Verifique se filtros de partição estão sendo usados
3. Confirme projection pushdown
4. Monitore I/O de rede (para cloud storage)
5. Aumente threads se I/O-bound

# Soluções
- Adicionar filtros de partição
- Especificar colunas (não SELECT *)
- Aumentar threads para I/O paralelo
- Considerar compactação de small files

# Exemplo/Bloco 12
# Soluções
1. Aumentar memory_limit
2. Processar em batches
3. Usar temp_directory para spilling
4. Reduzir threads (paradoxalmente pode ajudar)
5. Otimizar query para usar menos memória

# Exemplo/Bloco 13
# Prevenção
1. Usar ACID guarantees do Iceberg
2. Evitar múltiplos writers simultâneos sem coordenação
3. Usar catálogos com lock management
4. Implementar retry logic com exponential backoff
