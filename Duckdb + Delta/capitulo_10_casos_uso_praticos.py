# -*- coding: utf-8 -*-
"""
capitulo-10-casos-uso-praticos
"""

# capitulo-10-casos-uso-praticos
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd

class EcommerceAnalyticsPipeline:
    """
    Pipeline de analytics para e-commerce
    """

    def __init__(self, s3_base_path: str):
        self.s3_base_path = s3_base_path
        self.con = duckdb.connect()
        self._setup_secrets()

    def _setup_secrets(self):
        """Configurar acesso S3"""
        self.con.execute("LOAD httpfs")
        self.con.execute("""
            CREATE SECRET (
                TYPE S3,
                PROVIDER credential_chain
            )
        """)

    def get_sales_metrics(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Calcular métricas principais de vendas
        """
        query = f"""
        WITH daily_sales AS (
            SELECT
                DATE_TRUNC('day', order_date) as date,
                COUNT(DISTINCT order_id) as orders,
                COUNT(DISTINCT customer_id) as unique_customers,
                SUM(total_amount) as revenue,
                AVG(total_amount) as avg_order_value
            FROM delta_scan('{self.s3_base_path}/sales')
            WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1
        )
        SELECT
            date,
            orders,
            unique_customers,
            revenue,
            avg_order_value,
            -- Moving averages
            AVG(revenue) OVER (
                ORDER BY date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as revenue_7d_ma,
            -- Growth rates
            (revenue - LAG(revenue, 7) OVER (ORDER BY date)) /
                LAG(revenue, 7) OVER (ORDER BY date) * 100 as wow_growth_pct
        FROM daily_sales
        ORDER BY date DESC
        """

        return self.con.execute(query).df()

    def get_product_performance(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Análise de performance por produto
        """
        query = f"""
        SELECT
            p.product_id,
            p.product_name,
            p.category,
            COUNT(DISTINCT s.order_id) as times_ordered,
            SUM(s.quantity) as total_quantity_sold,
            SUM(s.line_total) as total_revenue,
            AVG(s.line_total / s.quantity) as avg_selling_price,
            COUNT(DISTINCT s.customer_id) as unique_buyers
        FROM delta_scan('{self.s3_base_path}/order_items') s
        JOIN delta_scan('{self.s3_base_path}/products') p
            ON s.product_id = p.product_id
        WHERE s.order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY p.product_id, p.product_name, p.category
        ORDER BY total_revenue DESC
        LIMIT 100
        """

        return self.con.execute(query).df()

    def get_customer_segments(self) -> pd.DataFrame:
        """
        Segmentação RFM de clientes
        """
        query = f"""
        WITH customer_metrics AS (
            SELECT
                customer_id,
                MAX(order_date) as last_order_date,
                COUNT(DISTINCT order_id) as frequency,
                SUM(total_amount) as monetary
            FROM delta_scan('{self.s3_base_path}/sales')
            GROUP BY customer_id
        ),
        rfm_scored AS (
            SELECT
                customer_id,
                DATEDIFF('day', last_order_date, CURRENT_DATE) as recency,
                frequency,
                monetary,
                -- RFM scoring (1-5)
                NTILE(5) OVER (ORDER BY DATEDIFF('day', last_order_date, CURRENT_DATE) DESC) as r_score,
                NTILE(5) OVER (ORDER BY frequency) as f_score,
                NTILE(5) OVER (ORDER BY monetary) as m_score
            FROM customer_metrics
        )
        SELECT
            CASE
                WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
                WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score < 3 THEN 'Promising'
                WHEN r_score < 3 AND f_score >= 4 THEN 'At Risk'
                WHEN r_score < 3 AND f_score < 3 THEN 'Hibernating'
                ELSE 'Others'
            END as segment,
            COUNT(*) as customer_count,
            AVG(recency) as avg_recency_days,
            AVG(frequency) as avg_frequency,
            AVG(monetary) as avg_monetary
        FROM rfm_scored
        GROUP BY segment
        ORDER BY avg_monetary DESC
        """

        return self.con.execute(query).df()

    def generate_daily_report(self, date: str = None) -> Dict[str, pd.DataFrame]:
        """
        Gerar relatório diário completo
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')

        # Período: últimos 30 dias
        end_date = date
        start_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=30)).strftime('%Y-%m-%d')

        print(f"Generating report for {start_date} to {end_date}...")

        report = {
            'sales_metrics': self.get_sales_metrics(start_date, end_date),
            'product_performance': self.get_product_performance(start_date, end_date),
            'customer_segments': self.get_customer_segments()
        }

        return report

    def close(self):
        self.con.close()


# Uso
if __name__ == "__main__":
    pipeline = EcommerceAnalyticsPipeline('s3://my-data-lake/ecommerce')

    # Gerar relatório
    report = pipeline.generate_daily_report()

    print("\n=== SALES METRICS ===")
    print(report['sales_metrics'].head(10))

    print("\n=== TOP PRODUCTS ===")
    print(report['product_performance'].head(10))

    print("\n=== CUSTOMER SEGMENTS ===")
    print(report['customer_segments'])

    # Exportar para análise adicional
    report['sales_metrics'].to_csv('daily_sales.csv', index=False)
    report['product_performance'].to_csv('product_performance.csv', index=False)

    pipeline.close()

# Exemplo/Bloco 2
import duckdb
import pandas as pd
from typing import List, Dict
from dataclasses import dataclass

@dataclass
class QualityIssue:
    table: str
    column: str
    issue_type: str
    severity: str
    count: int
    sample_values: List

class DataQualityMonitor:
    """
    Monitor de qualidade de dados para tabelas Delta
    """

    def __init__(self):
        self.con = duckdb.connect()
        self.issues: List[QualityIssue] = []

    def check_nulls(self, table_path: str, critical_columns: List[str]) -> List[QualityIssue]:
        """
        Verificar valores nulos em colunas críticas
        """
        issues = []

        for column in critical_columns:
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                SUM(CASE WHEN "{column}" IS NULL THEN 1 ELSE 0 END) as null_count
            FROM delta_scan('{table_path}')
            """

            result = self.con.execute(query).fetchone()
            total_rows, null_count = result

            if null_count > 0:
                null_pct = (null_count / total_rows) * 100

                issue = QualityIssue(
                    table=table_path,
                    column=column,
                    issue_type='null_values',
                    severity='high' if null_pct > 5 else 'medium',
                    count=null_count,
                    sample_values=[]
                )
                issues.append(issue)

        return issues

    def check_duplicates(self, table_path: str, key_columns: List[str]) -> List[QualityIssue]:
        """
        Verificar duplicatas baseado em colunas chave
        """
        key_cols = ', '.join(f'"{col}"' for col in key_columns)

        query = f"""
        WITH duplicates AS (
            SELECT
                {key_cols},
                COUNT(*) as dup_count
            FROM delta_scan('{table_path}')
            GROUP BY {key_cols}
            HAVING COUNT(*) > 1
        )
        SELECT COUNT(*) as duplicate_groups, SUM(dup_count) as total_duplicates
        FROM duplicates
        """

        result = self.con.execute(query).fetchone()
        dup_groups, total_dups = result

        if dup_groups and dup_groups > 0:
            issue = QualityIssue(
                table=table_path,
                column=','.join(key_columns),
                issue_type='duplicates',
                severity='high',
                count=total_dups,
                sample_values=[]
            )
            return [issue]

        return []

    def check_outliers(self, table_path: str, numeric_columns: List[str]) -> List[QualityIssue]:
        """
        Detectar outliers usando IQR method
        """
        issues = []

        for column in numeric_columns:
            query = f"""
            WITH stats AS (
                SELECT
                    APPROX_QUANTILE("{column}", 0.25) as q1,
                    APPROX_QUANTILE("{column}", 0.75) as q3
                FROM delta_scan('{table_path}')
                WHERE "{column}" IS NOT NULL
            ),
            bounds AS (
                SELECT
                    q1 - 1.5 * (q3 - q1) as lower_bound,
                    q3 + 1.5 * (q3 - q1) as upper_bound
                FROM stats
            )
            SELECT COUNT(*) as outlier_count
            FROM delta_scan('{table_path}'), bounds
            WHERE "{column}" < lower_bound OR "{column}" > upper_bound
            """

            result = self.con.execute(query).fetchone()
            outlier_count = result[0]

            if outlier_count > 0:
                issue = QualityIssue(
                    table=table_path,
                    column=column,
                    issue_type='outliers',
                    severity='medium',
                    count=outlier_count,
                    sample_values=[]
                )
                issues.append(issue)

        return issues

    def check_schema_drift(self, table_path: str, expected_schema: Dict[str, str]) -> List[QualityIssue]:
        """
        Verificar se schema mudou inesperadamente
        """
        # Obter schema atual
        query = f"DESCRIBE SELECT * FROM delta_scan('{table_path}') LIMIT 0"
        current_schema_df = self.con.execute(query).df()

        current_schema = dict(zip(
            current_schema_df['column_name'],
            current_schema_df['column_type']
        ))

        issues = []

        # Verificar colunas faltantes
        for col, expected_type in expected_schema.items():
            if col not in current_schema:
                issue = QualityIssue(
                    table=table_path,
                    column=col,
                    issue_type='missing_column',
                    severity='high',
                    count=1,
                    sample_values=[]
                )
                issues.append(issue)
            elif current_schema[col] != expected_type:
                issue = QualityIssue(
                    table=table_path,
                    column=col,
                    issue_type='type_mismatch',
                    severity='high',
                    count=1,
                    sample_values=[f"Expected: {expected_type}, Got: {current_schema[col]}"]
                )
                issues.append(issue)

        return issues

    def run_full_check(
        self,
        table_path: str,
        critical_columns: List[str],
        key_columns: List[str],
        numeric_columns: List[str],
        expected_schema: Dict[str, str]
    ) -> List[QualityIssue]:
        """
        Executar verificação completa de qualidade
        """
        print(f"Running quality checks on {table_path}...")

        all_issues = []

        # Nulls
        print("  Checking for null values...")
        all_issues.extend(self.check_nulls(table_path, critical_columns))

        # Duplicates
        print("  Checking for duplicates...")
        all_issues.extend(self.check_duplicates(table_path, key_columns))

        # Outliers
        print("  Checking for outliers...")
        all_issues.extend(self.check_outliers(table_path, numeric_columns))

        # Schema drift
        print("  Checking schema...")
        all_issues.extend(self.check_schema_drift(table_path, expected_schema))

        self.issues.extend(all_issues)
        return all_issues

    def generate_report(self) -> pd.DataFrame:
        """
        Gerar relatório de qualidade
        """
        if not self.issues:
            return pd.DataFrame()

        report_data = [
            {
                'table': issue.table,
                'column': issue.column,
                'issue_type': issue.issue_type,
                'severity': issue.severity,
                'count': issue.count
            }
            for issue in self.issues
        ]

        return pd.DataFrame(report_data)


# Uso
if __name__ == "__main__":
    monitor = DataQualityMonitor()

    # Definir expectativas
    expected_schema = {
        'order_id': 'BIGINT',
        'customer_id': 'BIGINT',
        'order_date': 'DATE',
        'total_amount': 'DOUBLE'
    }

    # Executar verificações
    issues = monitor.run_full_check(
        table_path='./sales',
        critical_columns=['order_id', 'customer_id', 'total_amount'],
        key_columns=['order_id'],
        numeric_columns=['total_amount'],
        expected_schema=expected_schema
    )

    # Gerar relatório
    report = monitor.generate_report()

    if len(report) > 0:
        print("\n⚠ QUALITY ISSUES FOUND:")
        print(report.to_string(index=False))

        # Exportar
        report.to_csv('data_quality_report.csv', index=False)
    else:
        print("\n✓ No quality issues found!")

# Exemplo/Bloco 3
import duckdb
from flask import Flask, jsonify
from datetime import datetime, timedelta
from functools import lru_cache
import time

app = Flask(__name__)

class DashboardBackend:
    """
    Backend para dashboard em tempo real
    """

    def __init__(self, delta_path: str):
        self.delta_path = delta_path
        # Usar conexão compartilhada
        self.con = duckdb.connect(':memory:', read_only=False)
        self.con.execute("LOAD delta")
        self._setup_views()

    def _setup_views(self):
        """
        Criar views para queries frequentes
        """
        self.con.execute(f"""
            CREATE OR REPLACE VIEW sales AS
            SELECT * FROM delta_scan('{self.delta_path}/sales')
        """)

        self.con.execute(f"""
            CREATE OR REPLACE VIEW products AS
            SELECT * FROM delta_scan('{self.delta_path}/products')
        """)

    @lru_cache(maxsize=128)
    def get_metrics_cached(self, date_str: str):
        """
        Obter métricas com cache (atualiza a cada minuto)
        """
        # Cache baseado em timestamp arredondado para minuto
        # Isso permite cache de ~1 minuto
        minute_timestamp = int(time.time() / 60)

        return self._get_metrics_impl(date_str, minute_timestamp)

    def _get_metrics_impl(self, date_str: str, _cache_key: int):
        """
        Implementação interna (não fazer cache diretamente desta)
        """
        query = f"""
        SELECT
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value
        FROM sales
        WHERE DATE_TRUNC('day', order_date) = '{date_str}'
        """

        result = self.con.execute(query).fetchone()

        return {
            'date': date_str,
            'total_orders': result[0],
            'unique_customers': result[1],
            'revenue': float(result[2]) if result[2] else 0,
            'avg_order_value': float(result[3]) if result[3] else 0
        }

    def get_hourly_trend(self, date_str: str):
        """
        Tendência horária de vendas
        """
        query = f"""
        SELECT
            DATE_TRUNC('hour', order_timestamp) as hour,
            COUNT(*) as orders,
            SUM(total_amount) as revenue
        FROM sales
        WHERE DATE_TRUNC('day', order_date) = '{date_str}'
        GROUP BY hour
        ORDER BY hour
        """

        df = self.con.execute(query).df()
        return df.to_dict('records')

    def get_top_products(self, limit: int = 10):
        """
        Top produtos do dia
        """
        today = datetime.now().strftime('%Y-%m-%d')

        query = f"""
        SELECT
            p.product_name,
            COUNT(*) as times_ordered,
            SUM(s.quantity) as quantity_sold,
            SUM(s.line_total) as revenue
        FROM sales s
        JOIN products p ON s.product_id = p.product_id
        WHERE DATE_TRUNC('day', s.order_date) = '{today}'
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT {limit}
        """

        df = self.con.execute(query).df()
        return df.to_dict('records')


# Inicializar backend
backend = DashboardBackend('./data_lake')

# Endpoints Flask
@app.route('/api/metrics')
def metrics():
    """Métricas do dia atual"""
    today = datetime.now().strftime('%Y-%m-%d')
    data = backend.get_metrics_cached(today)
    return jsonify(data)

@app.route('/api/metrics/<date>')
def metrics_by_date(date):
    """Métricas de data específica"""
    data = backend.get_metrics_cached(date)
    return jsonify(data)

@app.route('/api/trend/hourly')
def hourly_trend():
    """Tendência horária"""
    today = datetime.now().strftime('%Y-%m-%d')
    data = backend.get_hourly_trend(today)
    return jsonify(data)

@app.route('/api/products/top')
def top_products():
    """Top produtos"""
    data = backend.get_top_products(limit=10)
    return jsonify(data)

@app.route('/api/health')
def health():
    """Health check"""
    return jsonify({'status': 'ok', 'timestamp': datetime.now().isoformat()})

if __name__ == '__main__':
    app.run(debug=True, port=5000)

# Exemplo/Bloco 4
import duckdb
from deltalake import write_deltalake
import pandas as pd
from datetime import datetime

class DataLakeMigration:
    """
    Migração de PostgreSQL para Delta Lake
    """

    def __init__(self, pg_connection_string: str, delta_base_path: str):
        self.pg_conn = pg_connection_string
        self.delta_path = delta_base_path
        self.con = duckdb.connect()

        # Carregar extensões
        self.con.execute("INSTALL postgres")
        self.con.execute("LOAD postgres")

    def migrate_table(self, table_name: str, partition_column: str = None):
        """
        Migrar tabela do Postgres para Delta
        """
        print(f"Migrating table: {table_name}")

        # 1. Attach PostgreSQL
        self.con.execute(f"""
            ATTACH '{self.pg_conn}' AS pg (TYPE postgres)
        """)

        # 2. Ler dados do PostgreSQL
        print("  Reading from PostgreSQL...")
        df = self.con.execute(f"""
            SELECT * FROM pg.public.{table_name}
        """).df()

        print(f"  Rows to migrate: {len(df):,}")

        # 3. Adicionar metadata de migração
        df['_migrated_at'] = datetime.now()
        df['_source'] = 'postgresql'

        # 4. Escrever para Delta
        print("  Writing to Delta Lake...")
        delta_table_path = f"{self.delta_path}/{table_name}"

        write_deltalake(
            delta_table_path,
            df,
            partition_by=[partition_column] if partition_column else None,
            mode="overwrite"
        )

        print(f"  ✓ Migration completed: {delta_table_path}")

        # 5. Verificar
        count = self.con.execute(f"""
            SELECT COUNT(*) FROM delta_scan('{delta_table_path}')
        """).fetchone()[0]

        print(f"  Verification: {count:,} rows in Delta table")

        return delta_table_path

    def incremental_sync(self, table_name: str, timestamp_column: str):
        """
        Sincronização incremental (apenas novos/alterados)
        """
        delta_table_path = f"{self.delta_path}/{table_name}"

        # Obter último timestamp migrado
        last_sync = self.con.execute(f"""
            SELECT MAX({timestamp_column}) as last_ts
            FROM delta_scan('{delta_table_path}')
        """).fetchone()[0]

        print(f"Last sync: {last_sync}")

        # Ler apenas registros novos
        self.con.execute(f"ATTACH '{self.pg_conn}' AS pg (TYPE postgres)")

        new_data = self.con.execute(f"""
            SELECT *
            FROM pg.public.{table_name}
            WHERE {timestamp_column} > '{last_sync}'
        """).df()

        if len(new_data) == 0:
            print("No new data to sync")
            return

        print(f"Syncing {len(new_data):,} new rows...")

        # Adicionar metadata
        new_data['_migrated_at'] = datetime.now()
        new_data['_source'] = 'postgresql'

        # Append para Delta
        write_deltalake(
            delta_table_path,
            new_data,
            mode="append"
        )

        print("✓ Incremental sync completed")

    def hybrid_query(self, query: str):
        """
        Query híbrida: dados de PostgreSQL + Delta Lake
        """
        self.con.execute(f"ATTACH '{self.pg_conn}' AS pg (TYPE postgres)")

        # Exemplo: JOIN entre Postgres e Delta
        result = self.con.execute(query).df()
        return result


# Uso
if __name__ == "__main__":
    migration = DataLakeMigration(
        pg_connection_string='host=localhost port=5432 dbname=mydb user=user password=pass',
        delta_base_path='./data_lake'
    )

    # Migração inicial
    migration.migrate_table('orders', partition_column='order_date')
    migration.migrate_table('customers')

    # Sincronização incremental periódica
    migration.incremental_sync('orders', timestamp_column='updated_at')

    # Query híbrida (exemplo)
    result = migration.hybrid_query("""
        SELECT
            d.customer_id,
            d.total_orders_delta,
            p.customer_name,
            p.email
        FROM (
            SELECT customer_id, COUNT(*) as total_orders_delta
            FROM delta_scan('./data_lake/orders')
            GROUP BY customer_id
        ) d
        JOIN pg.public.customers p ON d.customer_id = p.id
        ORDER BY total_orders_delta DESC
        LIMIT 10
    """)

    print("\nHybrid Query Result:")
    print(result)

# Exemplo/Bloco 5
# ✓ Use partition pruning
WHERE year = 2024 AND month = 1

# ✓ Projection pushdown
SELECT id, name FROM delta_scan('./table')

# ✓ Cache queries frequentes
@lru_cache(maxsize=128)
def get_metrics(date): ...

# Exemplo/Bloco 6
# Para dados grandes: Spark write + DuckDB read
spark_df.write.format("delta").save("./data")
duckdb.execute("SELECT * FROM delta_scan('./data')")

# Para dados médios: DuckDB direto
write_deltalake("./data", df)

# Exemplo/Bloco 7
# ✓ Use secrets
con.execute("CREATE SECRET (TYPE S3, PROVIDER credential_chain)")

# ✓ Read-only quando possível
con = duckdb.connect('db.duckdb', read_only=True)

# Exemplo/Bloco 8
# ✓ Estrutura modular
class DataPipeline:
    def extract(self): ...
    def transform(self): ...
    def load(self): ...

# ✓ Logging
import logging
logging.info(f"Processed {count} rows")

# ✓ Testes
def test_pipeline():
    assert pipeline.run() == expected_result

