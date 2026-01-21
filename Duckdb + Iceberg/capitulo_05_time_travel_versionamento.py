# -*- coding: utf-8 -*-
"""
capitulo_05_time_travel_versionamento
"""

# capitulo_05_time_travel_versionamento
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
from datetime import datetime, timedelta

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

# Comparar dados de hoje vs semana passada
week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

# Dados atuais
current = con.execute("""
    SELECT count(*) as total, sum(amount) as revenue
    FROM iceberg_scan('s3://bucket/sales')
""").fetchone()

# Dados de uma semana atrás
past = con.execute(f"""
    SELECT count(*) as total, sum(amount) as revenue
    FROM iceberg_scan(
        's3://bucket/sales',
        snapshot_from_timestamp = '{week_ago}'::TIMESTAMP
    )
""").fetchone()

print(f"Hoje: {current[0]:,} pedidos, R$ {current[1]:,.2f}")
print(f"Semana passada: {past[0]:,} pedidos, R$ {past[1]:,.2f}")
print(f"Crescimento: {(current[0] - past[0]):,} pedidos")

# Exemplo/Bloco 2
def audit_changes(table_path, timestamp1, timestamp2):
    """Compara dados entre dois momentos"""
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    # Dados no tempo 1
    t1 = con.execute(f"""
        SELECT customer_id, sum(amount) as total
        FROM iceberg_scan(
            '{table_path}',
            snapshot_from_timestamp = '{timestamp1}'::TIMESTAMP
        )
        GROUP BY customer_id
    """).df()

    # Dados no tempo 2
    t2 = con.execute(f"""
        SELECT customer_id, sum(amount) as total
        FROM iceberg_scan(
            '{table_path}',
            snapshot_from_timestamp = '{timestamp2}'::TIMESTAMP
        )
        GROUP BY customer_id
    """).df()

    # Comparar
    merged = t1.merge(t2, on='customer_id', how='outer', suffixes=('_t1', '_t2'))
    merged['change'] = merged['total_t2'] - merged['total_t1']

    return merged[merged['change'] != 0]

# Usar
changes = audit_changes(
    's3://bucket/sales',
    '2024-01-01 00:00:00',
    '2024-01-31 23:59:59'
)
print("Mudanças detectadas:", len(changes))

# Exemplo/Bloco 3
def recover_deleted_records(table_path, before_deletion_timestamp):
    """Recupera registros que foram deletados"""
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    # Dados antes da deleção
    before = con.execute(f"""
        SELECT *
        FROM iceberg_scan(
            '{table_path}',
            snapshot_from_timestamp = '{before_deletion_timestamp}'::TIMESTAMP
        )
    """).df()

    # Dados atuais
    current = con.execute(f"""
        SELECT *
        FROM iceberg_scan('{table_path}')
    """).df()

    # Encontrar deletados
    deleted = before[~before['id'].isin(current['id'])]

    return deleted

# Recuperar registros deletados
deleted_records = recover_deleted_records(
    's3://bucket/users',
    '2024-01-15 10:00:00'
)
print(f"Registros deletados: {len(deleted_records)}")

# Exemplo/Bloco 4
import duckdb
from datetime import datetime, timedelta
import pandas as pd

def monthly_snapshot_analysis(table_path, months_back=12):
    """Análise de snapshots mensais"""
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    results = []

    for i in range(months_back):
        month_date = datetime.now() - timedelta(days=30 * i)
        month_str = month_date.strftime('%Y-%m-01 00:00:00')

        stats = con.execute(f"""
            SELECT
                '{month_str}' as snapshot_date,
                count(*) as record_count,
                sum(amount) as total_amount
            FROM iceberg_scan(
                '{table_path}',
                snapshot_from_timestamp = '{month_str}'::TIMESTAMP
            )
        """).fetchone()

        results.append(stats)

    return pd.DataFrame(results, columns=['date', 'records', 'amount'])

# Análise histórica
history = monthly_snapshot_analysis('s3://bucket/sales', months_back=12)
print(history)

# Exemplo/Bloco 5
import duckdb

class IcebergVersionManager:
    def __init__(self, table_path):
        self.table_path = table_path
        self.con = duckdb.connect()
        self.# LOAD iceberg handled by safe_install_ext

    def list_versions(self):
        """Lista todas as versões disponíveis"""
        return self.con.execute(f"""
            SELECT
                snapshot_id,
                sequence_number,
                to_timestamp(timestamp_ms / 1000) as created_at,
                timestamp_ms
            FROM iceberg_snapshots('{self.table_path}')
            ORDER BY sequence_number DESC
        """).df()

    def get_version_at_time(self, timestamp):
        """Retorna snapshot mais próximo de um timestamp"""
        result = self.con.execute(f"""
            SELECT snapshot_id
            FROM iceberg_snapshots('{self.table_path}')
            WHERE timestamp_ms <= {int(timestamp.timestamp() * 1000)}
            ORDER BY sequence_number DESC
            LIMIT 1
        """).fetchone()

        return result[0] if result else None

# Usar
vm = IcebergVersionManager('s3://bucket/sales')
versions = vm.list_versions()
print(f"Total de versões: {len(versions)}")

# Exemplo/Bloco 6
# ✅ BOM: Especificar colunas necessárias
SELECT customer_id, amount
FROM iceberg_scan(
    's3://bucket/sales',
    snapshot_from_timestamp = '2024-01-01'::TIMESTAMP
);

# ❌ RUIM: SELECT * em snapshot antigo
SELECT *
FROM iceberg_scan(
    's3://bucket/sales',
    snapshot_from_timestamp = '2024-01-01'::TIMESTAMP
);

# Exemplo/Bloco 7
import duckdb

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


con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# Criar tabela temporária com snapshot específico
con.execute("""
    CREATE TEMP TABLE snapshot_jan1 AS
    SELECT *
    FROM iceberg_scan(
        's3://bucket/sales',
        snapshot_from_timestamp = '2024-01-01'::TIMESTAMP
    )
""")

# Fazer múltiplas análises na tabela temp
result1 = con.execute("SELECT count(*) FROM snapshot_jan1").fetchone()
result2 = con.execute("SELECT sum(amount) FROM snapshot_jan1").fetchone()
