# -*- coding: utf-8 -*-
"""
Iceberg-04-metadados-snapshots
"""

# Iceberg-04-metadados-snapshots
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

con = duckdb.connect()
con.execute("LOAD iceberg")

# Explorar metadados
metadata = con.execute("""
    SELECT
        file_path,
        record_count,
        file_size_in_bytes / 1024 / 1024 as size_mb,
        status,
        content
    FROM iceberg_metadata('s3://bucket/sales')
""").df()

print(f"Total de arquivos: {len(metadata)}")
print(f"Total de registros: {metadata['record_count'].sum():,}")
print(f"Tamanho total: {metadata['size_mb'].sum():.2f} MB")

# Exemplo/Bloco 2
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("LOAD iceberg")

# Ver histórico de snapshots
snapshots = con.execute("""
    SELECT
        snapshot_id,
        to_timestamp(timestamp_ms / 1000) as snapshot_time,
        sequence_number
    FROM iceberg_snapshots('s3://bucket/sales')
    ORDER BY sequence_number DESC
""").df()

print("Histórico de Snapshots:")
print(snapshots)

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()
con.execute("LOAD iceberg")

stats = con.execute("""
    SELECT
        count(*) as total_files,
        sum(record_count) as total_records,
        sum(file_size_in_bytes) / 1024 / 1024 / 1024 as total_size_gb,
        avg(file_size_in_bytes) / 1024 / 1024 as avg_file_size_mb,
        min(record_count) as min_records,
        max(record_count) as max_records
    FROM iceberg_metadata('s3://bucket/sales')
    WHERE status = 'EXISTING'
""").fetchone()

print(f"""
Estatísticas da Tabela Iceberg:
- Total de arquivos: {stats[0]:,}
- Total de registros: {stats[1]:,}
- Tamanho total: {stats[2]:.2f} GB
- Tamanho médio por arquivo: {stats[3]:.2f} MB
- Registros por arquivo: min={stats[4]:,}, max={stats[5]:,}
""")

# Exemplo/Bloco 4
import duckdb
import matplotlib.pyplot as plt

con = duckdb.connect()
con.execute("LOAD iceberg")

# Análise de crescimento
growth = con.execute("""
    SELECT
        to_timestamp(timestamp_ms / 1000) as snapshot_time,
        snapshot_id,
        sequence_number
    FROM iceberg_snapshots('s3://bucket/sales')
    ORDER BY sequence_number
""").df()

# Visualizar (se matplotlib disponível)
plt.figure(figsize=(12, 6))
plt.plot(growth['snapshot_time'], growth['sequence_number'])
plt.title('Evolução de Snapshots')
plt.xlabel('Data')
plt.ylabel('Número de Sequência')
plt.grid(True)
plt.show()

# Exemplo/Bloco 5
import duckdb
from datetime import datetime

class IcebergMetadataExplorer:
    def __init__(self, table_path):
        self.table_path = table_path
        self.con = duckdb.connect()
        self.con.execute("LOAD iceberg")

    def get_file_stats(self):
        """Estatísticas de arquivos"""
        return self.con.execute(f"""
            SELECT
                count(*) as file_count,
                sum(record_count) as total_records,
                sum(file_size_in_bytes) / 1024 / 1024 as total_mb
            FROM iceberg_metadata('{self.table_path}')
            WHERE status = 'EXISTING'
        """).fetchone()

    def get_snapshot_history(self):
        """Histórico de snapshots"""
        return self.con.execute(f"""
            SELECT *
            FROM iceberg_snapshots('{self.table_path}')
            ORDER BY sequence_number DESC
        """).df()

    def get_latest_snapshot(self):
        """Último snapshot"""
        return self.con.execute(f"""
            SELECT snapshot_id, timestamp_ms
            FROM iceberg_snapshots('{self.table_path}')
            ORDER BY sequence_number DESC
            LIMIT 1
        """).fetchone()

# Usar
explorer = IcebergMetadataExplorer('s3://bucket/sales')
stats = explorer.get_file_stats()
print(f"Arquivos: {stats[0]}, Registros: {stats[1]:,}")

