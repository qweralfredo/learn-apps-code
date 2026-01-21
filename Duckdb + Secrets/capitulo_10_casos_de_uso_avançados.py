# -*- coding: utf-8 -*-
"""
Capítulo 10: Casos de Uso Avançados
"""

# Capítulo 10: Casos de Uso Avançados
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL httpfs; LOAD httpfs;")

# Multi-cloud setup simulado
con.execute("""
    CREATE SECRET s3_source (TYPE s3, PROVIDER credential_chain, SCOPE 's3://raw-data/')
""")

con.execute("""
    CREATE SECRET gcs_processing (TYPE gcs, PROVIDER credential_chain, SCOPE 'gs://processing/')
""")

# Exemplo de query cross-cloud (conceitual)
print("""
ETL Pipeline Multi-Cloud:
1. Extract from S3 (raw data)
2. Transform in DuckDB
3. Load to GCS (processed data)
""")

# Listar configuração
secrets = con.execute("SELECT name, type, scope FROM duckdb_secrets()").df()
print("\nConfiguração Multi-Cloud:")
print(secrets)

con.close()
