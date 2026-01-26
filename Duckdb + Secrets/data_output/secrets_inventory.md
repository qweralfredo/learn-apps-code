# DuckDB Secrets Inventory

Total de secrets: 6

## GCS

### __default_gcs

- Type: gcs
- Provider: config
- Scope: gcs://, gs://
- Persistent: True
- Storage: local_file

### secret6

- Type: gcs
- Provider: config
- Scope: gcs://, gs://
- Persistent: True
- Storage: local_file

## HTTP

### api_auth

- Type: http
- Provider: config
- Scope: (global)
- Persistent: False
- Storage: memory

## S3

### dev_s3_data

- Type: s3
- Provider: config
- Scope: s3://dev-data/
- Persistent: False
- Storage: memory

### prod_s3_data

- Type: s3
- Provider: config
- Scope: s3://prod-data/
- Persistent: False
- Storage: memory

### secret1

- Type: s3
- Provider: config
- Scope: s3://, s3n://, s3a://
- Persistent: True
- Storage: local_file

