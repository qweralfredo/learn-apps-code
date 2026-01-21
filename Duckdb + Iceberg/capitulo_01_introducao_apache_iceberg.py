# -*- coding: utf-8 -*-
"""
Capítulo 1: Introdução ao Apache Iceberg
Curso: DuckDB + Apache Iceberg

Este script demonstra os conceitos fundamentais do Apache Iceberg
"""

import duckdb
import sys

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def setup_duckdb():
    """Configurar DuckDB com extensão Iceberg"""
    con = duckdb.connect()

    # Instalar e carregar extensão Iceberg
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")

    print("✅ DuckDB configurado com Iceberg")
    return con

def exemplo_iceberg_vs_parquet():
    """Demonstra diferenças entre Iceberg e Parquet"""
    print("\n" + "="*60)
    print("ICEBERG VS PARQUET")
    print("="*60)

    print("""
    PARQUET (Formato de Arquivo):
    ❌ Sem controle de versão
    ❌ Sem transações ACID
    ❌ Schema fixo
    ❌ Particionamento manual

    ICEBERG (Formato de Tabela):
    ✅ Versionamento completo (snapshots)
    ✅ Transações ACID garantidas
    ✅ Schema evolution automático
    ✅ Hidden partitioning
    ✅ Time travel
    """)

def exemplo_arquitetura_iceberg():
    """Mostra arquitetura em camadas do Iceberg"""
    print("\n" + "="*60)
    print("ARQUITETURA DO ICEBERG")
    print("="*60)

    print("""
    ┌─────────────────────────────────────┐
    │     CATALOG (REST/Glue/Unity)       │  ← Gerencia metadados
    └─────────────────────────────────────┘
                  ↓
    ┌─────────────────────────────────────┐
    │     METADATA FILES (.json)          │  ← Schema, partições, snapshots
    └─────────────────────────────────────┘
                  ↓
    ┌─────────────────────────────────────┐
    │     MANIFEST LISTS (.avro)          │  ← Lista de manifests
    └─────────────────────────────────────┘
                  ↓
    ┌─────────────────────────────────────┐
    │     MANIFEST FILES (.avro)          │  ← Lista de data files
    └─────────────────────────────────────┘
                  ↓
    ┌─────────────────────────────────────┐
    │     DATA FILES (.parquet)           │  ← Dados reais
    └─────────────────────────────────────┘
    """)

def exemplo_conceitos_fundamentais():
    """Demonstra conceitos fundamentais"""
    print("\n" + "="*60)
    print("CONCEITOS FUNDAMENTAIS")
    print("="*60)

    print("""
    1. SNAPSHOTS (Versionamento):
       - Cada modificação cria novo snapshot
       - Snapshots são imutáveis
       - Permite time travel

    2. SCHEMA EVOLUTION:
       - Adicionar colunas sem reescrever dados
       - Renomear colunas (apenas metadados)
       - Mudar tipos compatíveis

    3. HIDDEN PARTITIONING:
       - Particionamento transparente
       - Usuário não precisa saber sobre partições
       - Query: WHERE sale_date = '2024-01-15'
       - Iceberg usa partições automaticamente

    4. PARTITION EVOLUTION:
       - Mudar estratégia de particionamento
       - Sem reescrever dados antigos
       - Mantém compatibilidade
    """)

def exemplo_quando_usar_iceberg():
    """Mostra quando usar Iceberg"""
    print("\n" + "="*60)
    print("QUANDO USAR ICEBERG?")
    print("="*60)

    print("""
    ✅ USE ICEBERG QUANDO:
       • Precisa de transações ACID
       • Múltiplos writers concorrentes
       • Schema muda com frequência
       • Precisa de time travel / auditoria
       • Dados particionados complexos
       • Interoperabilidade entre engines
       • Lakehouse architecture

    ❌ NÃO PRECISA DE ICEBERG QUANDO:
       • Dados pequenos (< 1 GB)
       • Read-only workloads simples
       • Single writer, no concurrency
       • Schema nunca muda
       • Não precisa de versionamento
       • Apenas análise ad-hoc rápida
    """)

def exemplo_terminologia():
    """Mostra terminologia essencial"""
    print("\n" + "="*60)
    print("TERMINOLOGIA ESSENCIAL")
    print("="*60)

    termos = {
        "Snapshot": "Versão imutável da tabela em um momento específico",
        "Manifest": "Lista de arquivos de dados com metadados (min/max, contagens)",
        "Manifest List": "Lista de manifests que compõem um snapshot",
        "Data File": "Arquivo Parquet/Avro/ORC com dados reais",
        "Metadata File": "Arquivo JSON com schema, partições e histórico",
        "Catalog": "Sistema que rastreia tabelas Iceberg e seus metadados",
        "Partition Spec": "Definição de como dados são particionados",
        "Schema": "Definição de colunas e tipos da tabela",
    }

    for termo, definicao in termos.items():
        print(f"\n{termo}:")
        print(f"  → {definicao}")

def exemplo_duckdb_iceberg():
    """Exemplo básico de DuckDB + Iceberg"""
    print("\n" + "="*60)
    print("EXEMPLO: DuckDB + Iceberg")
    print("="*60)

    print("""
    Por que DuckDB + Iceberg?

    DuckDB oferece:
    • 🚀 Performance extrema para OLAP
    • 💾 Processamento in-memory eficiente
    • 🐍 Integração perfeita com Python/Pandas
    • 📦 Zero dependências (embedded database)

    Iceberg oferece:
    • 📊 Gerenciamento de tabelas em escala
    • 🕐 Time travel e versionamento
    • 🔄 ACID transactions
    • 🌐 Interoperabilidade (Spark, Flink, Trino)

    Código exemplo:

    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")

    # Query em tabela Iceberg
    result = con.execute('''
        SELECT category, sum(revenue)
        FROM iceberg_scan('s3://bucket/sales')
        GROUP BY category
    ''').df()
    """)

def main():
    """Função principal"""
    print("="*60)
    print("CAPÍTULO 1: Introdução ao Apache Iceberg")
    print("="*60)

    # Setup DuckDB
    con = setup_duckdb()

    # Exemplos conceituais
    exemplo_iceberg_vs_parquet()
    exemplo_arquitetura_iceberg()
    exemplo_conceitos_fundamentais()
    exemplo_quando_usar_iceberg()
    exemplo_terminologia()
    exemplo_duckdb_iceberg()

    print("\n" + "="*60)
    print("EXERCÍCIOS PRÁTICOS:")
    print("="*60)
    print("""
    1. Pesquise sobre outros formatos de tabela (Delta Lake, Hudi)
    2. Leia a documentação oficial do Apache Iceberg
    3. Explore exemplos de uso do DuckDB com Iceberg
    4. Identifique casos de uso na sua organização
    5. Configure um ambiente de desenvolvimento local
    """)

    print("\n✅ Capítulo 1 concluído!")
    print("📚 Próximo: Capítulo 2 - Instalação e Configuração")

if __name__ == "__main__":
    main()
