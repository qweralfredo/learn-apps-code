# -*- coding: utf-8 -*-
"""
Capítulo 1: Introdução ao DuckDB
Demonstração prática dos conceitos fundamentais do DuckDB
"""

import duckdb
import sys

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def main():
    print("=" * 80)
    print("CAPÍTULO 1: INTRODUÇÃO AO DUCKDB")
    print("=" * 80)

    # 1. Criar conexão DuckDB em memória
    print("\n1. Criando conexão DuckDB em memória...")
    con = duckdb.connect(':memory:')

    # Verificar versão
    version = con.execute("SELECT version()").fetchone()[0]
    print(f"   Versão do DuckDB: {version}")

    # 2. Query básica
    print("\n2. Query básica - SELECT simples:")
    result = con.execute("SELECT 42 as resposta, 'Hello DuckDB!' as mensagem").fetchone()
    print(f"   Resultado: {result}")

    # 3. Criar dados de exemplo com range
    print("\n3. Gerando dados com range():")
    con.execute("""
        CREATE TABLE vendas AS
        SELECT
            i as venda_id,
            'Cliente ' || (i % 100) as cliente,
            ['Norte', 'Sul', 'Leste', 'Oeste'][i % 4 + 1] as regiao,
            CAST('2024-01-01' AS DATE) + (i % 365) * INTERVAL '1 day' as data_venda,
            RANDOM() * 1000 as valor
        FROM range(0, 1000) tbl(i)
    """)
    print("   ✓ Tabela 'vendas' criada com 1000 registros")

    # 4. Análise de dados
    print("\n4. Análise agregada por região:")
    df = con.execute("""
        SELECT
            regiao,
            COUNT(*) as total_vendas,
            ROUND(AVG(valor), 2) as valor_medio,
            ROUND(SUM(valor), 2) as valor_total
        FROM vendas
        GROUP BY regiao
        ORDER BY valor_total DESC
    """).df()
    print(df.to_string(index=False))

    # 5. Window Functions
    print("\n5. Análise temporal com Window Functions:")
    df_temporal = con.execute("""
        SELECT
            DATE_TRUNC('month', data_venda) as mes,
            COUNT(*) as vendas_mes,
            ROUND(SUM(valor), 2) as receita_mes,
            ROUND(AVG(valor), 2) as ticket_medio,
            ROUND(SUM(SUM(valor)) OVER (ORDER BY DATE_TRUNC('month', data_venda)), 2) as receita_acumulada
        FROM vendas
        GROUP BY DATE_TRUNC('month', data_venda)
        ORDER BY mes
        LIMIT 5
    """).df()
    print(df_temporal.to_string(index=False))

    # 6. Exportar para Parquet
    print("\n6. Exportando dados para Parquet:")
    output_file = "C:/projetos/Cursos/Duckdb + Delta/code/vendas_exemplo.parquet"
    con.execute(f"""
        COPY (SELECT * FROM vendas WHERE regiao = 'Norte')
        TO '{output_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"   ✓ Arquivo criado: {output_file}")

    # 7. Ler Parquet
    print("\n7. Lendo arquivo Parquet:")
    df_parquet = con.execute(f"""
        SELECT regiao, COUNT(*) as total, ROUND(AVG(valor), 2) as media
        FROM read_parquet('{output_file}')
        GROUP BY regiao
    """).df()
    print(df_parquet.to_string(index=False))

    # 8. Performance: Execução Vetorizada
    print("\n8. Demonstração de Performance (Vetorizada):")
    import time

    # Criar tabela maior
    con.execute("""
        CREATE OR REPLACE TABLE vendas_grande AS
        SELECT
            i as id,
            RANDOM() * 10000 as valor,
            ['A', 'B', 'C', 'D', 'E'][i % 5 + 1] as categoria
        FROM range(0, 1000000) tbl(i)
    """)

    start = time.time()
    result = con.execute("""
        SELECT categoria, COUNT(*), ROUND(AVG(valor), 2) as media
        FROM vendas_grande
        GROUP BY categoria
    """).fetchdf()
    elapsed = time.time() - start

    print(f"   ✓ Processados 1.000.000 registros em {elapsed:.3f} segundos")
    print(result.to_string(index=False))

    # 9. CTEs (Common Table Expressions)
    print("\n9. Queries complexas com CTEs:")
    df_cte = con.execute("""
        WITH vendas_mensais AS (
            SELECT
                DATE_TRUNC('month', data_venda) as mes,
                regiao,
                SUM(valor) as receita
            FROM vendas
            GROUP BY 1, 2
        ),
        ranking AS (
            SELECT
                mes,
                regiao,
                receita,
                RANK() OVER (PARTITION BY mes ORDER BY receita DESC) as rank_regiao
            FROM vendas_mensais
        )
        SELECT
            mes,
            regiao,
            ROUND(receita, 2) as receita,
            rank_regiao
        FROM ranking
        WHERE rank_regiao <= 2
        ORDER BY mes, rank_regiao
        LIMIT 10
    """).df()
    print(df_cte.to_string(index=False))

    # 10. Estatísticas da base de dados
    print("\n10. Estatísticas do banco:")
    stats = con.execute("""
        SELECT
            table_name,
            estimated_size as linhas
        FROM duckdb_tables()
        WHERE schema_name = 'main'
    """).df()
    print(stats.to_string(index=False))

    # Fechar conexão
    con.close()

    print("\n" + "=" * 80)
    print("✓ Demonstração do Capítulo 1 concluída com sucesso!")
    print("=" * 80)

if __name__ == "__main__":
    main()
