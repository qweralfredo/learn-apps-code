# -*- coding: utf-8 -*-
"""
Capítulo 4: Leitura de Tabelas Delta
Técnicas avançadas de leitura e consulta de tabelas Delta
"""

import duckdb
import sys
import pandas as pd
from pathlib import Path

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def preparar_dados_demonstracao():
    """Preparar dados para demonstração"""
    print("\n1. PREPARANDO DADOS PARA DEMONSTRAÇÃO")
    print("-" * 60)

    try:
        from deltalake import write_deltalake

        base_path = Path("C:/projetos/Cursos/Duckdb + Delta/code/delta_leitura")
        base_path.mkdir(exist_ok=True)

        con = duckdb.connect()

        # Criar tabela de vendas com mais dados
        print("   Criando tabela de vendas...")
        vendas_df = con.execute("""
            SELECT
                i as pedido_id,
                1 + (i % 500) as cliente_id,
                ['Norte', 'Sul', 'Leste', 'Oeste', 'Centro'][i % 5 + 1] as regiao,
                ['Eletrônicos', 'Roupas', 'Alimentos', 'Livros', 'Móveis'][i % 5 + 1] as categoria,
                CAST('2024-01-01' AS DATE) + (i % 120) * INTERVAL '1 day' as data_pedido,
                50.0 + RANDOM() * 950 as valor_pedido,
                [true, false][i % 2 + 1] as entregue
            FROM range(1, 5001) tbl(i)
        """).df()

        write_deltalake(str(base_path / "vendas"), vendas_df, mode="overwrite")
        print(f"   ✓ Tabela criada com {len(vendas_df)} registros")

        con.close()
        return base_path

    except ImportError:
        print("   ✗ Erro: biblioteca 'deltalake' não instalada")
        return None

def queries_basicas(base_path):
    """Demonstrar queries básicas"""
    print("\n2. QUERIES BÁSICAS")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    # SELECT com filtros
    print("\n   a) SELECT com filtros múltiplos:")
    df = con.execute(f"""
        SELECT
            pedido_id,
            regiao,
            categoria,
            data_pedido,
            ROUND(valor_pedido, 2) as valor
        FROM delta_scan('{base_path}/vendas')
        WHERE regiao = 'Sul'
          AND valor_pedido > 500
          AND data_pedido >= '2024-02-01'
        ORDER BY valor_pedido DESC
        LIMIT 10
    """).df()
    print(df.to_string(index=False))

    # Agregações
    print("\n   b) Agregações por região:")
    df_agg = con.execute(f"""
        SELECT
            regiao,
            COUNT(*) as total_pedidos,
            ROUND(SUM(valor_pedido), 2) as receita_total,
            ROUND(AVG(valor_pedido), 2) as ticket_medio,
            ROUND(MIN(valor_pedido), 2) as menor_pedido,
            ROUND(MAX(valor_pedido), 2) as maior_pedido
        FROM delta_scan('{base_path}/vendas')
        GROUP BY regiao
        ORDER BY receita_total DESC
    """).df()
    print(df_agg.to_string(index=False))

    con.close()

def queries_avancadas(base_path):
    """Demonstrar queries avançadas"""
    print("\n3. QUERIES AVANÇADAS")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    # Window Functions
    print("\n   a) Ranking de pedidos por região:")
    df_rank = con.execute(f"""
        SELECT
            regiao,
            pedido_id,
            ROUND(valor_pedido, 2) as valor,
            RANK() OVER (PARTITION BY regiao ORDER BY valor_pedido DESC) as ranking
        FROM delta_scan('{base_path}/vendas')
        QUALIFY ranking <= 3
        ORDER BY regiao, ranking
    """).df()
    print(df_rank.to_string(index=False))

    # CTEs (Common Table Expressions)
    print("\n   b) Análise com CTEs - Crescimento mensal:")
    df_cte = con.execute(f"""
        WITH vendas_mensais AS (
            SELECT
                DATE_TRUNC('month', data_pedido) as mes,
                COUNT(*) as pedidos,
                SUM(valor_pedido) as receita
            FROM delta_scan('{base_path}/vendas')
            GROUP BY 1
        )
        SELECT
            mes,
            pedidos,
            ROUND(receita, 2) as receita,
            ROUND(receita - LAG(receita) OVER (ORDER BY mes), 2) as variacao,
            ROUND((receita - LAG(receita) OVER (ORDER BY mes)) / LAG(receita) OVER (ORDER BY mes) * 100, 2) as crescimento_pct
        FROM vendas_mensais
        ORDER BY mes
    """).df()
    print(df_cte.to_string(index=False))

    # Subqueries
    print("\n   c) Subquery - Clientes com pedidos acima da média:")
    df_sub = con.execute(f"""
        SELECT
            cliente_id,
            COUNT(*) as qtd_pedidos,
            ROUND(AVG(valor_pedido), 2) as ticket_medio
        FROM delta_scan('{base_path}/vendas')
        GROUP BY cliente_id
        HAVING AVG(valor_pedido) > (
            SELECT AVG(valor_pedido)
            FROM delta_scan('{base_path}/vendas')
        )
        ORDER BY ticket_medio DESC
        LIMIT 10
    """).df()
    print(df_sub.to_string(index=False))

    con.close()

def analise_temporal(base_path):
    """Análise temporal de dados"""
    print("\n4. ANÁLISE TEMPORAL")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    # Análise por dia da semana
    print("\n   a) Vendas por dia da semana:")
    df_dow = con.execute(f"""
        SELECT
            CASE DAYOFWEEK(data_pedido)
                WHEN 0 THEN 'Domingo'
                WHEN 1 THEN 'Segunda'
                WHEN 2 THEN 'Terça'
                WHEN 3 THEN 'Quarta'
                WHEN 4 THEN 'Quinta'
                WHEN 5 THEN 'Sexta'
                WHEN 6 THEN 'Sábado'
            END as dia_semana,
            COUNT(*) as total_pedidos,
            ROUND(AVG(valor_pedido), 2) as ticket_medio
        FROM delta_scan('{base_path}/vendas')
        GROUP BY DAYOFWEEK(data_pedido), dia_semana
        ORDER BY DAYOFWEEK(data_pedido)
    """).df()
    print(df_dow.to_string(index=False))

    # Tendência diária
    print("\n   b) Tendência diária (últimos 14 dias):")
    df_trend = con.execute(f"""
        SELECT
            data_pedido,
            COUNT(*) as pedidos,
            ROUND(SUM(valor_pedido), 2) as receita,
            ROUND(AVG(valor_pedido), 2) as ticket_medio,
            ROUND(AVG(COUNT(*)) OVER (
                ORDER BY data_pedido
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2) as media_movel_7d
        FROM delta_scan('{base_path}/vendas')
        GROUP BY data_pedido
        ORDER BY data_pedido DESC
        LIMIT 14
    """).df()
    print(df_trend.to_string(index=False))

    con.close()

def exportar_resultados(base_path):
    """Exportar resultados para diferentes formatos"""
    print("\n5. EXPORTANDO RESULTADOS")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    output_path = Path("C:/projetos/Cursos/Duckdb + Delta/code/exports")
    output_path.mkdir(exist_ok=True)

    # Exportar para Parquet
    print("   a) Exportando para Parquet...")
    parquet_file = str(output_path / "vendas_norte.parquet")
    con.execute(f"""
        COPY (
            SELECT * FROM delta_scan('{base_path}/vendas')
            WHERE regiao = 'Norte'
        ) TO '{parquet_file}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"   ✓ Arquivo criado: {parquet_file}")

    # Exportar para CSV
    print("   b) Exportando para CSV...")
    csv_file = str(output_path / "resumo_categoria.csv")
    con.execute(f"""
        COPY (
            SELECT
                categoria,
                COUNT(*) as total,
                ROUND(AVG(valor_pedido), 2) as media
            FROM delta_scan('{base_path}/vendas')
            GROUP BY categoria
        ) TO '{csv_file}' (HEADER, DELIMITER ',')
    """)
    print(f"   ✓ Arquivo criado: {csv_file}")

    # Exportar para JSON
    print("   c) Exportando para JSON...")
    json_file = str(output_path / "top_pedidos.json")
    con.execute(f"""
        COPY (
            SELECT pedido_id, regiao, categoria, valor_pedido
            FROM delta_scan('{base_path}/vendas')
            ORDER BY valor_pedido DESC
            LIMIT 10
        ) TO '{json_file}' (FORMAT JSON, ARRAY true)
    """)
    print(f"   ✓ Arquivo criado: {json_file}")

    con.close()

def integracao_pandas(base_path):
    """Integração com Pandas"""
    print("\n6. INTEGRAÇÃO COM PANDAS")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    # Ler para DataFrame
    print("   a) Lendo Delta para Pandas DataFrame:")
    df = con.execute(f"""
        SELECT * FROM delta_scan('{base_path}/vendas')
        WHERE data_pedido >= '2024-03-01'
    """).df()

    print(f"   ✓ {len(df)} registros carregados")
    print(f"   Colunas: {list(df.columns)}")
    print(f"   Tipos: {df.dtypes.to_dict()}")

    # Análise com Pandas
    print("\n   b) Análise descritiva com Pandas:")
    stats = df[['valor_pedido']].describe()
    print(stats)

    # Processar com Pandas e retornar para DuckDB
    print("\n   c) Processamento híbrido Pandas + DuckDB:")
    df_processado = df[df['entregue'] == True].copy()
    df_processado['valor_com_desconto'] = df_processado['valor_pedido'] * 0.9

    # Consultar DataFrame processado com DuckDB
    result = con.execute("""
        SELECT
            regiao,
            COUNT(*) as entregas,
            ROUND(AVG(valor_com_desconto), 2) as media_com_desconto
        FROM df_processado
        GROUP BY regiao
        ORDER BY media_com_desconto DESC
    """).df()
    print(result.to_string(index=False))

    con.close()

def main():
    print("=" * 80)
    print("CAPÍTULO 4: LEITURA DE TABELAS DELTA")
    print("=" * 80)

    base_path = preparar_dados_demonstracao()

    if base_path:
        queries_basicas(base_path)
        queries_avancadas(base_path)
        analise_temporal(base_path)
        exportar_resultados(base_path)
        integracao_pandas(base_path)

        print("\n" + "=" * 80)
        print("✓ Demonstração do Capítulo 4 concluída com sucesso!")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("✗ Demonstração não concluída - instale 'deltalake'")
        print("=" * 80)

if __name__ == "__main__":
    main()
