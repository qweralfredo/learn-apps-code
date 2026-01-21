# -*- coding: utf-8 -*-
"""
Capítulo 4: Trabalhando com Arquivos Parquet
DuckDB - Easy Course

Este script demonstra todas as funcionalidades para trabalhar com arquivos Parquet.
"""

import duckdb
import os
import sys
import tempfile
import time

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def criar_dados_exemplo():
    """Cria dados de exemplo e salva em diferentes formatos."""
    temp_dir = tempfile.gettempdir()

    con = duckdb.connect()

    # Criar tabela de exemplo
    con.sql("""
        CREATE OR REPLACE TABLE vendas_completas AS
        SELECT
            range as id,
            'Produto_' || (range % 10) as produto,
            'Categoria_' || (range % 3) as categoria,
            (range % 100) + 1 as quantidade,
            (range % 1000) + 10.50 as preco,
            DATE '2024-01-01' + INTERVAL (range % 365) DAY as data
        FROM range(1000)
    """)

    # Salvar como CSV para comparação
    csv_file = os.path.join(temp_dir, 'vendas.csv')
    con.sql(f"COPY vendas_completas TO '{csv_file}'")

    # Salvar como Parquet
    parquet_file = os.path.join(temp_dir, 'vendas.parquet')
    con.sql(f"COPY vendas_completas TO '{parquet_file}' (FORMAT parquet)")

    con.close()
    return temp_dir


def exemplo_01_leitura_basica():
    """Exemplo 1: Leitura básica de Parquet."""
    print("=" * 60)
    print("EXEMPLO 1: Leitura Básica de Parquet")
    print("=" * 60)

    temp_dir = criar_dados_exemplo()
    parquet_file = os.path.join(temp_dir, 'vendas.parquet')

    con = duckdb.connect()

    # Leitura direta
    print("\n1.1 - Leitura direta:")
    result = con.sql(f"SELECT * FROM '{parquet_file}' LIMIT 5")
    result.show()

    # Usando read_parquet
    print("\n1.2 - Usando read_parquet:")
    result = con.sql(f"SELECT * FROM read_parquet('{parquet_file}') LIMIT 5")
    result.show()

    # DESCRIBE para ver schema
    print("\n1.3 - DESCRIBE para ver schema:")
    con.sql(f"DESCRIBE SELECT * FROM '{parquet_file}'").show()

    con.close()


def exemplo_02_comparacao_csv_parquet():
    """Exemplo 2: Comparação de performance CSV vs Parquet."""
    print("\n" + "=" * 60)
    print("EXEMPLO 2: Performance CSV vs Parquet")
    print("=" * 60)

    temp_dir = criar_dados_exemplo()
    csv_file = os.path.join(temp_dir, 'vendas.csv')
    parquet_file = os.path.join(temp_dir, 'vendas.parquet')

    con = duckdb.connect()

    # Tamanho dos arquivos
    csv_size = os.path.getsize(csv_file)
    parquet_size = os.path.getsize(parquet_file)

    print(f"\n2.1 - Tamanho dos arquivos:")
    print(f"CSV:     {csv_size:,} bytes ({csv_size / 1024:.2f} KB)")
    print(f"Parquet: {parquet_size:,} bytes ({parquet_size / 1024:.2f} KB)")
    print(f"Economia: {100 * (1 - parquet_size / csv_size):.1f}%")

    # Performance de leitura - CSV
    print(f"\n2.2 - Performance de leitura:")
    start = time.time()
    con.sql(f"SELECT count(*) FROM '{csv_file}'").fetchone()
    csv_time = time.time() - start
    print(f"CSV:     {csv_time:.4f}s")

    # Performance de leitura - Parquet
    start = time.time()
    con.sql(f"SELECT count(*) FROM '{parquet_file}'").fetchone()
    parquet_time = time.time() - start
    print(f"Parquet: {parquet_time:.4f}s")
    print(f"Speedup: {csv_time / parquet_time:.1f}x mais rápido")

    # Projection pushdown (ler apenas 2 colunas)
    print(f"\n2.3 - Projection Pushdown (2 colunas de 6):")
    start = time.time()
    con.sql(f"SELECT produto, preco FROM '{csv_file}'").fetchall()
    csv_proj_time = time.time() - start
    print(f"CSV:     {csv_proj_time:.4f}s")

    start = time.time()
    con.sql(f"SELECT produto, preco FROM '{parquet_file}'").fetchall()
    parquet_proj_time = time.time() - start
    print(f"Parquet: {parquet_proj_time:.4f}s")
    print(f"Speedup: {csv_proj_time / parquet_proj_time:.1f}x mais rápido")

    con.close()


def exemplo_03_multiplos_arquivos():
    """Exemplo 3: Leitura de múltiplos arquivos Parquet."""
    print("\n" + "=" * 60)
    print("EXEMPLO 3: Múltiplos Arquivos Parquet")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()
    con = duckdb.connect()

    # Criar múltiplos arquivos Parquet
    for mes in range(1, 4):
        con.sql(f"""
            COPY (
                SELECT
                    'Produto_' || range as produto,
                    range as quantidade,
                    range * 10.5 as preco,
                    {mes} as mes
                FROM range(10)
            ) TO '{temp_dir}/vendas_2024_{mes:02d}.parquet'
            (FORMAT parquet)
        """)

    # Ler com lista explícita
    print("\n3.1 - Lista explícita de arquivos:")
    result = con.sql(f"""
        SELECT * FROM read_parquet([
            '{temp_dir}/vendas_2024_01.parquet',
            '{temp_dir}/vendas_2024_02.parquet',
            '{temp_dir}/vendas_2024_03.parquet'
        ])
    """)
    print(f"Total de linhas: {result.count('*').fetchone()[0]}")
    result.limit(5).show()

    # Ler com glob pattern
    print("\n3.2 - Usando glob pattern:")
    result = con.sql(f"SELECT * FROM '{temp_dir}/vendas_2024_*.parquet'")
    print(f"Total de linhas: {result.count('*').fetchone()[0]}")
    result.limit(5).show()

    # Com coluna filename
    print("\n3.3 - Com coluna filename:")
    result = con.sql(f"""
        SELECT *, filename
        FROM read_parquet('{temp_dir}/vendas_2024_*.parquet', filename=true)
        LIMIT 5
    """)
    result.show()

    con.close()


def exemplo_04_escrita_parquet():
    """Exemplo 4: Escrita de arquivos Parquet com diferentes compressões."""
    print("\n" + "=" * 60)
    print("EXEMPLO 4: Escrita de Parquet com Compressão")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()
    con = duckdb.connect()

    # Criar dados de exemplo
    con.sql("""
        CREATE OR REPLACE TABLE dados_teste AS
        SELECT
            range as id,
            'texto_' || range as descricao,
            (range % 100) as valor
        FROM range(10000)
    """)

    compressoes = ['snappy', 'zstd', 'lz4', 'uncompressed']

    print("\n4.1 - Comparação de compressões:")
    print(f"{'Compressão':<15} {'Tamanho':<15} {'Tempo':<10}")
    print("-" * 40)

    for comp in compressoes:
        output_file = os.path.join(temp_dir, f'dados_{comp}.parquet')

        start = time.time()
        con.sql(f"""
            COPY dados_teste TO '{output_file}'
            (FORMAT parquet, COMPRESSION {comp})
        """)
        elapsed = time.time() - start

        size = os.path.getsize(output_file)
        print(f"{comp:<15} {size:>10,} bytes {elapsed:>8.4f}s")

    # Parquet com metadados customizados
    print("\n4.2 - Parquet com metadados customizados:")
    metadata_file = os.path.join(temp_dir, 'dados_metadata.parquet')
    con.sql(f"""
        COPY (SELECT 42 AS resposta, 'DuckDB' as ferramenta)
        TO '{metadata_file}' (
            FORMAT parquet,
            KV_METADATA {{
                autor: 'Curso DuckDB',
                versao: '1.0',
                descricao: 'Arquivo de exemplo'
            }}
        )
    """)
    print(f"Arquivo criado: {metadata_file}")

    # Ler metadados
    print("\n4.3 - Ler metadados do arquivo:")
    con.sql(f"SELECT * FROM parquet_kv_metadata('{metadata_file}')").show()

    con.close()


def exemplo_05_metadados_parquet():
    """Exemplo 5: Explorar metadados de arquivos Parquet."""
    print("\n" + "=" * 60)
    print("EXEMPLO 5: Metadados de Arquivos Parquet")
    print("=" * 60)

    temp_dir = criar_dados_exemplo()
    parquet_file = os.path.join(temp_dir, 'vendas.parquet')

    con = duckdb.connect()

    # parquet_metadata
    print("\n5.1 - parquet_metadata (geral):")
    con.sql(f"SELECT * FROM parquet_metadata('{parquet_file}')").show()

    # parquet_schema
    print("\n5.2 - parquet_schema:")
    con.sql(f"SELECT * FROM parquet_schema('{parquet_file}')").show()

    # parquet_file_metadata
    print("\n5.3 - parquet_file_metadata:")
    con.sql(f"SELECT * FROM parquet_file_metadata('{parquet_file}')").show()

    con.close()


def exemplo_06_filter_pushdown():
    """Exemplo 6: Demonstração de Filter Pushdown."""
    print("\n" + "=" * 60)
    print("EXEMPLO 6: Filter Pushdown")
    print("=" * 60)

    temp_dir = criar_dados_exemplo()
    parquet_file = os.path.join(temp_dir, 'vendas.parquet')

    con = duckdb.connect()

    # Query sem filtro
    print("\n6.1 - Query sem filtro (lê tudo):")
    start = time.time()
    result = con.sql(f"SELECT count(*) FROM '{parquet_file}'")
    elapsed1 = time.time() - start
    print(f"Resultado: {result.fetchone()[0]} linhas")
    print(f"Tempo: {elapsed1:.4f}s")

    # Query com filtro (filter pushdown)
    print("\n6.2 - Query com filtro (filter pushdown):")
    start = time.time()
    result = con.sql(f"""
        SELECT count(*)
        FROM '{parquet_file}'
        WHERE data >= '2024-06-01' AND quantidade > 50
    """)
    elapsed2 = time.time() - start
    print(f"Resultado: {result.fetchone()[0]} linhas")
    print(f"Tempo: {elapsed2:.4f}s")

    # EXPLAIN ANALYZE
    print("\n6.3 - EXPLAIN ANALYZE mostra filter pushdown:")
    con.sql(f"""
        EXPLAIN ANALYZE
        SELECT * FROM '{parquet_file}'
        WHERE data >= '2024-06-01'
        LIMIT 5
    """).show()

    con.close()


def exemplo_07_conversao_csv_parquet():
    """Exemplo 7: Conversão CSV para Parquet."""
    print("\n" + "=" * 60)
    print("EXEMPLO 7: Conversão CSV para Parquet")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()

    # Criar CSV grande
    csv_file = os.path.join(temp_dir, 'dados_grandes.csv')

    con = duckdb.connect()

    print("\n7.1 - Criando CSV de exemplo...")
    con.sql(f"""
        COPY (
            SELECT
                range as id,
                'Cliente_' || range as nome,
                'email' || range || '@example.com' as email,
                (range % 1000) + 100 as valor
            FROM range(50000)
        ) TO '{csv_file}'
    """)

    csv_size = os.path.getsize(csv_file)
    print(f"CSV criado: {csv_size:,} bytes ({csv_size / (1024*1024):.2f} MB)")

    # Converter para Parquet
    parquet_file = os.path.join(temp_dir, 'dados_grandes.parquet')

    print("\n7.2 - Convertendo para Parquet (ZSTD)...")
    start = time.time()
    con.sql(f"""
        COPY (SELECT * FROM '{csv_file}')
        TO '{parquet_file}'
        (FORMAT parquet, COMPRESSION zstd)
    """)
    elapsed = time.time() - start

    parquet_size = os.path.getsize(parquet_file)
    print(f"Parquet criado: {parquet_size:,} bytes ({parquet_size / (1024*1024):.2f} MB)")
    print(f"Tempo de conversão: {elapsed:.4f}s")
    print(f"Economia de espaço: {100 * (1 - parquet_size / csv_size):.1f}%")
    print(f"Fator de compressão: {csv_size / parquet_size:.1f}x")

    con.close()


def exemplo_08_python_api():
    """Exemplo 8: Uso da API Python com Parquet."""
    print("\n" + "=" * 60)
    print("EXEMPLO 8: API Python com Parquet")
    print("=" * 60)

    temp_dir = criar_dados_exemplo()
    parquet_file = os.path.join(temp_dir, 'vendas.parquet')

    # Ler Parquet e converter para DataFrame
    print("\n8.1 - Ler Parquet e converter para DataFrame:")
    df = duckdb.read_parquet(parquet_file).df()
    print(df.head())
    print(f"\nShape: {df.shape}")

    # Query e salvar como Parquet
    output_file = os.path.join(temp_dir, 'vendas_python.parquet')
    print(f"\n8.2 - Query e salvar como Parquet:")
    duckdb.sql(f"""
        SELECT
            categoria,
            sum(quantidade) as total_quantidade,
            avg(preco) as preco_medio
        FROM '{parquet_file}'
        GROUP BY categoria
    """).write_parquet(output_file)

    print(f"Arquivo salvo: {output_file}")

    # Verificar arquivo salvo
    print("\n8.3 - Verificar arquivo salvo:")
    duckdb.sql(f"SELECT * FROM '{output_file}'").show()


def exemplo_09_hive_partitioning():
    """Exemplo 9: Hive Partitioning."""
    print("\n" + "=" * 60)
    print("EXEMPLO 9: Hive Partitioning")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()
    con = duckdb.connect()

    # Criar dados
    con.sql("""
        CREATE OR REPLACE TABLE vendas_part AS
        SELECT
            range as id,
            'Produto_' || (range % 5) as produto,
            (range % 12) + 1 as mes,
            2024 as ano,
            range * 10.5 as valor
        FROM range(100)
    """)

    # Salvar com particionamento Hive
    output_dir = os.path.join(temp_dir, 'vendas_particionadas')
    print(f"\n9.1 - Salvando com particionamento Hive:")
    print(f"Diretório: {output_dir}")

    con.sql(f"""
        COPY vendas_part TO '{output_dir}'
        (FORMAT parquet, PARTITION_BY (ano, mes))
    """)

    # Listar arquivos criados
    print("\n9.2 - Estrutura de diretórios criada:")
    for root, dirs, files in os.walk(output_dir):
        level = root.replace(output_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files[:3]:  # Mostrar apenas 3 primeiros
            print(f"{subindent}{file}")
        if len(files) > 3:
            print(f"{subindent}... e mais {len(files) - 3} arquivos")

    # Ler dados particionados
    print("\n9.3 - Ler dados particionados:")
    result = con.sql(f"""
        SELECT ano, mes, count(*) as total
        FROM read_parquet('{output_dir}/**/*.parquet', hive_partitioning=true)
        GROUP BY ano, mes
        ORDER BY ano, mes
        LIMIT 5
    """)
    result.show()

    con.close()


def exercicio_pratico():
    """Exercício prático completo."""
    print("\n" + "=" * 60)
    print("EXERCÍCIO PRÁTICO: Analytics Pipeline com Parquet")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()
    con = duckdb.connect()

    print("\n1. Criar dados de vendas simulados:")
    con.sql("""
        CREATE OR REPLACE TABLE vendas_raw AS
        SELECT
            range as id,
            DATE '2024-01-01' + INTERVAL (range % 365) DAY as data,
            'Loja_' || ((range % 5) + 1) as loja,
            'Produto_' || ((range % 20) + 1) as produto,
            ((range % 10) + 1) as quantidade,
            ((range % 500) + 50) as preco_unitario
        FROM range(10000)
    """)
    print("Dados criados: 10,000 linhas")

    # Salvar como Parquet
    raw_parquet = os.path.join(temp_dir, 'vendas_raw.parquet')
    print(f"\n2. Salvando como Parquet: {raw_parquet}")
    con.sql(f"""
        COPY vendas_raw TO '{raw_parquet}'
        (FORMAT parquet, COMPRESSION zstd)
    """)

    size = os.path.getsize(raw_parquet)
    print(f"Tamanho: {size:,} bytes ({size / 1024:.2f} KB)")

    # Análise: Vendas por loja
    print("\n3. Análise: Top 5 lojas por receita:")
    con.sql(f"""
        SELECT
            loja,
            count(*) as num_vendas,
            sum(quantidade) as total_quantidade,
            sum(quantidade * preco_unitario) as receita_total
        FROM '{raw_parquet}'
        GROUP BY loja
        ORDER BY receita_total DESC
        LIMIT 5
    """).show()

    # Análise: Produtos mais vendidos
    print("\n4. Análise: Top 5 produtos:")
    con.sql(f"""
        SELECT
            produto,
            sum(quantidade) as total_vendido,
            sum(quantidade * preco_unitario) as receita
        FROM '{raw_parquet}'
        GROUP BY produto
        ORDER BY total_vendido DESC
        LIMIT 5
    """).show()

    # Criar agregação mensal e salvar
    monthly_parquet = os.path.join(temp_dir, 'vendas_mensais.parquet')
    print(f"\n5. Criar agregação mensal: {monthly_parquet}")
    con.sql(f"""
        COPY (
            SELECT
                date_trunc('month', data) as mes,
                loja,
                count(*) as num_vendas,
                sum(quantidade * preco_unitario) as receita
            FROM '{raw_parquet}'
            GROUP BY mes, loja
        ) TO '{monthly_parquet}'
        (FORMAT parquet, COMPRESSION zstd)
    """)

    # Verificar resultado
    print("\n6. Verificar agregação mensal (primeiras linhas):")
    con.sql(f"SELECT * FROM '{monthly_parquet}' LIMIT 10").show()

    con.close()
    print("\nExercício concluído!")


def main():
    """Função principal que executa todos os exemplos."""
    print("\n" + "=" * 60)
    print("CAPÍTULO 4: TRABALHANDO COM PARQUET - DUCKDB")
    print("=" * 60)

    try:
        exemplo_01_leitura_basica()
        exemplo_02_comparacao_csv_parquet()
        exemplo_03_multiplos_arquivos()
        exemplo_04_escrita_parquet()
        exemplo_05_metadados_parquet()
        exemplo_06_filter_pushdown()
        exemplo_07_conversao_csv_parquet()
        exemplo_08_python_api()
        exemplo_09_hive_partitioning()
        exercicio_pratico()

        print("\n" + "=" * 60)
        print("TODOS OS EXEMPLOS EXECUTADOS COM SUCESSO!")
        print("=" * 60)

    except Exception as e:
        print(f"\nErro ao executar exemplos: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
