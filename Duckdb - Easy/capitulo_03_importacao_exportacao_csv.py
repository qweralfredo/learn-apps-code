# -*- coding: utf-8 -*-
"""
Capítulo 3: Importação e Exportação de Arquivos CSV
DuckDB - Easy Course

Este script demonstra todas as funcionalidades de importação e exportação CSV no DuckDB.
"""

import duckdb
import os
import sys
import tempfile
from pathlib import Path

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def criar_csv_exemplo():
    """Cria arquivos CSV de exemplo para demonstração."""
    temp_dir = tempfile.gettempdir()

    # CSV 1: Vendas básicas
    vendas_csv = os.path.join(temp_dir, 'vendas.csv')
    with open(vendas_csv, 'w', encoding='utf-8') as f:
        f.write('produto,quantidade,preco,data\n')
        f.write('Notebook,10,2500.50,2024-01-15\n')
        f.write('Mouse,50,25.00,2024-01-16\n')
        f.write('Teclado,30,150.00,2024-01-17\n')
        f.write('Monitor,15,800.00,2024-01-18\n')

    # CSV 2: Formato brasileiro (ponto-e-vírgula e vírgula decimal)
    vendas_br_csv = os.path.join(temp_dir, 'vendas_br.csv')
    with open(vendas_br_csv, 'w', encoding='utf-8') as f:
        f.write('produto;quantidade;preco;data\n')
        f.write('Notebook;10;2500,50;15/01/2024\n')
        f.write('Mouse;50;25,00;16/01/2024\n')
        f.write('Teclado;30;150,00;17/01/2024\n')

    # CSV 3: Sem cabeçalho
    dados_sem_header = os.path.join(temp_dir, 'dados_sem_header.csv')
    with open(dados_sem_header, 'w', encoding='utf-8') as f:
        f.write('1,Alice,30,São Paulo\n')
        f.write('2,Bob,25,Rio de Janeiro\n')
        f.write('3,Carlos,35,Belo Horizonte\n')

    # CSV 4: Com encoding Latin-1
    latin1_csv = os.path.join(temp_dir, 'dados_latin1.csv')
    with open(latin1_csv, 'w', encoding='latin-1') as f:
        f.write('nome,cidade\n')
        f.write('José,São Paulo\n')
        f.write('María,Rio de Janeiro\n')

    return temp_dir


def exemplo_01_leitura_basica():
    """Exemplo 1: Leitura básica de CSV."""
    print("=" * 60)
    print("EXEMPLO 1: Leitura Básica de CSV")
    print("=" * 60)

    temp_dir = criar_csv_exemplo()
    vendas_csv = os.path.join(temp_dir, 'vendas.csv')

    con = duckdb.connect()

    # Leitura direta - auto-detect
    print("\n1.1 - Leitura direta (auto-detect):")
    result = con.sql(f"SELECT * FROM '{vendas_csv}'")
    result.show()

    # Usando read_csv
    print("\n1.2 - Usando read_csv():")
    result = con.sql(f"SELECT * FROM read_csv('{vendas_csv}')")
    result.show()

    # Python API
    print("\n1.3 - Usando Python API:")
    rel = duckdb.read_csv(vendas_csv)
    print(rel)

    con.close()


def exemplo_02_parametros_read_csv():
    """Exemplo 2: Parâmetros personalizados do read_csv."""
    print("\n" + "=" * 60)
    print("EXEMPLO 2: Parâmetros Personalizados")
    print("=" * 60)

    temp_dir = criar_csv_exemplo()
    vendas_br_csv = os.path.join(temp_dir, 'vendas_br.csv')

    con = duckdb.connect()

    # CSV com formato brasileiro
    print("\n2.1 - CSV formato brasileiro (delim=';', decimal=','):")
    result = con.sql(f"""
        SELECT * FROM read_csv('{vendas_br_csv}',
            delim = ';',
            decimal_separator = ',',
            dateformat = '%d/%m/%Y'
        )
    """)
    result.show()

    # CSV sem cabeçalho
    dados_sem_header = os.path.join(temp_dir, 'dados_sem_header.csv')
    print("\n2.2 - CSV sem cabeçalho:")
    result = con.sql(f"""
        SELECT * FROM read_csv('{dados_sem_header}',
            header = false,
            columns = {{
                'id': 'INTEGER',
                'nome': 'VARCHAR',
                'idade': 'INTEGER',
                'cidade': 'VARCHAR'
            }}
        )
    """)
    result.show()

    # CSV com encoding Latin-1
    latin1_csv = os.path.join(temp_dir, 'dados_latin1.csv')
    print("\n2.3 - CSV com encoding Latin-1:")
    result = con.sql(f"""
        SELECT * FROM read_csv('{latin1_csv}',
            encoding = 'latin-1'
        )
    """)
    result.show()

    con.close()


def exemplo_03_criar_tabelas():
    """Exemplo 3: Criar tabelas a partir de CSV."""
    print("\n" + "=" * 60)
    print("EXEMPLO 3: Criar Tabelas a partir de CSV")
    print("=" * 60)

    temp_dir = criar_csv_exemplo()
    vendas_csv = os.path.join(temp_dir, 'vendas.csv')

    con = duckdb.connect()

    # CREATE TABLE AS (recomendado)
    print("\n3.1 - CREATE TABLE AS:")
    con.sql(f"CREATE TABLE vendas AS SELECT * FROM '{vendas_csv}'")
    con.sql("SELECT * FROM vendas").show()

    # Usando COPY
    print("\n3.2 - Usando COPY:")
    con.sql("DROP TABLE IF EXISTS vendas2")
    con.sql("""
        CREATE TABLE vendas2 (
            produto VARCHAR,
            quantidade INTEGER,
            preco DECIMAL(10, 2),
            data DATE
        )
    """)
    con.sql(f"COPY vendas2 FROM '{vendas_csv}'")
    con.sql("SELECT * FROM vendas2").show()

    con.close()


def exemplo_04_exportacao_csv():
    """Exemplo 4: Exportação para CSV."""
    print("\n" + "=" * 60)
    print("EXEMPLO 4: Exportação para CSV")
    print("=" * 60)

    temp_dir = criar_csv_exemplo()
    vendas_csv = os.path.join(temp_dir, 'vendas.csv')

    con = duckdb.connect()
    con.sql(f"CREATE TABLE vendas AS SELECT * FROM '{vendas_csv}'")

    # Exportar para CSV
    output_csv = os.path.join(temp_dir, 'vendas_output.csv')
    print(f"\n4.1 - Exportando para: {output_csv}")
    con.sql(f"COPY vendas TO '{output_csv}'")

    # Verificar arquivo exportado
    print("\n4.2 - Verificando arquivo exportado:")
    result = con.sql(f"SELECT * FROM '{output_csv}'")
    result.show()

    # Exportar com delimitador customizado
    output_br_csv = os.path.join(temp_dir, 'vendas_br_output.csv')
    print(f"\n4.3 - Exportando formato brasileiro: {output_br_csv}")
    con.sql(f"""
        COPY vendas TO '{output_br_csv}'
        (DELIMITER ';', HEADER)
    """)

    # Exportar query
    output_query_csv = os.path.join(temp_dir, 'vendas_filtradas.csv')
    print(f"\n4.4 - Exportando resultado de query: {output_query_csv}")
    con.sql(f"""
        COPY (
            SELECT produto, quantidade * preco as total
            FROM vendas
            WHERE preco > 100
        ) TO '{output_query_csv}'
    """)

    result = con.sql(f"SELECT * FROM '{output_query_csv}'")
    result.show()

    con.close()


def exemplo_05_python_api():
    """Exemplo 5: Uso da API Python."""
    print("\n" + "=" * 60)
    print("EXEMPLO 5: API Python para CSV")
    print("=" * 60)

    temp_dir = criar_csv_exemplo()
    vendas_csv = os.path.join(temp_dir, 'vendas.csv')

    # Ler CSV e converter para DataFrame
    print("\n5.1 - Ler CSV e converter para DataFrame:")
    df = duckdb.read_csv(vendas_csv).df()
    print(df)
    print(f"\nTipo: {type(df)}")

    # Query direta
    print("\n5.2 - Query direta:")
    result = duckdb.sql(f"SELECT * FROM '{vendas_csv}' WHERE quantidade > 20")
    print(result.df())

    # Salvar como CSV via Python
    output_python_csv = os.path.join(temp_dir, 'vendas_python.csv')
    print(f"\n5.3 - Salvar como CSV via Python: {output_python_csv}")
    duckdb.sql(f"SELECT * FROM '{vendas_csv}'").write_csv(output_python_csv)
    print(f"Arquivo salvo em: {output_python_csv}")


def exemplo_06_pipeline_completo():
    """Exemplo 6: Pipeline completo - Ler, Transformar e Salvar."""
    print("\n" + "=" * 60)
    print("EXEMPLO 6: Pipeline Completo ETL")
    print("=" * 60)

    temp_dir = criar_csv_exemplo()
    vendas_csv = os.path.join(temp_dir, 'vendas.csv')

    con = duckdb.connect()

    # Pipeline: Ler CSV, transformar e salvar
    output_pipeline = os.path.join(temp_dir, 'vendas_transformadas.csv')
    print(f"\n6.1 - Executando pipeline ETL...")

    con.sql(f"""
        COPY (
            SELECT
                produto,
                quantidade,
                preco,
                quantidade * preco as valor_total,
                CASE
                    WHEN preco > 500 THEN 'Alto'
                    WHEN preco > 100 THEN 'Médio'
                    ELSE 'Baixo'
                END as categoria_preco
            FROM '{vendas_csv}'
            ORDER BY valor_total DESC
        ) TO '{output_pipeline}' (HEADER, DELIMITER ',')
    """)

    print(f"Pipeline executado! Arquivo salvo em: {output_pipeline}")

    # Visualizar resultado
    print("\n6.2 - Resultado do pipeline:")
    result = con.sql(f"SELECT * FROM '{output_pipeline}'")
    result.show()

    con.close()


def exemplo_07_multiplos_csvs():
    """Exemplo 7: Trabalhando com múltiplos CSVs."""
    print("\n" + "=" * 60)
    print("EXEMPLO 7: Múltiplos Arquivos CSV")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()

    # Criar múltiplos CSVs
    for i in range(1, 4):
        csv_file = os.path.join(temp_dir, f'vendas_mes_{i}.csv')
        with open(csv_file, 'w', encoding='utf-8') as f:
            f.write('produto,quantidade,mes\n')
            f.write(f'Produto_A,{i*10},{i}\n')
            f.write(f'Produto_B,{i*20},{i}\n')

    con = duckdb.connect()

    # Ler múltiplos CSVs usando glob
    print("\n7.1 - Ler múltiplos CSVs com glob pattern:")
    pattern = os.path.join(temp_dir, 'vendas_mes_*.csv')
    result = con.sql(f"SELECT * FROM '{pattern}'")
    result.show()

    # Agregar dados de múltiplos CSVs
    print("\n7.2 - Agregar dados de múltiplos CSVs:")
    result = con.sql(f"""
        SELECT
            produto,
            sum(quantidade) as total_quantidade,
            count(*) as num_registros
        FROM '{pattern}'
        GROUP BY produto
    """)
    result.show()

    con.close()


def exercicio_pratico():
    """Exercício prático completo."""
    print("\n" + "=" * 60)
    print("EXERCÍCIO PRÁTICO: Sistema de Vendas")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()

    # Criar CSV de vendas
    vendas_csv = os.path.join(temp_dir, 'sistema_vendas.csv')
    with open(vendas_csv, 'w', encoding='utf-8') as f:
        f.write('id,cliente,produto,quantidade,preco_unitario,data\n')
        f.write('1,Alice,Notebook,2,2500.00,2024-01-15\n')
        f.write('2,Bob,Mouse,5,25.00,2024-01-15\n')
        f.write('3,Alice,Teclado,3,150.00,2024-01-16\n')
        f.write('4,Carlos,Monitor,1,800.00,2024-01-16\n')
        f.write('5,Bob,Notebook,1,2500.00,2024-01-17\n')
        f.write('6,Alice,Mouse,10,25.00,2024-01-17\n')

    con = duckdb.connect()

    print("\n1. Importar CSV:")
    con.sql(f"CREATE TABLE vendas AS SELECT * FROM '{vendas_csv}'")
    con.sql("SELECT * FROM vendas").show()

    print("\n2. Calcular total de vendas por produto:")
    con.sql("""
        SELECT
            produto,
            sum(quantidade) as qtd_total,
            sum(quantidade * preco_unitario) as valor_total,
            count(*) as num_vendas
        FROM vendas
        GROUP BY produto
        ORDER BY valor_total DESC
    """).show()

    print("\n3. Vendas por cliente:")
    con.sql("""
        SELECT
            cliente,
            count(*) as num_compras,
            sum(quantidade * preco_unitario) as valor_total
        FROM vendas
        GROUP BY cliente
        ORDER BY valor_total DESC
    """).show()

    # Exportar relatório
    relatorio_csv = os.path.join(temp_dir, 'relatorio_vendas.csv')
    print(f"\n4. Exportar relatório para: {relatorio_csv}")
    con.sql(f"""
        COPY (
            SELECT
                cliente,
                produto,
                quantidade,
                preco_unitario,
                quantidade * preco_unitario as total,
                data
            FROM vendas
            ORDER BY data, cliente
        ) TO '{relatorio_csv}' (HEADER, DELIMITER ',')
    """)

    print("Relatório exportado com sucesso!")

    con.close()


def main():
    """Função principal que executa todos os exemplos."""
    print("\n" + "=" * 60)
    print("CAPÍTULO 3: IMPORTAÇÃO E EXPORTAÇÃO CSV - DUCKDB")
    print("=" * 60)

    try:
        exemplo_01_leitura_basica()
        exemplo_02_parametros_read_csv()
        exemplo_03_criar_tabelas()
        exemplo_04_exportacao_csv()
        exemplo_05_python_api()
        exemplo_06_pipeline_completo()
        exemplo_07_multiplos_csvs()
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
