# -*- coding: utf-8 -*-
"""
Capítulo 3: Introdução à Extensão Delta
Criação e leitura de tabelas Delta Lake com DuckDB
"""

import duckdb
import sys
import pandas as pd
from pathlib import Path

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def criar_tabelas_delta_exemplo():
    """Criar tabelas Delta de exemplo para demonstração"""
    print("\n1. CRIANDO TABELAS DELTA DE EXEMPLO")
    print("-" * 60)

    try:
        from deltalake import write_deltalake

        base_path = Path("C:/projetos/Cursos/Duckdb + Delta/code/delta_tables")
        base_path.mkdir(exist_ok=True)

        con = duckdb.connect()

        # 1. Tabela de Clientes
        print("   Criando tabela 'clientes'...")
        clientes_df = con.execute("""
            SELECT
                i as cliente_id,
                'Cliente ' || i as nome,
                ['BR', 'US', 'UK', 'JP'][i % 4 + 1] as pais,
                CAST('2020-01-01' AS DATE) + (i % 1000) * INTERVAL '1 day' as data_cadastro
            FROM range(1, 101) tbl(i)
        """).df()

        write_deltalake(str(base_path / "clientes"), clientes_df, mode="overwrite")
        print(f"   ✓ Tabela 'clientes' criada com {len(clientes_df)} registros")

        # 2. Tabela de Produtos
        print("   Criando tabela 'produtos'...")
        produtos_df = con.execute("""
            SELECT
                i as produto_id,
                'Produto ' || i as nome_produto,
                ['Eletrônicos', 'Roupas', 'Alimentos', 'Livros'][i % 4 + 1] as categoria,
                10.0 + RANDOM() * 990 as preco
            FROM range(1, 51) tbl(i)
        """).df()

        write_deltalake(str(base_path / "produtos"), produtos_df, mode="overwrite")
        print(f"   ✓ Tabela 'produtos' criada com {len(produtos_df)} registros")

        # 3. Tabela de Vendas (particionada)
        print("   Criando tabela 'vendas' (particionada por data)...")
        vendas_df = con.execute("""
            SELECT
                i as venda_id,
                1 + (i % 100) as cliente_id,
                1 + (i % 50) as produto_id,
                1 + (RANDOM() * 5)::INTEGER as quantidade,
                CAST('2024-01-01' AS DATE) + (i % 90) * INTERVAL '1 day' as data_venda,
                RANDOM() * 1000 as valor
            FROM range(1, 1001) tbl(i)
        """).df()

        # Adicionar coluna de partição
        vendas_df['ano'] = pd.to_datetime(vendas_df['data_venda']).dt.year
        vendas_df['mes'] = pd.to_datetime(vendas_df['data_venda']).dt.month

        write_deltalake(
            str(base_path / "vendas"),
            vendas_df,
            partition_by=["ano", "mes"],
            mode="overwrite"
        )
        print(f"   ✓ Tabela 'vendas' criada com {len(vendas_df)} registros (particionada)")

        con.close()
        return base_path

    except ImportError:
        print("   ✗ Erro: biblioteca 'deltalake' não instalada")
        print("   Execute: pip install deltalake")
        return None

def ler_tabelas_delta(base_path):
    """Ler e consultar tabelas Delta"""
    print("\n2. LENDO TABELAS DELTA COM DUCKDB")
    print("-" * 60)

    if base_path is None:
        print("   ✗ Tabelas Delta não foram criadas")
        return

    con = duckdb.connect()
    con.execute("INSTALL delta")
    con.execute("LOAD delta")

    # Ler clientes
    print("\n   a) Tabela Clientes (primeiros 5 registros):")
    df_clientes = con.execute(f"""
        SELECT * FROM delta_scan('{base_path}/clientes')
        ORDER BY cliente_id
        LIMIT 5
    """).df()
    print(df_clientes.to_string(index=False))

    # Ler produtos
    print("\n   b) Tabela Produtos (primeiros 5 registros):")
    df_produtos = con.execute(f"""
        SELECT produto_id, nome_produto, categoria, ROUND(preco, 2) as preco
        FROM delta_scan('{base_path}/produtos')
        ORDER BY produto_id
        LIMIT 5
    """).df()
    print(df_produtos.to_string(index=False))

    # Ler vendas
    print("\n   c) Tabela Vendas (primeiros 5 registros):")
    df_vendas = con.execute(f"""
        SELECT venda_id, cliente_id, produto_id, quantidade, data_venda, ROUND(valor, 2) as valor
        FROM delta_scan('{base_path}/vendas')
        ORDER BY venda_id
        LIMIT 5
    """).df()
    print(df_vendas.to_string(index=False))

    con.close()

def queries_avancadas_delta(base_path):
    """Demonstrar queries avançadas com Delta tables"""
    print("\n3. QUERIES AVANÇADAS COM DELTA TABLES")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    # JOIN entre tabelas Delta
    print("\n   a) JOIN: Vendas com informações de Cliente e Produto:")
    df_join = con.execute(f"""
        SELECT
            v.venda_id,
            c.nome as cliente,
            c.pais,
            p.nome_produto,
            p.categoria,
            v.quantidade,
            ROUND(v.valor, 2) as valor
        FROM delta_scan('{base_path}/vendas') v
        JOIN delta_scan('{base_path}/clientes') c ON v.cliente_id = c.cliente_id
        JOIN delta_scan('{base_path}/produtos') p ON v.produto_id = p.produto_id
        ORDER BY v.venda_id
        LIMIT 10
    """).df()
    print(df_join.to_string(index=False))

    # Agregação
    print("\n   b) Análise de Vendas por País:")
    df_agg = con.execute(f"""
        SELECT
            c.pais,
            COUNT(*) as total_vendas,
            ROUND(SUM(v.valor), 2) as valor_total,
            ROUND(AVG(v.valor), 2) as valor_medio
        FROM delta_scan('{base_path}/vendas') v
        JOIN delta_scan('{base_path}/clientes') c ON v.cliente_id = c.cliente_id
        GROUP BY c.pais
        ORDER BY valor_total DESC
    """).df()
    print(df_agg.to_string(index=False))

    # Window Functions
    print("\n   c) Top 3 Produtos por Categoria:")
    df_window = con.execute(f"""
        WITH vendas_produto AS (
            SELECT
                p.categoria,
                p.nome_produto,
                SUM(v.valor) as valor_total,
                COUNT(*) as qtd_vendas
            FROM delta_scan('{base_path}/vendas') v
            JOIN delta_scan('{base_path}/produtos') p ON v.produto_id = p.produto_id
            GROUP BY p.categoria, p.nome_produto
        )
        SELECT
            categoria,
            nome_produto,
            ROUND(valor_total, 2) as valor_total,
            qtd_vendas,
            RANK() OVER (PARTITION BY categoria ORDER BY valor_total DESC) as ranking
        FROM vendas_produto
        QUALIFY ranking <= 3
        ORDER BY categoria, ranking
    """).df()
    print(df_window.to_string(index=False))

    con.close()

def criar_views_delta(base_path):
    """Criar views sobre tabelas Delta"""
    print("\n4. CRIANDO VIEWS SOBRE TABELAS DELTA")
    print("-" * 60)

    if base_path is None:
        return

    con = duckdb.connect()
    con.execute("LOAD delta")

    # View de vendas com informações completas
    con.execute(f"""
        CREATE OR REPLACE VIEW vw_vendas_completas AS
        SELECT
            v.venda_id,
            v.data_venda,
            c.nome as cliente,
            c.pais,
            p.nome_produto,
            p.categoria,
            v.quantidade,
            v.valor,
            v.quantidade * v.valor as valor_total
        FROM delta_scan('{base_path}/vendas') v
        JOIN delta_scan('{base_path}/clientes') c ON v.cliente_id = c.cliente_id
        JOIN delta_scan('{base_path}/produtos') p ON v.produto_id = p.produto_id
    """)
    print("   ✓ View 'vw_vendas_completas' criada")

    # Usar a view
    print("\n   Consultando a view (primeiros 5 registros):")
    df_view = con.execute("""
        SELECT
            venda_id,
            data_venda,
            cliente,
            pais,
            nome_produto,
            ROUND(valor_total, 2) as valor_total
        FROM vw_vendas_completas
        ORDER BY valor_total DESC
        LIMIT 5
    """).df()
    print(df_view.to_string(index=False))

    con.close()

def explorar_estrutura_delta(base_path):
    """Explorar estrutura física das tabelas Delta"""
    print("\n5. EXPLORANDO ESTRUTURA DAS TABELAS DELTA")
    print("-" * 60)

    if base_path is None:
        return

    import json

    # Explorar transaction log
    vendas_path = base_path / "vendas" / "_delta_log"

    if vendas_path.exists():
        print(f"\n   Transaction log em: {vendas_path}")
        log_files = list(vendas_path.glob("*.json"))
        print(f"   Arquivos no log: {len(log_files)}")

        if log_files:
            # Ler primeiro arquivo do log
            with open(log_files[0], 'r') as f:
                first_line = f.readline()
                entry = json.loads(first_line)

                if 'metaData' in entry:
                    print(f"\n   Metadata da tabela:")
                    print(f"   - Schema: {entry['metaData'].get('schemaString', 'N/A')[:100]}...")
                elif 'add' in entry:
                    print(f"\n   Primeiro arquivo de dados:")
                    print(f"   - Path: {entry['add']['path']}")
                    print(f"   - Size: {entry['add']['size']} bytes")

def main():
    print("=" * 80)
    print("CAPÍTULO 3: INTRODUÇÃO À EXTENSÃO DELTA")
    print("=" * 80)

    base_path = criar_tabelas_delta_exemplo()

    if base_path:
        ler_tabelas_delta(base_path)
        queries_avancadas_delta(base_path)
        criar_views_delta(base_path)
        explorar_estrutura_delta(base_path)

        print("\n" + "=" * 80)
        print("✓ Demonstração do Capítulo 3 concluída com sucesso!")
        print("=" * 80)
        print(f"\nTabelas Delta criadas em: {base_path}")
    else:
        print("\n" + "=" * 80)
        print("✗ Demonstração não concluída - instale 'deltalake'")
        print("=" * 80)

if __name__ == "__main__":
    main()
