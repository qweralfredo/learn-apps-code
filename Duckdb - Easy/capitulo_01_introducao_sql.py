"""
Capítulo 1: Introdução ao SQL no DuckDB
========================================

Este script demonstra os conceitos básicos de SQL usando DuckDB:
- Criação de tabelas
- Inserção de dados
- Comandos SELECT, WHERE, ORDER BY, DISTINCT
- JOINs (INNER, LEFT OUTER)
- Funções de agregação (GROUP BY, HAVING)
- UPDATE e DELETE
"""

import duckdb
import sys

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def main():
    print("=" * 60)
    print("Capítulo 1: Introdução ao SQL no DuckDB")
    print("=" * 60)

    # Criar conexão em memória
    con = duckdb.connect()

    # =================================================================
    # 1.1 Criando Tabelas e Inserindo Dados
    # =================================================================
    print("\n>>> 1.1 Criando Tabelas")

    # Criar tabela weather
    con.execute("""
        CREATE TABLE weather (
            city VARCHAR,
            temp_lo INTEGER,      -- temperatura mínima
            temp_hi INTEGER,      -- temperatura máxima
            prcp FLOAT,           -- precipitação
            date DATE
        )
    """)

    # Criar tabela cities
    con.execute("""
        CREATE TABLE cities (
            name VARCHAR,
            lat DECIMAL,
            lon DECIMAL
        )
    """)

    print("✓ Tabelas 'weather' e 'cities' criadas com sucesso!")

    # Inserir dados na tabela weather
    print("\n>>> Inserindo dados na tabela 'weather'")

    con.execute("INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27')")
    con.execute("INSERT INTO weather (city, temp_lo, temp_hi, prcp, date) VALUES ('San Francisco', 43, 57, 0.0, '1994-11-29')")
    con.execute("INSERT INTO weather (date, city, temp_hi, temp_lo) VALUES ('1994-11-29', 'Hayward', 54, 37)")
    con.execute("INSERT INTO weather VALUES ('Los Angeles', 50, 75, 0.0, '1994-11-28')")
    con.execute("INSERT INTO weather VALUES ('Seattle', 35, 45, 0.5, '1994-11-27')")

    print("✓ 5 registros inseridos na tabela 'weather'")

    # Inserir dados na tabela cities
    print("\n>>> Inserindo dados na tabela 'cities'")

    con.execute("INSERT INTO cities VALUES ('San Francisco', 37.7749, -122.4194)")
    con.execute("INSERT INTO cities VALUES ('Hayward', 37.6688, -122.0808)")
    con.execute("INSERT INTO cities VALUES ('Los Angeles', 34.0522, -118.2437)")

    print("✓ 3 cidades inseridas na tabela 'cities'")

    # =================================================================
    # 1.2 SELECT Simples
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.2 SELECT Simples")
    print("=" * 60)

    print("\nTodas as colunas da tabela weather:")
    result = con.execute("SELECT * FROM weather").fetchall()
    for row in result:
        print(row)

    print("\nColunas específicas:")
    result = con.execute("SELECT city, temp_lo, temp_hi, date FROM weather").fetchall()
    for row in result:
        print(row)

    # =================================================================
    # 1.3 Expressões e Aliases
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.3 Expressões e Aliases")
    print("=" * 60)

    print("\nCalculando temperatura média:")
    result = con.execute("""
        SELECT city, (temp_hi + temp_lo) / 2 AS temp_avg, date
        FROM weather
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}°F em {row[2]}")

    # =================================================================
    # 1.4 Comando WHERE
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.4 Comando WHERE")
    print("=" * 60)

    print("\nFiltrando por cidade e precipitação:")
    result = con.execute("""
        SELECT * FROM weather
        WHERE city = 'San Francisco' AND prcp > 0.0
    """).fetchall()
    for row in result:
        print(row)

    # =================================================================
    # 1.5 ORDER BY
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.5 ORDER BY")
    print("=" * 60)

    print("\nOrdenando por cidade:")
    result = con.execute("SELECT * FROM weather ORDER BY city").fetchall()
    for row in result:
        print(row)

    print("\nOrdenando por cidade e temperatura mínima:")
    result = con.execute("SELECT * FROM weather ORDER BY city, temp_lo").fetchall()
    for row in result:
        print(row)

    # =================================================================
    # 1.6 DISTINCT
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.6 DISTINCT")
    print("=" * 60)

    print("\nCidades únicas:")
    result = con.execute("SELECT DISTINCT city FROM weather ORDER BY city").fetchall()
    for row in result:
        print(row[0])

    # =================================================================
    # 1.7 JOINS
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.7 JOINS")
    print("=" * 60)

    # Inner Join (sintaxe tradicional)
    print("\nINNER JOIN (sintaxe tradicional):")
    result = con.execute("""
        SELECT weather.city, temp_lo, temp_hi, prcp, date, lon, lat
        FROM weather, cities
        WHERE city = name
    """).fetchall()
    for row in result:
        print(row)

    # Inner Join (sintaxe explícita)
    print("\nINNER JOIN (sintaxe explícita):")
    result = con.execute("""
        SELECT weather.city, temp_lo, temp_hi, lat, lon
        FROM weather
        INNER JOIN cities ON weather.city = cities.name
    """).fetchall()
    for row in result:
        print(row)

    # Left Outer Join
    print("\nLEFT OUTER JOIN (mostra todas as cidades, mesmo sem coordenadas):")
    result = con.execute("""
        SELECT weather.city, temp_lo, temp_hi, lat, lon
        FROM weather
        LEFT OUTER JOIN cities ON weather.city = cities.name
    """).fetchall()
    for row in result:
        print(row)

    # =================================================================
    # 1.8 Funções de Agregação
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.8 Funções de Agregação")
    print("=" * 60)

    print("\nTemperatura mínima mais alta:")
    result = con.execute("SELECT max(temp_lo) FROM weather").fetchone()
    print(f"Max temp_lo: {result[0]}°F")

    print("\nCidade com a temperatura mínima mais alta:")
    result = con.execute("""
        SELECT city FROM weather
        WHERE temp_lo = (SELECT max(temp_lo) FROM weather)
    """).fetchall()
    for row in result:
        print(row[0])

    # =================================================================
    # 1.9 GROUP BY
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.9 GROUP BY")
    print("=" * 60)

    print("\nTemperatura mínima máxima por cidade:")
    result = con.execute("""
        SELECT city, max(temp_lo) as max_temp_lo
        FROM weather
        GROUP BY city
        ORDER BY city
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}°F")

    # =================================================================
    # 1.10 HAVING
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.10 HAVING")
    print("=" * 60)

    print("\nCidades com temperatura mínima máxima < 45:")
    result = con.execute("""
        SELECT city, max(temp_lo) as max_temp_lo
        FROM weather
        GROUP BY city
        HAVING max(temp_lo) < 45
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}°F")

    # =================================================================
    # 1.11 LIKE com GROUP BY
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.11 LIKE com GROUP BY")
    print("=" * 60)

    print("\nCidades que começam com 'S' e temp_lo max < 50:")
    result = con.execute("""
        SELECT city, max(temp_lo) as max_temp_lo
        FROM weather
        WHERE city LIKE 'S%'
        GROUP BY city
        HAVING max(temp_lo) < 50
    """).fetchall()
    for row in result:
        print(f"{row[0]}: {row[1]}°F")

    # =================================================================
    # 1.12 UPDATE
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.12 UPDATE")
    print("=" * 60)

    print("\nAntes do UPDATE:")
    result = con.execute("SELECT city, temp_hi, temp_lo, date FROM weather WHERE date > '1994-11-28'").fetchall()
    for row in result:
        print(row)

    con.execute("""
        UPDATE weather
        SET temp_hi = temp_hi - 2, temp_lo = temp_lo - 2
        WHERE date > '1994-11-28'
    """)

    print("\nDepois do UPDATE (reduzindo 2°F):")
    result = con.execute("SELECT city, temp_hi, temp_lo, date FROM weather WHERE date > '1994-11-28'").fetchall()
    for row in result:
        print(row)

    # =================================================================
    # 1.13 DELETE
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> 1.13 DELETE")
    print("=" * 60)

    print("\nRegistros antes do DELETE:")
    result = con.execute("SELECT COUNT(*) FROM weather").fetchone()
    print(f"Total de registros: {result[0]}")

    con.execute("DELETE FROM weather WHERE city = 'Hayward'")

    print("\nRegistros depois do DELETE (removido Hayward):")
    result = con.execute("SELECT COUNT(*) FROM weather").fetchone()
    print(f"Total de registros: {result[0]}")

    print("\nRegistros restantes:")
    result = con.execute("SELECT city FROM weather ORDER BY city").fetchall()
    for row in result:
        print(row[0])

    # =================================================================
    # EXERCÍCIOS PRÁTICOS
    # =================================================================
    print("\n" + "=" * 60)
    print(">>> EXERCÍCIOS PRÁTICOS")
    print("=" * 60)

    print("\n>>> Exercício 1: Criar tabela 'produtos'")
    con.execute("""
        CREATE TABLE produtos (
            nome VARCHAR,
            categoria VARCHAR,
            preco DECIMAL(10, 2),
            estoque INTEGER
        )
    """)
    print("✓ Tabela 'produtos' criada!")

    print("\n>>> Exercício 2: Inserir 5 produtos")
    produtos_data = [
        ('Notebook Dell', 'Eletrônicos', 3500.00, 10),
        ('Mouse Logitech', 'Eletrônicos', 89.90, 50),
        ('Cadeira Gamer', 'Móveis', 1200.00, 15),
        ('Mesa Escritório', 'Móveis', 800.00, 8),
        ('Teclado Mecânico', 'Eletrônicos', 450.00, 25)
    ]

    for produto in produtos_data:
        con.execute("INSERT INTO produtos VALUES (?, ?, ?, ?)", produto)

    print("✓ 5 produtos inseridos!")
    result = con.execute("SELECT * FROM produtos").fetchall()
    for row in result:
        print(row)

    print("\n>>> Exercício 3: Produto mais caro")
    result = con.execute("""
        SELECT nome, preco
        FROM produtos
        WHERE preco = (SELECT MAX(preco) FROM produtos)
    """).fetchone()
    print(f"Produto mais caro: {result[0]} - R$ {result[1]}")

    print("\n>>> Exercício 4: Preço médio por categoria")
    result = con.execute("""
        SELECT categoria, AVG(preco) as preco_medio
        FROM produtos
        GROUP BY categoria
        ORDER BY categoria
    """).fetchall()
    for row in result:
        print(f"{row[0]}: R$ {row[1]:.2f}")

    print("\n>>> Exercício 5: Categorias com preço médio > 50")
    result = con.execute("""
        SELECT categoria, AVG(preco) as preco_medio
        FROM produtos
        GROUP BY categoria
        HAVING AVG(preco) > 50
        ORDER BY preco_medio DESC
    """).fetchall()
    print("Categorias com preço médio acima de R$ 50:")
    for row in result:
        print(f"{row[0]}: R$ {row[1]:.2f}")

    # Fechar conexão
    con.close()

    print("\n" + "=" * 60)
    print("✓ Script concluído com sucesso!")
    print("=" * 60)

if __name__ == "__main__":
    main()
