# -*- coding: utf-8 -*-
"""
capitulo_01_introducao_sql_duckdb
"""

# capitulo_01_introducao_sql_duckdb
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
con = duckdb.connect(database=':memory:')

con.execute("""
CREATE TABLE weather (
    city VARCHAR,
    temp_lo INTEGER,      -- temperatura mínima
    temp_hi INTEGER,      -- temperatura máxima
    prcp FLOAT,
    date DATE
);

CREATE TABLE cities (
    name VARCHAR,
    lat DECIMAL,
    lon DECIMAL
);
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Inserção básica
INSERT INTO weather VALUES ('San Francisco', 46, 50, 0.25, '1994-11-27');

-- Inserção com colunas explícitas
INSERT INTO weather (city, temp_lo, temp_hi, prcp, date)
VALUES ('San Francisco', 43, 57, 0.0, '1994-11-29');

-- Inserção omitindo colunas (valores serão NULL)
INSERT INTO weather (date, city, temp_hi, temp_lo)
VALUES ('1994-11-29', 'Hayward', 54, 37);
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Selecionar todas as colunas
SELECT * FROM weather;

-- Selecionar colunas específicas
SELECT city, temp_lo, temp_hi, prcp, date FROM weather;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Calcular temperatura média
SELECT city, (temp_hi + temp_lo) / 2 AS temp_avg, date
FROM weather;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Filtro simples
SELECT * FROM weather
WHERE city = 'San Francisco' AND prcp > 0.0;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Ordenar por uma coluna
SELECT * FROM weather ORDER BY city;

-- Ordenar por múltiplas colunas
SELECT * FROM weather ORDER BY city, temp_lo;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Obter cidades únicas
SELECT DISTINCT city FROM weather;

-- DISTINCT com ordenação
SELECT DISTINCT city FROM weather ORDER BY city;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Junção implícita
SELECT * FROM weather, cities
WHERE city = name;

-- Especificando colunas
SELECT city, temp_lo, temp_hi, prcp, date, lon, lat
FROM weather, cities
WHERE city = name;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT * FROM weather
INNER JOIN cities ON weather.city = cities.name;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT * FROM weather
LEFT OUTER JOIN cities ON weather.city = cities.name;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Encontrar o valor máximo
SELECT max(temp_lo) FROM weather;

-- Encontrar a cidade com a temperatura mínima mais alta
SELECT city FROM weather
WHERE temp_lo = (SELECT max(temp_lo) FROM weather);
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Agrupar por cidade
SELECT city, max(temp_lo) FROM weather GROUP BY city;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Filtrar grupos
SELECT city, max(temp_lo) FROM weather
GROUP BY city
HAVING max(temp_lo) < 40;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Cidades que começam com 'S'
SELECT city, max(temp_lo) FROM weather
WHERE city LIKE 'S%'
GROUP BY city
HAVING max(temp_lo) < 40;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Atualizar múltiplas colunas
UPDATE weather
SET temp_hi = temp_hi - 2, temp_lo = temp_lo - 2
WHERE date > '1994-11-28';
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Deletar linhas específicas
DELETE FROM weather WHERE city = 'Hayward';
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- CUIDADO: Isso remove TODAS as linhas!
DELETE FROM weather;
""")
print(con.fetchall()) # Inspect result


