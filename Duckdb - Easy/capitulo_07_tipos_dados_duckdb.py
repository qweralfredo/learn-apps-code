# -*- coding: utf-8 -*-
"""
capitulo_07_tipos_dados_duckdb
"""

# capitulo_07_tipos_dados_duckdb
import duckdb
import os

# Exemplo/Bloco 1
import duckdb
con = duckdb.connect(database=':memory:')

con.execute("""
CREATE TABLE numeros (
    pequeno TINYINT,
    medio INTEGER,
    grande BIGINT,
    enorme HUGEINT
);

INSERT INTO numeros VALUES (127, 1000000, 9223372036854775807, 123456789012345678901234567890);
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE contadores (
    id UINTEGER,
    visitas UBIGINT
);

INSERT INTO contadores VALUES (1, 18446744073709551615);
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE medicoes (
    temperatura FLOAT,
    precisao DOUBLE
);

INSERT INTO medicoes VALUES (36.5, 3.14159265358979323846);
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- DECIMAL(precisão, escala)
CREATE TABLE financeiro (
    preco DECIMAL(10, 2),  -- 10 dígitos, 2 decimais
    taxa DECIMAL(5, 4)     -- 5 dígitos, 4 decimais
);

INSERT INTO financeiro VALUES (12345.67, 0.0525);
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE textos (
    nome VARCHAR,
    descricao TEXT,
    comentario STRING
);

INSERT INTO textos VALUES
('Alice', 'Desenvolvedora', 'Especialista em Python');
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- VARCHAR com limite de tamanho
CREATE TABLE usuarios (
    username VARCHAR(50),
    email VARCHAR(255)
);
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE configuracoes (
    ativo BOOLEAN,
    verificado BOOL,      -- Alias
    publico LOGICAL       -- Alias
);

INSERT INTO configuracoes VALUES (true, false, true);
INSERT INTO configuracoes VALUES (1, 0, 1);  -- 1 = true, 0 = false
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE eventos (
    nome VARCHAR,
    data DATE
);

INSERT INTO eventos VALUES
('Lançamento', '2024-01-15'),
('Conferência', DATE '2024-06-20');
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE horarios (
    abertura TIME,
    fechamento TIME
);

INSERT INTO horarios VALUES
('09:00:00', '18:00:00'),
(TIME '14:30:00', TIME '23:59:59');
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE logs (
    mensagem VARCHAR,
    timestamp TIMESTAMP
);

INSERT INTO logs VALUES
('Início', '2024-01-15 10:30:00'),
('Fim', TIMESTAMP '2024-01-15 18:45:30.123456');
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE eventos_globais (
    evento VARCHAR,
    quando TIMESTAMP WITH TIME ZONE
);

INSERT INTO eventos_globais VALUES
('Webinar', '2024-01-15 10:00:00-03:00'),  -- Horário de Brasília
('Meeting', '2024-01-15 14:00:00+00:00');  -- UTC
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Intervalo de tempo
SELECT
    '2024-01-15'::DATE + INTERVAL '7 days' as uma_semana_depois,
    '2024-01-15 10:00:00'::TIMESTAMP + INTERVAL '2 hours 30 minutes' as mais_tarde;
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE arquivos (
    nome VARCHAR,
    conteudo BLOB
);

-- Inserir dados binários
INSERT INTO arquivos VALUES
('imagem.png', '\xAB\xCD\xEF'::BLOB);
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE usuarios (
    id UUID,
    nome VARCHAR
);

-- Gerar UUID
INSERT INTO usuarios VALUES
(uuid(), 'Alice'),
(uuid(), 'Bob');

SELECT * FROM usuarios;
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE documentos (
    id INTEGER,
    dados JSON
);

INSERT INTO documentos VALUES
(1, '{"nome": "Alice", "idade": 30}'),
(2, '{"produto": "Laptop", "preco": 1200.50, "specs": {"ram": "16GB"}}');

-- Consultar JSON
SELECT
    id,
    dados->>'$.nome' as nome,
    dados->>'$.idade' as idade
FROM documentos;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Array com tamanho fixo (3 elementos)
CREATE TABLE vetores (
    coordenadas INTEGER[3]
);

INSERT INTO vetores VALUES ([1, 2, 3]);
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Lista com tamanho variável
CREATE TABLE listas (
    numeros INTEGER[]
);

INSERT INTO listas VALUES
([1, 2, 3]),
([1, 2]),           -- Tamanhos diferentes OK
([10, 20, 30, 40, 50]);

-- Acessar elementos (indexação 1-based para LISTs!)
SELECT numeros[1] as primeiro FROM listas;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT
    [1, 2, 3] AS array,
    array_length([1, 2, 3]) as tamanho,
    list_contains([1, 2, 3], 2) as contem,
    list_sum([1, 2, 3]) as soma;
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE pessoas (
    id INTEGER,
    endereco STRUCT(rua VARCHAR, cidade VARCHAR, cep VARCHAR)
);

INSERT INTO pessoas VALUES
(1, {'rua': 'Rua A', 'cidade': 'São Paulo', 'cep': '01000-000'}),
(2, ROW('Rua B', 'Rio de Janeiro', '20000-000'));
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Notação de ponto
SELECT
    id,
    endereco.rua,
    endereco.cidade,
    endereco.cep
FROM pessoas;

-- Ou usando colchetes
SELECT endereco['rua'] FROM pessoas;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT {
    'pessoa': {
        'nome': 'Alice',
        'idade': 30
    },
    'contato': {
        'email': 'alice@example.com',
        'telefone': '11-99999-9999'
    }
} AS dados_complexos;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- MAP de chave → valor
CREATE TABLE configs (
    id INTEGER,
    opcoes MAP(VARCHAR, INTEGER)
);

-- Criar MAP
INSERT INTO configs VALUES
(1, map(['theme', 'font_size', 'line_height'], [1, 14, 20]));

-- Acessar valores
SELECT
    id,
    opcoes['theme'] as tema,
    opcoes['font_size'] as tamanho_fonte
FROM configs;
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE valores_mistos (
    id INTEGER,
    valor UNION(num INTEGER, texto VARCHAR)
);

INSERT INTO valores_mistos VALUES
(1, union_value(num := 42)),
(2, union_value(texto := 'Olá'));
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Extrair valor com base no tipo
SELECT
    id,
    union_extract(valor, 'num') as numero,
    union_extract(valor, 'texto') as texto,
    union_tag(valor) as tipo_atual
FROM valores_mistos;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT {
    'birds': ['duck', 'goose', 'heron'],
    'aliens': NULL,
    'amphibians': ['frog', 'toad']
} AS animais;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT {
    'test': [
        MAP([1, 5], [42.1, 45]),
        MAP([1, 5], [42.1, 45])
    ]
} AS dados_complexos;
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT [
    union_value(num := 2),
    union_value(str := 'ABC')::UNION(str VARCHAR, num INTEGER)
] AS lista_mista;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Usando CAST
SELECT CAST('42' AS INTEGER);
SELECT CAST(3.14 AS INTEGER);  -- 3

-- Usando ::
SELECT '42'::INTEGER;
SELECT '2024-01-15'::DATE;
SELECT 'true'::BOOLEAN;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- TRY_CAST retorna NULL em vez de erro
SELECT TRY_CAST('abc' AS INTEGER);  -- NULL
SELECT TRY_CAST('42' AS INTEGER);   -- 42
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE produtos (
    id UINTEGER PRIMARY KEY,
    nome VARCHAR,
    preco DECIMAL(10, 2),
    estoque INTEGER,
    categorias VARCHAR[],
    detalhes STRUCT(
        peso DECIMAL(5, 2),
        dimensoes STRUCT(
            altura INTEGER,
            largura INTEGER,
            profundidade INTEGER
        )
    ),
    metadados JSON,
    criado_em TIMESTAMP,
    ativo BOOLEAN
);

INSERT INTO produtos VALUES (
    1,
    'Notebook',
    2499.99,
    50,
    ['Eletrônicos', 'Computadores', 'Portáteis'],
    {
        'peso': 1.5,
        'dimensoes': {'altura': 2, 'largura': 35, 'profundidade': 25}
    },
    '{"marca": "TechCo", "garantia": "2 anos"}',
    '2024-01-15 10:00:00',
    true
);
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT
    nome,
    preco,
    array_length(categorias) as num_categorias,
    categorias[1] as categoria_principal,
    detalhes.peso as peso_kg,
    detalhes.dimensoes.altura as altura_cm,
    metadados->>'$.marca' as marca,
    metadados->>'$.garantia' as garantia,
    date_diff('day', criado_em, current_timestamp) as dias_desde_criacao
FROM produtos
WHERE ativo = true;
""")
print(con.fetchall()) # Inspect result

con.execute("""
CREATE TABLE logs (
    timestamp TIMESTAMP,
    nivel VARCHAR,
    mensagem VARCHAR,
    contexto MAP(VARCHAR, VARCHAR),
    stack_trace VARCHAR[]
);

INSERT INTO logs VALUES (
    '2024-01-15 14:30:00',
    'ERROR',
    'Connection timeout',
    map(['server', 'port', 'retry_count'], ['api.example.com', '443', '3']),
    ['app.py:42', 'connection.py:123', 'socket.py:456']
);

-- Consulta
SELECT
    timestamp,
    nivel,
    mensagem,
    contexto['server'] as servidor,
    contexto['port'] as porta,
    stack_trace[1] as primeira_linha_stack
FROM logs
WHERE nivel = 'ERROR';
""")
print(con.fetchall()) # Inspect result

con.execute("""
SELECT
    typeof(42) as tipo_int,
    typeof('texto') as tipo_varchar,
    typeof([1, 2, 3]) as tipo_lista,
    typeof({'a': 1}) as tipo_struct;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- Ver tipos das colunas
DESCRIBE SELECT * FROM produtos;
""")
print(con.fetchall()) # Inspect result

con.execute("""
-- NULL é compatível com todos os tipos
CREATE TABLE exemplo (
    num INTEGER,
    texto VARCHAR,
    lista INTEGER[]
);

INSERT INTO exemplo VALUES (NULL, NULL, NULL);

-- Verificar NULL
SELECT
    num IS NULL as num_nulo,
    COALESCE(num, 0) as num_com_default,
    IFNULL(texto, 'sem texto') as texto_com_default
FROM exemplo;
""")
print(con.fetchall()) # Inspect result


