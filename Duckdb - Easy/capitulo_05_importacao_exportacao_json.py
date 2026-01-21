# -*- coding: utf-8 -*-
"""
Capítulo 5: Importação e Exportação de JSON
DuckDB - Easy Course
"""

import duckdb
import os
import sys
import tempfile
import json

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


def criar_json_exemplo():
    """Cria arquivos JSON de exemplo."""
    temp_dir = tempfile.gettempdir()

    # JSON 1: Array de objetos
    todos_json = os.path.join(temp_dir, 'todos.json')
    with open(todos_json, 'w', encoding='utf-8') as f:
        json.dump([
            {"userId": 1, "id": 1, "title": "Tarefa 1", "completed": False},
            {"userId": 1, "id": 2, "title": "Tarefa 2", "completed": True},
            {"userId": 2, "id": 3, "title": "Tarefa 3", "completed": False}
        ], f, indent=2)

    # JSON 2: Dados aninhados
    complex_json = os.path.join(temp_dir, 'complex.json')
    with open(complex_json, 'w', encoding='utf-8') as f:
        json.dump({
            "company": "TechCorp",
            "employees": [
                {"name": "Alice", "department": "Engineering", "skills": ["Python", "SQL"]},
                {"name": "Bob", "department": "Sales", "skills": ["Communication"]}
            ],
            "locations": {"headquarters": "NYC", "offices": ["SF", "London"]}
        }, f, indent=2)

    return temp_dir


def exemplo_01_leitura_basica():
    """Exemplo 1: Leitura básica de JSON."""
    print("=" * 60)
    print("EXEMPLO 1: Leitura Básica de JSON")
    print("=" * 60)

    temp_dir = criar_json_exemplo()
    todos_json = os.path.join(temp_dir, 'todos.json')

    con = duckdb.connect()

    print("\n1.1 - Leitura direta:")
    con.sql(f"SELECT * FROM '{todos_json}'").show()

    print("\n1.2 - Usando read_json:")
    con.sql(f"SELECT * FROM read_json('{todos_json}')").show()

    print("\n1.3 - Python API:")
    df = duckdb.read_json(todos_json).df()
    print(df)

    con.close()


def exemplo_02_json_aninhado():
    """Exemplo 2: Trabalhando com JSON aninhado."""
    print("\n" + "=" * 60)
    print("EXEMPLO 2: JSON Aninhado")
    print("=" * 60)

    con = duckdb.connect()

    # Criar tabela com JSON
    con.sql("""
        CREATE TABLE documentos (
            id INTEGER,
            dados JSON
        )
    """)

    con.sql("""
        INSERT INTO documentos VALUES
        (1, '{"name": "Alice", "age": 30, "city": "NYC"}'),
        (2, '{"name": "Bob", "age": 25, "hobbies": ["reading", "gaming"]}')
    """)

    # Consultar JSON com operador ->
    print("\n2.1 - Extrair valores com -> (retorna JSON):")
    con.sql("SELECT id, dados->'$.name' as name FROM documentos").show()

    # Consultar JSON com operador ->>
    print("\n2.2 - Extrair valores com ->> (retorna VARCHAR):")
    con.sql("SELECT id, dados->>'$.name' as name, dados->>'$.age' as age FROM documentos").show()

    con.close()


def exemplo_03_json_complexo():
    """Exemplo 3: Navegação em estruturas JSON complexas."""
    print("\n" + "=" * 60)
    print("EXEMPLO 3: Estruturas JSON Complexas")
    print("=" * 60)

    temp_dir = criar_json_exemplo()
    complex_json = os.path.join(temp_dir, 'complex.json')

    con = duckdb.connect()

    # Carregar JSON
    con.sql(f"CREATE TABLE company AS SELECT * FROM '{complex_json}'")

    # Acessar campos aninhados
    print("\n3.1 - Acessar campos do JSON:")
    con.sql("SELECT * FROM company").show()

    # JSONPath para arrays
    print("\n3.2 - Criar consulta estruturada:")
    con.sql("""
        CREATE TABLE emp_table (
            id INTEGER,
            data JSON
        )
    """)

    con.sql("""
        INSERT INTO emp_table VALUES
        (1, '{"employees": [
            {"name": "Alice", "dept": "Eng", "skills": ["Python", "SQL"]},
            {"name": "Bob", "dept": "Sales", "skills": ["CRM"]}
        ]}')
    """)

    print("\n3.3 - Acessar elementos de array:")
    con.sql("""
        SELECT
            id,
            data->'$.employees[0].name' as first_employee,
            data->'$.employees[0].skills' as skills
        FROM emp_table
    """).show()

    con.close()


def exemplo_04_exportacao_json():
    """Exemplo 4: Exportação para JSON."""
    print("\n" + "=" * 60)
    print("EXEMPLO 4: Exportação para JSON")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()
    con = duckdb.connect()

    # Criar dados
    con.sql("""
        CREATE TABLE produtos AS
        SELECT
            range as id,
            'Produto_' || range as nome,
            (range * 10.5) as preco
        FROM range(5)
    """)

    # Exportar como JSON
    output_json = os.path.join(temp_dir, 'produtos_output.json')
    print(f"\n4.1 - Exportando para: {output_json}")
    con.sql(f"COPY produtos TO '{output_json}'")

    # Verificar arquivo exportado
    print("\n4.2 - Verificar arquivo exportado:")
    con.sql(f"SELECT * FROM '{output_json}'").show()

    # Exportar como array JSON
    output_array = os.path.join(temp_dir, 'produtos_array.json')
    print(f"\n4.3 - Exportar como array JSON: {output_array}")
    con.sql(f"COPY produtos TO '{output_array}' (FORMAT JSON, ARRAY true)")

    con.close()


def exemplo_05_json_to_parquet():
    """Exemplo 5: Conversão JSON para Parquet."""
    print("\n" + "=" * 60)
    print("EXEMPLO 5: Conversão JSON para Parquet")
    print("=" * 60)

    temp_dir = criar_json_exemplo()
    todos_json = os.path.join(temp_dir, 'todos.json')

    con = duckdb.connect()

    # Converter JSON para Parquet
    output_parquet = os.path.join(temp_dir, 'todos.parquet')
    print(f"\n5.1 - Convertendo JSON para Parquet: {output_parquet}")
    con.sql(f"""
        COPY (SELECT * FROM '{todos_json}')
        TO '{output_parquet}' (FORMAT parquet)
    """)

    # Comparar tamanhos
    json_size = os.path.getsize(todos_json)
    parquet_size = os.path.getsize(output_parquet)

    print(f"\nJSON:    {json_size:,} bytes")
    print(f"Parquet: {parquet_size:,} bytes")
    print(f"Economia: {100 * (1 - parquet_size / json_size):.1f}%")

    con.close()


def exercicio_pratico():
    """Exercício prático completo."""
    print("\n" + "=" * 60)
    print("EXERCÍCIO PRÁTICO: API Response Processing")
    print("=" * 60)

    temp_dir = tempfile.gettempdir()

    # Simular resposta de API
    api_response = os.path.join(temp_dir, 'api_users.json')
    with open(api_response, 'w', encoding='utf-8') as f:
        json.dump({
            "status": "success",
            "data": {
                "users": [
                    {"id": 1, "name": "Alice", "email": "alice@example.com", "active": True},
                    {"id": 2, "name": "Bob", "email": "bob@example.com", "active": False},
                    {"id": 3, "name": "Carlos", "email": "carlos@example.com", "active": True}
                ],
                "total": 3
            }
        }, f, indent=2)

    con = duckdb.connect()

    print("\n1. Carregar resposta da API:")
    con.sql(f"CREATE TABLE api_raw AS SELECT * FROM '{api_response}'")
    con.sql("SELECT * FROM api_raw").show()

    print("\n2. Extrair apenas usuários ativos:")
    con.sql("""
        CREATE TABLE users AS
        SELECT * FROM '{}'
    """.format(api_response))

    con.sql("SELECT * FROM users").show()

    print("\nExercício concluído!")
    con.close()


def main():
    """Função principal."""
    print("\n" + "=" * 60)
    print("CAPÍTULO 5: IMPORTAÇÃO E EXPORTAÇÃO JSON - DUCKDB")
    print("=" * 60)

    try:
        exemplo_01_leitura_basica()
        exemplo_02_json_aninhado()
        exemplo_03_json_complexo()
        exemplo_04_exportacao_json()
        exemplo_05_json_to_parquet()
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
