"""
Capítulo 2: Instalação e Configuração do DuckDB
================================================

Este script demonstra:
- Conexões em memória vs persistentes
- Configurações do DuckDB (threads, memória)
- Thread safety e boas práticas
- Queries incrementais
- Instalação e uso de extensões
"""

import duckdb
import os
import sys
from pathlib import Path

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def exemplo_conexao_memoria():
    """Demonstra conexão em memória (padrão)"""
    print("\n>>> Conexão em Memória")
    print("-" * 50)

    # Conexão padrão (em memória)
    con = duckdb.connect()

    # Query simples
    result = con.sql("SELECT 42 AS x").fetchall()
    print(f"Resultado da query: {result[0][0]}")

    con.close()
    print("✓ Conexão em memória criada e fechada")

def exemplo_conexao_persistente():
    """Demonstra conexão com banco de dados persistente"""
    print("\n>>> Conexão Persistente")
    print("-" * 50)

    db_path = "exemplo.db"

    # Remover arquivo se existir (para demonstração)
    if os.path.exists(db_path):
        os.remove(db_path)

    # Criar banco de dados persistente
    con = duckdb.connect(db_path)

    # Criar tabela e inserir dados
    con.execute("CREATE TABLE test (i INTEGER, nome VARCHAR)")
    con.execute("INSERT INTO test VALUES (42, 'DuckDB'), (99, 'Python')")

    # Consultar dados
    result = con.table("test").fetchall()
    print("Dados salvos:")
    for row in result:
        print(f"  ID: {row[0]}, Nome: {row[1]}")

    con.close()
    print(f"✓ Banco de dados salvo em '{db_path}'")

    # Reconectar para verificar persistência
    print("\n>>> Reconectando ao banco de dados...")
    con = duckdb.connect(db_path)

    result = con.table("test").fetchall()
    print("Dados recuperados após reconexão:")
    for row in result:
        print(f"  ID: {row[0]}, Nome: {row[1]}")

    con.close()
    print("✓ Dados persistiram com sucesso!")

    # Limpar arquivo de teste
    if os.path.exists(db_path):
        os.remove(db_path)

def exemplo_context_manager():
    """Demonstra uso de context manager (forma recomendada)"""
    print("\n>>> Context Manager (Recomendado)")
    print("-" * 50)

    db_path = "exemplo_context.db"

    # Remover arquivo se existir
    if os.path.exists(db_path):
        os.remove(db_path)

    # Usar context manager
    with duckdb.connect(db_path) as con:
        con.execute("CREATE TABLE usuarios (id INTEGER, nome VARCHAR)")
        con.execute("INSERT INTO usuarios VALUES (1, 'Alice'), (2, 'Bob')")
        result = con.table("usuarios").fetchall()

        print("Dados inseridos:")
        for row in result:
            print(f"  ID: {row[0]}, Nome: {row[1]}")

    # Conexão é fechada automaticamente
    print("✓ Conexão fechada automaticamente pelo context manager")

    # Verificar que os dados persistiram
    with duckdb.connect(db_path) as con:
        result = con.execute("SELECT COUNT(*) FROM usuarios").fetchone()
        print(f"✓ Total de usuários após reconexão: {result[0]}")

    # Limpar
    if os.path.exists(db_path):
        os.remove(db_path)

def exemplo_configuracoes():
    """Demonstra diferentes configurações do DuckDB"""
    print("\n>>> Configurações do DuckDB")
    print("-" * 50)

    # Configurando threads
    print("\n1. Configurando número de threads:")
    con = duckdb.connect(config={'threads': 1})
    result = con.execute("SELECT current_setting('threads')").fetchone()
    print(f"   Threads configuradas: {result[0]}")
    con.close()

    # Múltiplas configurações
    print("\n2. Múltiplas configurações:")
    con = duckdb.connect(config={
        'threads': 4,
        'max_memory': '1GB',
        'default_order': 'DESC'
    })

    result_threads = con.execute("SELECT current_setting('threads')").fetchone()
    result_memory = con.execute("SELECT current_setting('max_memory')").fetchone()

    print(f"   Threads: {result_threads[0]}")
    print(f"   Max Memory: {result_memory[0]}")

    con.close()
    print("✓ Configurações aplicadas com sucesso")

def exemplo_queries_incrementais():
    """Demonstra queries incrementais (referenciar resultados anteriores)"""
    print("\n>>> Queries Incrementais")
    print("-" * 50)

    # Query incremental
    r1 = duckdb.sql("SELECT 42 AS i")
    print(f"Resultado da primeira query (r1): {r1.fetchall()}")

    # Referenciar resultado anterior
    r2 = duckdb.sql("SELECT i * 2 AS k FROM r1")
    print(f"Resultado da segunda query (r1 * 2): {r2.fetchall()}")

    # Outro exemplo mais complexo
    r3 = duckdb.sql("""
        SELECT unnest(range(1, 6)) AS num
    """)
    print("\nNúmeros de 1 a 5:")
    print(r3.fetchall())

    r4 = duckdb.sql("SELECT num, num * num AS quadrado FROM r3")
    print("\nNúmeros e seus quadrados:")
    for row in r4.fetchall():
        print(f"  {row[0]}² = {row[1]}")

    print("✓ Queries incrementais executadas com sucesso")

def exemplo_comandos_sessao():
    """Demonstra configurações de sessão em tempo de execução"""
    print("\n>>> Configurações de Sessão")
    print("-" * 50)

    con = duckdb.connect()

    # Configurar threads em tempo de execução
    con.execute("SET threads = 2")
    result = con.execute("SELECT current_setting('threads')").fetchone()
    print(f"Threads alteradas para: {result[0]}")

    # Outras configurações
    con.execute("SET memory_limit = '500MB'")
    result = con.execute("SELECT current_setting('memory_limit')").fetchone()
    print(f"Memory limit alterado para: {result[0]}")

    con.close()
    print("✓ Configurações de sessão aplicadas")

def exemplo_thread_safety():
    """Demonstra boas práticas de thread safety"""
    print("\n>>> Thread Safety - Boas Práticas")
    print("-" * 50)

    print("\n✅ BOA PRÁTICA: Cada operação usa sua própria conexão")

    def boa_pratica():
        con = duckdb.connect()
        result = con.execute("SELECT 1 AS valor").fetchone()
        con.close()
        return result[0]

    resultado = boa_pratica()
    print(f"   Resultado: {resultado}")

    print("\n❌ MÁ PRÁTICA: Evite compartilhar conexões entre threads")
    print("   (Veja os comentários no código para entender o problema)")

    # Não demonstramos aqui pois causaria problemas em ambientes multi-thread
    print("✓ Sempre crie conexões locais em cada thread/função")

def exemplo_extensoes():
    """Demonstra instalação e uso de extensões"""
    print("\n>>> Extensões do DuckDB")
    print("-" * 50)

    con = duckdb.connect()

    print("\n1. Verificando extensões disponíveis:")
    try:
        # Listar extensões instaladas
        result = con.execute("""
            SELECT extension_name, loaded, installed
            FROM duckdb_extensions()
            WHERE installed = true
            LIMIT 5
        """).fetchall()

        if result:
            print("   Extensões instaladas:")
            for row in result:
                status = "carregada" if row[1] else "instalada"
                print(f"   - {row[0]} ({status})")
        else:
            print("   Nenhuma extensão instalada ainda")
    except Exception as e:
        print(f"   Nenhuma extensão disponível no momento")

    print("\n2. Exemplo de uso de extensão JSON (geralmente já incluída):")
    try:
        # JSON é uma extensão core, normalmente já disponível
        con.execute("""
            CREATE TABLE json_test AS
            SELECT '{"nome": "Alice", "idade": 30}'::JSON AS dados
        """)

        result = con.execute("""
            SELECT json_extract(dados, '$.nome') AS nome
            FROM json_test
        """).fetchone()

        print(f"   Nome extraído do JSON: {result[0]}")
        print("   ✓ Extensão JSON funcionando")
    except Exception as e:
        print(f"   Nota: Funcionalidade JSON pode variar por versão")

    con.close()
    print("\n✓ Exemplo de extensões concluído")

def exemplo_verificacao_instalacao():
    """Verifica se o DuckDB está instalado corretamente"""
    print("\n>>> Verificação da Instalação")
    print("-" * 50)

    # Verificar versão
    con = duckdb.connect()
    result = con.execute("SELECT version()").fetchone()
    print(f"Versão do DuckDB: {result[0]}")

    # Query simples de teste
    result = con.execute("SELECT 42 AS resposta").fetchone()
    print(f"Query de teste: {result[0]}")

    if result[0] == 42:
        print("✓ DuckDB instalado e funcionando corretamente!")
    else:
        print("✗ Problema na instalação")

    con.close()

def exercicios_praticos():
    """Executa os exercícios práticos do capítulo"""
    print("\n" + "=" * 60)
    print("EXERCÍCIOS PRÁTICOS")
    print("=" * 60)

    # Exercício 1: Instalar DuckDB (já feito via pip)
    print("\n>>> Exercício 1: Verificar instalação")
    con = duckdb.connect()
    result = con.execute("SELECT 'DuckDB instalado!' AS status").fetchone()
    print(f"   {result[0]}")
    con.close()

    # Exercício 2: Criar banco persistente
    print("\n>>> Exercício 2: Criar banco 'exercicio.db'")
    db_path = "exercicio.db"

    if os.path.exists(db_path):
        os.remove(db_path)

    con = duckdb.connect(db_path)
    print(f"   ✓ Banco '{db_path}' criado")

    # Exercício 3: Criar tabela e inserir dados
    print("\n>>> Exercício 3: Criar tabela 'alunos' e inserir dados")
    con.execute("""
        CREATE TABLE alunos (
            id INTEGER,
            nome VARCHAR,
            nota DECIMAL(4, 2)
        )
    """)

    alunos = [
        (1, 'Ana Silva', 9.5),
        (2, 'Bruno Costa', 8.7),
        (3, 'Carla Santos', 9.2)
    ]

    for aluno in alunos:
        con.execute("INSERT INTO alunos VALUES (?, ?, ?)", aluno)

    result = con.table("alunos").fetchall()
    print("   Dados inseridos:")
    for row in result:
        print(f"   - ID {row[0]}: {row[1]} - Nota: {row[2]}")

    con.close()

    # Exercício 4: Reconectar e verificar persistência
    print("\n>>> Exercício 4: Reconectar e verificar persistência")
    con = duckdb.connect(db_path)

    result = con.execute("SELECT COUNT(*) FROM alunos").fetchone()
    print(f"   Total de alunos recuperados: {result[0]}")

    result = con.execute("SELECT nome, nota FROM alunos ORDER BY nota DESC").fetchall()
    print("   Alunos ordenados por nota:")
    for row in result:
        print(f"   - {row[0]}: {row[1]}")

    # Exercício 5: Configurar threads e executar query
    print("\n>>> Exercício 5: Configurar 2 threads e executar query")
    con.execute("SET threads = 2")

    result = con.execute("""
        SELECT nome, nota * 10 AS nota_percentual
        FROM alunos
        WHERE nota > 9.0
    """).fetchall()

    print("   Alunos com nota > 9.0:")
    for row in result:
        print(f"   - {row[0]}: {row[1]}%")

    con.close()

    # Limpar arquivo de exercício
    if os.path.exists(db_path):
        os.remove(db_path)

    print("\n✓ Todos os exercícios concluídos!")

def main():
    print("=" * 60)
    print("Capítulo 2: Instalação e Configuração do DuckDB")
    print("=" * 60)

    # Executar exemplos
    exemplo_verificacao_instalacao()
    exemplo_conexao_memoria()
    exemplo_conexao_persistente()
    exemplo_context_manager()
    exemplo_configuracoes()
    exemplo_queries_incrementais()
    exemplo_comandos_sessao()
    exemplo_thread_safety()
    exemplo_extensoes()

    # Executar exercícios
    exercicios_praticos()

    print("\n" + "=" * 60)
    print("✓ Script concluído com sucesso!")
    print("=" * 60)

if __name__ == "__main__":
    main()
