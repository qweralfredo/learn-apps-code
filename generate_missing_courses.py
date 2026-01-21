import os
import json
import pathlib

# --- Templates ---

def create_duckdb_content(chapter_num, chapter_title, specific_topics):
    py_content = f'''# -*- coding: utf-8 -*-
"""
Capítulo {chapter_num}: {chapter_title}
"""

import duckdb
import pandas as pd
import pathlib

# ==============================================================================
# SETUP E DADOS DE EXEMPLO
# ==============================================================================
print(f"--- Iniciando Capítulo {chapter_num}: {chapter_title} ---")

# Conexão em memória para testes
con = duckdb.connect(database=':memory:')

# Criação de dados mock para exemplos
con.execute("""
    CREATE TABLE IF NOT EXISTS vendas (
        id INTEGER,
        data DATE,
        produto VARCHAR,
        categoria VARCHAR,
        valor DECIMAL(10,2),
        quantidade INTEGER
    );
    
    INSERT INTO vendas VALUES
    (1, '2023-01-01', 'Notebook', 'Eletronicos', 3500.00, 2),
    (2, '2023-01-02', 'Mouse', 'Perifericos', 50.00, 10),
    (3, '2023-01-03', 'Teclado', 'Perifericos', 120.00, 5),
    (4, '2023-01-04', 'Monitor', 'Eletronicos', 1200.00, 3);
""")
print("Dados de exemplo 'vendas' criados com sucesso.")

# ==============================================================================
# CONTEÚDO DO CAPÍTULO
# ==============================================================================
'''
    for topic in specific_topics:
        py_content += f'''
# -----------------------------------------------------------------------------
# Tópico: {topic}
# -----------------------------------------------------------------------------
print(f"\\n>>> Executando: {topic}")

# TODO: Implementar exemplos práticos para {topic}
# Exemplo genérico:
# result = con.sql("SELECT * FROM vendas LIMIT 1").show()
'''
    
    py_content += '\nprint("\\n--- Capítulo concluído com sucesso ---")\n'
    return py_content

def create_security_content(chapter_num, chapter_title, specific_topics):
    py_content = f'''# -*- coding: utf-8 -*-
"""
Capítulo {chapter_num}: {chapter_title}
⚠️ AVISO: Códigos para fins educacionais. NÃO use exemplos vulneráveis em produção.
"""

import hashlib
import sqlite3

# ==============================================================================
# SETUP
# ==============================================================================
print(f"--- Iniciando Capítulo {chapter_num}: {chapter_title} ---")

# Mock de banco de dados para exemplos
def get_db():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, password TEXT)")
    cursor.execute("INSERT INTO users (username, password) VALUES ('admin', '12345')")
    cursor.execute("INSERT INTO users (username, password) VALUES ('user', 'abcde')")
    conn.commit()
    return conn

# ==============================================================================
# CONTEÚDO DO CAPÍTULO
# ==============================================================================
'''
    for topic in specific_topics:
        py_content += f'''
# -----------------------------------------------------------------------------
# Tópico: {topic}
# -----------------------------------------------------------------------------
print(f"\\n>>> Explorando: {topic}")

def exemplo_vulneravel_{topic.lower().replace(' ', '_').replace('-', '_')}():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_{topic.lower().replace(' ', '_').replace('-', '_')}():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_{topic.lower().replace(' ', '_').replace('-', '_')}()
# exemplo_seguro_{topic.lower().replace(' ', '_').replace('-', '_')}()
'''
    
    py_content += '\nprint("\\n--- Capítulo concluído com sucesso ---")\n'
    return py_content

def create_notebook_from_py(chapter_num, chapter_title, py_content):
    # Create simple structure
    cells = []
    
    # Title Cell
    cells.append({
        "cell_type": "markdown",
        "metadata": {},
        "source": [f"# Capítulo {chapter_num}: {chapter_title}\n", "\n", "Este notebook acompanha os exemplos práticos do script Python."]
    })
    
    # Split content
    code_blocks = py_content.split('# -----------------------------------------------------------------------------')
    
    # Main/Setup Block
    if len(code_blocks) > 0:
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": code_blocks[0].strip().split('\n')
        })
        
    for block in code_blocks[1:]:
        lines = block.strip().split('\n')
        if not lines: continue
        
        topic_title = "Exemplo"
        for line in lines:
            if line.startswith("# Tópico:"):
                topic_title = line.replace("# Tópico:", "").strip()
                break
        
        cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"## {topic_title}"]
        })
        
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": ["# ----------------------------------------\n"] + [l + "\n" for l in lines]
        })

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {"name": "ipython", "version": 3},
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

# --- Definitions ---

courses = [
    {
        "folder": "Duckdb - Easy",
        "template": "duckdb",
        "chapters": [
            ("01", "Introdução ao SQL no DuckDB", ["Conceitos Básicos", "SELECT Simples", "Filtragem"]),
            ("02", "Instalação e Configuração", ["Instalação Pip", "Conexão Memória", "Conexão Persistente"]),
            ("03", "Importação Exportação CSV", ["Read CSV", "Read CSV Auto", "Copy To CSV"]),
            ("04", "Trabalhando com Parquet", ["Read Parquet", "Performance Parquet", "Partitioned Writes"]),
            ("05", "Importação e Exportação JSON", ["Read JSON", "JSON Auto", "Diferenças NDJSON"]),
            ("06", "Integração com Python", ["DuckDB Relational API", "Pandas Dataframes", "Result Conversion"]),
            ("07", "Tipos de Dados DuckDB", ["Numeric Types", "Text Types", "Date/Time", "Nested Types"]),
            ("08", "Consultas em Arquivos Remotos", ["HTTPFS", "Reading from URL", "S3 Basics"]),
            ("09", "Meta Queries", ["Describe Table", "Show Tables", "Explain Analyze"]),
            ("10", "Performance e Boas Práticas", ["Index", "Pragma", "Memory Limit"])
        ]
    },
    {
        "folder": "Duckdb + S3",
        "template": "duckdb",
        "chapters": [
            ("01", "Introdução DuckDB e S3", ["Visão Geral S3", "Conceitos de Object Storage"]),
            ("02", "Instalação HTTPFS Extension", ["Install Extension", "Load Extension", "Configuração Básica"]),
            ("03", "Configuração Credenciais AWS", ["S3 Region", "Access Key", "Secret Key", "Session Token"]),
            ("04", "Leitura de Dados S3", ["Read Parquet S3", "Read CSV S3", "Glob Patterns"]),
            ("05", "Escrita de Dados S3", ["Write Parquet S3", "Partitioned Write S3"]),
            ("06", "Gerenciamento de Secrets", ["Create Secret", "Persistent Secrets", "Drop Secret"]),
            ("07", "Trabalhando com Parquet no S3", ["Hive Partitioning", "Column Projection"]),
            ("08", "Padrões Avançados e Globbing", ["Filename Expansion", "Recursive Search"]),
            ("09", "Integração com Cloud Services", ["Lambda Integration Mock", "Glue Mock"]),
            ("10", "Otimização e Boas Práticas", ["Parallel Reading", "Prefetching", "Network Config"])
        ]
    },
    {
        "folder": "Cyber Security - Ana",
        "template": "security",
        "chapters": [
             ("01", "Introdução à Segurança Web", ["OWASP Top 10", "Conceitos CIA", "HTTP Headers"]),
             ("03", "Cross-Site Scripting (XSS)", ["Stored XSS", "Reflected XSS", "DOM XSS"]),
             ("04", "Cross-Site Request Forgery (CSRF)", ["Tokens CSRF", "SameSite Cookies"]),
             ("05", "Autenticação e Autorização", ["Password Hashing", "Session Management", "MFA"]),
             ("06", "Controle de Acesso", ["IDOR", "Vertical Escalation", "Horizontal Escalation"]),
             ("07", "Segurança de APIs", ["JWT Issues", "Rate Limiting", "Input Validation"]),
             ("08", "Criptografia e Proteção de Dados", ["Symmetric Enc", "Asymmetric Enc", "Hashing"]),
             ("09", "Configurações Seguras", ["Security Headers", "Cookie flags", "Error Handling"]),
             ("10", "Checklist e Próximos Passos", ["Code Review", "Pentest Basics", "Security Automation"])
        ]
    },
    {
        "folder": "Duckdb + Delta",
        "template": "duckdb",
        "chapters": [
             ("01", "Introdução Delta Lake", ["Conceitos Delta", "DuckDB Delta Extension"]),
        ]
    }
]

# --- Main Logic ---

root_path = pathlib.Path(r"c:\projetos\learn-apps-code")

for course in courses:
    folder = root_path / course["folder"]
    folder.mkdir(exist_ok=True)
    
    print(f"\nProcessing: {course['folder']}")
    
    for num, title, topics in course["chapters"]:
        safe_title = title.lower().replace(" ", "_").replace("/", "_").replace("-", "_")
        # Handle special chars like 'ç', 'ã' roughly if needed, but windows handles utf8 filenames okay usually.
        # Let's do a simple mapping to be safe
        table = str.maketrans("áàãâéêíóôõúçÁÀÃÂÉÊÍÓÔÕÚÇ", "aaaaeeioooucAAAAEEIOOOUC")
        safe_title = safe_title.translate(table)
        
        base_name = f"capitulo_{num}_{safe_title}"
        py_filename = folder / f"{base_name}.py"
        nb_filename = folder / f"{base_name}.ipynb"
        
        # Override for 'Importação e Exportação JSON' since user file might expect underscores
        if "json" in safe_title:
             pass # simple logic is enough
        
        # Create Content (RESTAURADO!)
        if course["template"] == "duckdb":
            py_code = create_duckdb_content(num, title, topics)
        else:
            py_code = create_security_content(num, title, topics)
            
        nb_obj = create_notebook_from_py(num, title, py_code)
        
        # Write Files
        print(f"  Creating {py_filename.name}")
        with open(py_filename, 'w', encoding='utf-8') as f:
            f.write(py_code)
            
        print(f"  Creating {nb_filename.name}")
        with open(nb_filename, 'w', encoding='utf-8') as f:
            json.dump(nb_obj, f, ensure_ascii=False, indent=2)

print("\nAll courses processed.")
