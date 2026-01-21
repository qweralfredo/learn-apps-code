# -*- coding: utf-8 -*-
"""
Capítulo 01: Introdução à Segurança Web
⚠️ AVISO: Códigos para fins educacionais. NÃO use exemplos vulneráveis em produção.
"""

import hashlib
import sqlite3

# ==============================================================================
# SETUP
# ==============================================================================
print(f"--- Iniciando Capítulo 01: Introdução à Segurança Web ---")

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

# -----------------------------------------------------------------------------
# Tópico: OWASP Top 10
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: OWASP Top 10")

def exemplo_vulneravel_owasp_top_10():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_owasp_top_10():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_owasp_top_10()
# exemplo_seguro_owasp_top_10()

# -----------------------------------------------------------------------------
# Tópico: Conceitos CIA
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Conceitos CIA")

def exemplo_vulneravel_conceitos_cia():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_conceitos_cia():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_conceitos_cia()
# exemplo_seguro_conceitos_cia()

# -----------------------------------------------------------------------------
# Tópico: HTTP Headers
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: HTTP Headers")

def exemplo_vulneravel_http_headers():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_http_headers():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_http_headers()
# exemplo_seguro_http_headers()

print("\n--- Capítulo concluído com sucesso ---")
