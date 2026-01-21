# -*- coding: utf-8 -*-
"""
Capítulo 04: Cross-Site Request Forgery (CSRF)
⚠️ AVISO: Códigos para fins educacionais. NÃO use exemplos vulneráveis em produção.
"""

import hashlib
import sqlite3

# ==============================================================================
# SETUP
# ==============================================================================
print(f"--- Iniciando Capítulo 04: Cross-Site Request Forgery (CSRF) ---")

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
# Tópico: Tokens CSRF
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Tokens CSRF")

def exemplo_vulneravel_tokens_csrf():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_tokens_csrf():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_tokens_csrf()
# exemplo_seguro_tokens_csrf()

# -----------------------------------------------------------------------------
# Tópico: SameSite Cookies
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: SameSite Cookies")

def exemplo_vulneravel_samesite_cookies():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_samesite_cookies():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_samesite_cookies()
# exemplo_seguro_samesite_cookies()

print("\n--- Capítulo concluído com sucesso ---")
