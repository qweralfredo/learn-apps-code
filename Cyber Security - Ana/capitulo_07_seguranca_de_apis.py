# -*- coding: utf-8 -*-
"""
Capítulo 07: Segurança de APIs
⚠️ AVISO: Códigos para fins educacionais. NÃO use exemplos vulneráveis em produção.
"""

import hashlib
import sqlite3

# ==============================================================================
# SETUP
# ==============================================================================
print(f"--- Iniciando Capítulo 07: Segurança de APIs ---")

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
# Tópico: JWT Issues
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: JWT Issues")

def exemplo_vulneravel_jwt_issues():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_jwt_issues():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_jwt_issues()
# exemplo_seguro_jwt_issues()

# -----------------------------------------------------------------------------
# Tópico: Rate Limiting
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Rate Limiting")

def exemplo_vulneravel_rate_limiting():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_rate_limiting():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_rate_limiting()
# exemplo_seguro_rate_limiting()

# -----------------------------------------------------------------------------
# Tópico: Input Validation
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Input Validation")

def exemplo_vulneravel_input_validation():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_input_validation():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_input_validation()
# exemplo_seguro_input_validation()

print("\n--- Capítulo concluído com sucesso ---")
