# -*- coding: utf-8 -*-
"""
Capítulo 08: Criptografia e Proteção de Dados
⚠️ AVISO: Códigos para fins educacionais. NÃO use exemplos vulneráveis em produção.
"""

import hashlib
import sqlite3

# ==============================================================================
# SETUP
# ==============================================================================
print(f"--- Iniciando Capítulo 08: Criptografia e Proteção de Dados ---")

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
# Tópico: Symmetric Enc
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Symmetric Enc")

def exemplo_vulneravel_symmetric_enc():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_symmetric_enc():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_symmetric_enc()
# exemplo_seguro_symmetric_enc()

# -----------------------------------------------------------------------------
# Tópico: Asymmetric Enc
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Asymmetric Enc")

def exemplo_vulneravel_asymmetric_enc():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_asymmetric_enc():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_asymmetric_enc()
# exemplo_seguro_asymmetric_enc()

# -----------------------------------------------------------------------------
# Tópico: Hashing
# -----------------------------------------------------------------------------
print(f"\n>>> Explorando: Hashing")

def exemplo_vulneravel_hashing():
    """
    ❌ EXEMPLO VULNERÁVEL
    Demonstração de como NÃO fazer.
    """
    print("  [X] Executando código vulnerável...")
    # Implementação insegura aqui
    pass

def exemplo_seguro_hashing():
    """
    ✅ EXEMPLO SEGURO
    Demonstração da correção/boas práticas.
    """
    print("  [V] Executando código seguro...")
    # Implementação segura aqui
    pass

# Execução
# exemplo_vulneravel_hashing()
# exemplo_seguro_hashing()

print("\n--- Capítulo concluído com sucesso ---")
