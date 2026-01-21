# -*- coding: utf-8 -*-
"""
capitulo_02_injecao_sql
"""

# capitulo_02_injecao_sql
import duckdb
import os

# Exemplo/Bloco 1
import psycopg2

# SEGURO! ✅
def login(username, password):
    conn = psycopg2.connect(database="mydb")
    cursor = conn.cursor()

    # Prepared statement com %s
    query = "SELECT * FROM users WHERE username = %s AND password = %s"
    cursor.execute(query, (username, password))

    result = cursor.fetchone()
    cursor.close()
    conn.close()

    return result

