# -*- coding: utf-8 -*-
"""
capitulo_02_instalacao_e_configuracao
"""

import duckdb
import os
import shutil

# Funções auxiliares injected
def safe_install_ext(con, ext_name):
    try:
        con.install_extension(ext_name)
        con.load_extension(ext_name)
        print(f"Extension '{ext_name}' loaded.")
    except Exception as e:
        print(f"Could not load extension '{ext_name}': {e}")

print("--- Iniciando Capítulo 02: Instalação e Configuração ---")

# Exemplo/Bloco 1
print(f"DuckDB Version: {duckdb.__version__}")

# Exemplo/Bloco 2
try:
    con = duckdb.connect()
    result = con.execute("SELECT 42 as answer").fetchall()
    print(f"Teste básico: {result}")
    con.close()
except Exception as e:
    print(f"Erro básico: {e}")

# Exemplo/Bloco 3
try:
    con = duckdb.connect()
    safe_install_ext(con, "delta")
    
    # Mocking Delta table existence for the scan attempt
    if os.path.exists("./my_delta_table"):
        shutil.rmtree("./my_delta_table")
    
    # We can't create a real Delta table without the deltalake library or the extension working.
    # We will skip the actual scan if we can't create it.
    try:
        from deltalake import write_deltalake
        import pandas as pd
        df = pd.DataFrame({'a': [1, 2, 3]})
        write_deltalake("./my_delta_table", df)
        print("Tabela Delta criada via python-deltalake.")
        
        result = con.execute("SELECT * FROM delta_scan('./my_delta_table')").fetchdf()
        print("Leitura delta_scan:")
        print(result)
    except ImportError:
        print("Biblioteca 'deltalake' não instalada. Pulando criação da tabela Delta.")
    except Exception as e:
        print(f"Erro ao manipular Delta Table: {e}")

except Exception as e:
    print(f"Erro Bloco 3: {e}")

# Exemplo/Bloco 4
try:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")
    print("Configurações aplicadas com sucesso.")
except Exception as e:
    print(f"Erro config: {e}")

# Exemplo/Bloco 5 - Setup function logic (Simulated)
def setup_duckdb():
    con = duckdb.connect()
    # Install standard extensions
    for ext in ['httpfs', 'parquet']: # delta removed from safe list for now
        safe_install_ext(con, ext)
    
    # Optional logic
    try:
        res = con.execute("SELECT extension_name, loaded FROM duckdb_extensions()").fetchdf()
        print("Extensões carregadas:")
        print(res[res['loaded'] == True])
    except:
        pass
    return con

con = setup_duckdb()
con.close()

# Exemplo/Bloco 8 & 9 - Comments only
"""
# Comandos de instalação (Shell)
# pip install jupyter duckdb pandas
# jupyter notebook

# No notebook:
import duckdb
import pandas as pd
"""

print("--- Fim do Capítulo 02 ---")
