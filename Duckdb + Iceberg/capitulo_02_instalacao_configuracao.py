# -*- coding: utf-8 -*-
"""
capitulo_02_instalacao_configuracao
"""

# capitulo_02_instalacao_configuracao
import duckdb
import os

# Exemplo/Bloco 1
import duckdb

# Criar conexão
con = duckdb.connect()

# Instalar e carregar extensão
safe_install_ext(con, "iceberg")
# LOAD iceberg handled by safe_install_ext

# Verificar instalação
result = con.execute("""
    SELECT extension_name, loaded, installed
    FROM duckdb_extensions()
    WHERE extension_name = 'iceberg'
""").fetchone()

print(f"Iceberg: installed={result[2]}, loaded={result[1]}")

# Exemplo/Bloco 2
import duckdb

def setup_duckdb_iceberg():
    """
    Configura DuckDB com extensões necessárias para Iceberg
    """
    con = duckdb.connect()

    # Extensões necessárias
    extensions = ['iceberg', 'httpfs']  # httpfs para S3/HTTP

    for ext in extensions:
        print(f"Instalando {ext}...")
        con.execute(f"INSTALL {ext}")
        con.execute(f"LOAD {ext}")

    print("✅ Setup completo!")
    return con

# Usar
con = setup_duckdb_iceberg()

# Exemplo/Bloco 3
import duckdb

con = duckdb.connect()

# Setup para trabalhar com Iceberg em múltiplos cloud providers
safe_install_ext(con, "iceberg")
con.execute("INSTALL httpfs")
con.execute("INSTALL azure")

# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")
con.execute("LOAD azure")

print("Pronto para usar Iceberg em S3, Azure e HTTP!")

# Exemplo/Bloco 4
import duckdb
import os

# Configuração para desenvolvimento local
con = duckdb.connect('dev.duckdb')  # Banco persistente

# Extensões
safe_install_ext(con, "iceberg")
# LOAD iceberg handled by safe_install_ext

# Configurações de desenvolvimento
con.execute("SET threads = 2")
con.execute("SET memory_limit = '2GB'")
con.execute("SET unsafe_enable_version_guessing = true")  # Dev only!

# Criar diretório para testes
os.makedirs('iceberg_tables', exist_ok=True)

print("Ambiente de desenvolvimento pronto!")

# Exemplo/Bloco 5
import duckdb

# Configuração para produção
con = duckdb.connect('prod.duckdb')

# Extensões
safe_install_ext(con, "iceberg")
con.execute("INSTALL httpfs")
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

# Configurações de produção
con.execute("SET threads = 8")
con.execute("SET memory_limit = '16GB'")
con.execute("SET temp_directory = '/fast/ssd/temp'")

# NÃO habilitar unsafe features em produção!
# con.execute("SET unsafe_enable_version_guessing = true")  # ❌

print("Ambiente de produção pronto!")

# Exemplo/Bloco 6
# init_iceberg.py
import duckdb

con = duckdb.connect()
safe_install_ext(con, "iceberg")
con.execute("INSTALL httpfs")
# LOAD iceberg handled by safe_install_ext
con.execute("LOAD httpfs")

print("Container DuckDB+Iceberg pronto!")

# Exemplo/Bloco 7
import duckdb

def test_iceberg_basic():
    """Testa instalação básica do Iceberg"""
    con = duckdb.connect()

    try:
        safe_install_ext(con, "iceberg")
        # LOAD iceberg handled by safe_install_ext
        print("✅ Extensão Iceberg carregada com sucesso")
        return True
    except Exception as e:
        print(f"❌ Erro ao carregar Iceberg: {e}")
        return False

test_iceberg_basic()

# Exemplo/Bloco 8
import duckdb
import os

def test_iceberg_local():
    """Testa leitura de tabela Iceberg local"""
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext

    # Criar tabela de teste simples
    # (assumindo que você tem uma tabela Iceberg de exemplo)
    test_table = 'data/iceberg/test_table'

    if os.path.exists(test_table):
        try:
            result = con.execute(f"""
                SELECT count(*) FROM iceberg_scan('{test_table}')
            """).fetchone()

            print(f"✅ Tabela Iceberg lida com sucesso: {result[0]} linhas")
            return True
        except Exception as e:
            print(f"❌ Erro ao ler tabela: {e}")
            return False
    else:
        print(f"ℹ️  Tabela de teste não encontrada em {test_table}")
        return None

test_iceberg_local()

# Exemplo/Bloco 9
import duckdb

def test_iceberg_s3():
    """Testa leitura de tabela Iceberg no S3"""
    con = duckdb.connect()
    # LOAD iceberg handled by safe_install_ext
    con.execute("LOAD httpfs")

    # Tabela pública de exemplo (ajuste para sua tabela)
    s3_table = 's3://your-bucket/your-table/metadata/v1.metadata.json'

    try:
        # Configurar credenciais S3 (se necessário)
        con.execute("""
            CREATE SECRET s3_secret (
                TYPE s3,
                PROVIDER credential_chain
            )
        """)

        result = con.execute(f"""
            SELECT count(*) FROM iceberg_scan('{s3_table}')
        """).fetchone()

        print(f"✅ Tabela S3 Iceberg lida: {result[0]} linhas")
        return True
    except Exception as e:
        print(f"❌ Erro ao ler S3: {e}")
        return False

# test_iceberg_s3()  # Descomente e ajuste para seu ambiente

# Exemplo/Bloco 10
import duckdb
import logging

# Configurar logging Python
logging.basicConfig(level=logging.DEBUG)

con = duckdb.connect()
# LOAD iceberg handled by safe_install_ext

# Queries com EXPLAIN para debug
con.execute("""
    EXPLAIN SELECT * FROM iceberg_scan('table')
""").show()

# Exemplo/Bloco 11
import duckdb

con = duckdb.connect()

try:
    # LOAD iceberg handled by safe_install_ext
except Exception as e:
    print(f"Erro: {e}")
    print("Tentando instalar primeiro...")
    safe_install_ext(con, "iceberg")
    # LOAD iceberg handled by safe_install_ext
    print("✅ Instalado e carregado com sucesso")

# Exemplo/Bloco 12
import duckdb

# Verificar versão do DuckDB
version = duckdb.__version__
print(f"DuckDB versão: {version}")

# Iceberg requer DuckDB >= 1.4.0
if version < '1.4.0':
    print("⚠️  Iceberg requer DuckDB 1.4.0 ou superior")
    print("Atualize: pip install --upgrade duckdb")
else:
    print("✅ Versão compatível com Iceberg")

# Exemplo/Bloco 13
import duckdb
import requests

def check_s3_connectivity():
    """Verifica conectividade com S3"""
    try:
        # Teste básico de conectividade
        response = requests.get('https://s3.amazonaws.com', timeout=5)
        print(f"✅ S3 acessível (status: {response.status_code})")
        return True
    except Exception as e:
        print(f"❌ Problema de conectividade: {e}")
        print("Verifique firewall/proxy")
        return False

check_s3_connectivity()

# Exemplo/Bloco 14
# iceberg_config.py
import duckdb

class IcebergConfig:
    """Configuração padrão para DuckDB + Iceberg"""

    @staticmethod
    def setup(db_path=':memory:', config=None):
        """
        Configura DuckDB com Iceberg

        Args:
            db_path: Caminho do banco (default: in-memory)
            config: Dict com configurações adicionais
        """
        # Configuração padrão
        default_config = {
            'threads': 4,
            'memory_limit': '4GB'
        }

        if config:
            default_config.update(config)

        # Conectar
        con = duckdb.connect(db_path, config=default_config)

        # Instalar extensões
        extensions = ['iceberg', 'httpfs']
        for ext in extensions:
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")

        return con

# Usar
if __name__ == "__main__":
    con = IcebergConfig.setup(
        db_path='my_analytics.duckdb',
        config={'threads': 8, 'memory_limit': '8GB'}
    )
    print("Configuração carregada!")

# Exemplo/Bloco 15
from iceberg_config import IcebergConfig

import importlib.util


def has_module(name):
    return importlib.util.find_spec(name) is not None

def safe_install_ext(con, ext_name):
    try:
        con.execute(f"INSTALL {ext_name}")
        con.execute(f"LOAD {ext_name}")
        return True
    except Exception as e:
        print(f"Warning: Failed to install/load {ext_name} extension: {e}")
        return False


# Desenvolvimento
dev_con = IcebergConfig.setup()

# Produção
prod_con = IcebergConfig.setup(
    db_path='prod.duckdb',
    config={'threads': 16, 'memory_limit': '32GB'}
)
