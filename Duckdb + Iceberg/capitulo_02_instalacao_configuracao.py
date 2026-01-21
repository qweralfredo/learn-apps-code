# -*- coding: utf-8 -*-
"""
Capítulo 2: Instalação e Configuração da Extensão Iceberg
Curso: DuckDB + Apache Iceberg

Este script demonstra instalação e configuração do Iceberg no DuckDB
"""

import duckdb
import sys
import os

# Configurar encoding UTF-8 para Windows
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def instalacao_automatica():
    """Demonstra instalação automática"""
    print("\n" + "="*60)
    print("1. INSTALAÇÃO AUTOMÁTICA")
    print("="*60)

    con = duckdb.connect()

    # A primeira vez que usar, DuckDB instala automaticamente
    print("Tentando usar iceberg_scan (instalação automática)...")
    print("Nota: Precisa de uma tabela Iceberg válida para funcionar\n")

def instalacao_manual():
    """Demonstra instalação manual"""
    print("\n" + "="*60)
    print("2. INSTALAÇÃO MANUAL")
    print("="*60)

    con = duckdb.connect()

    # Instalar extensão
    print("Instalando extensão Iceberg...")
    con.execute("INSTALL iceberg")
    print("✅ Instalado")

    # Carregar extensão
    print("Carregando extensão Iceberg...")
    con.execute("LOAD iceberg")
    print("✅ Carregado")

    return con

def verificar_instalacao(con):
    """Verifica se extensão está instalada"""
    print("\n" + "="*60)
    print("3. VERIFICAR INSTALAÇÃO")
    print("="*60)

    result = con.execute("""
        SELECT extension_name, loaded, installed
        FROM duckdb_extensions()
        WHERE extension_name = 'iceberg'
    """).fetchone()

    if result:
        print(f"Extensão: {result[0]}")
        print(f"Loaded: {result[1]}")
        print(f"Installed: {result[2]}")
        print("✅ Iceberg instalado corretamente")
    else:
        print("❌ Iceberg não encontrado")

def atualizar_extensoes(con):
    """Atualiza extensões"""
    print("\n" + "="*60)
    print("4. ATUALIZAR EXTENSÕES")
    print("="*60)

    print("Atualizando extensões...")
    con.execute("UPDATE EXTENSIONS")
    print("✅ Extensões atualizadas")

def instalar_extensoes_relacionadas(con):
    """Instala extensões necessárias para cloud storage"""
    print("\n" + "="*60)
    print("5. EXTENSÕES RELACIONADAS (Cloud Storage)")
    print("="*60)

    # httpfs para S3/HTTP
    print("Instalando httpfs (para S3/HTTP)...")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    print("✅ httpfs instalado")

    # azure (opcional, para Azure Storage)
    print("\nInstalando azure (para Azure Blob Storage)...")
    try:
        con.execute("INSTALL azure")
        con.execute("LOAD azure")
        print("✅ azure instalado")
    except Exception as e:
        print(f"⚠️  azure não disponível: {e}")

def configuracoes_basicas(con):
    """Demonstra configurações básicas"""
    print("\n" + "="*60)
    print("6. CONFIGURAÇÕES BÁSICAS")
    print("="*60)

    # Threads
    print("Configurando threads...")
    con.execute("SET threads = 4")
    threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    print(f"Threads: {threads}")

    # Memória
    print("\nConfigurando memória...")
    con.execute("SET memory_limit = '4GB'")
    memory = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    print(f"Memory limit: {memory}")

def setup_desenvolvimento():
    """Configuração para ambiente de desenvolvimento"""
    print("\n" + "="*60)
    print("7. SETUP PARA DESENVOLVIMENTO")
    print("="*60)

    # Banco em memória para dev
    con = duckdb.connect(':memory:')

    # Extensões
    con.execute("INSTALL iceberg")
    con.execute("LOAD iceberg")

    # Configurações de dev
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '2GB'")

    # Habilitar version guessing (apenas dev!)
    con.execute("SET unsafe_enable_version_guessing = true")

    print("Configurações de desenvolvimento:")
    print("  - Banco: in-memory")
    print("  - Threads: 2")
    print("  - Memory: 2GB")
    print("  - Version guessing: HABILITADO (apenas dev!)")
    print("✅ Ambiente de desenvolvimento configurado")

    return con

def setup_producao():
    """Configuração para ambiente de produção"""
    print("\n" + "="*60)
    print("8. SETUP PARA PRODUÇÃO")
    print("="*60)

    # Banco persistente para produção
    db_path = 'prod_example.duckdb'
    con = duckdb.connect(db_path)

    # Extensões
    con.execute("INSTALL iceberg")
    con.execute("INSTALL httpfs")
    con.execute("LOAD iceberg")
    con.execute("LOAD httpfs")

    # Configurações de produção
    con.execute("SET threads = 8")
    con.execute("SET memory_limit = '16GB'")

    print("Configurações de produção:")
    print(f"  - Banco: {db_path}")
    print("  - Threads: 8")
    print("  - Memory: 16GB")
    print("  - Version guessing: DESABILITADO (segurança)")
    print("✅ Ambiente de produção configurado")

    # Limpar arquivo de exemplo
    con.close()
    if os.path.exists(db_path):
        os.remove(db_path)

def classe_helper_configuracao():
    """Classe helper para configuração reutilizável"""
    print("\n" + "="*60)
    print("9. CLASSE HELPER DE CONFIGURAÇÃO")
    print("="*60)

    class IcebergConfig:
        """Configuração padrão para DuckDB + Iceberg"""

        @staticmethod
        def setup(db_path=':memory:', threads=4, memory='4GB'):
            """
            Configura DuckDB com Iceberg

            Args:
                db_path: Caminho do banco (default: in-memory)
                threads: Número de threads
                memory: Limite de memória
            """
            # Conectar
            con = duckdb.connect(db_path)

            # Instalar extensões
            extensions = ['iceberg', 'httpfs']
            for ext in extensions:
                con.execute(f"INSTALL {ext}")
                con.execute(f"LOAD {ext}")

            # Configurações
            con.execute(f"SET threads = {threads}")
            con.execute(f"SET memory_limit = '{memory}'")

            return con

    # Usar
    print("Exemplo de uso:")
    print("""
    from iceberg_config import IcebergConfig

    # Desenvolvimento
    dev_con = IcebergConfig.setup(threads=2, memory='2GB')

    # Produção
    prod_con = IcebergConfig.setup(
        db_path='prod.duckdb',
        threads=16,
        memory='32GB'
    )
    """)

def teste_instalacao():
    """Testa instalação básica"""
    print("\n" + "="*60)
    print("10. TESTE DE INSTALAÇÃO")
    print("="*60)

    con = duckdb.connect()

    try:
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")
        print("✅ Extensão Iceberg carregada com sucesso")

        # Verificar versão
        version = con.execute("SELECT version()").fetchone()[0]
        print(f"✅ DuckDB versão: {version}")

        return True
    except Exception as e:
        print(f"❌ Erro ao carregar Iceberg: {e}")
        return False

def troubleshooting():
    """Demonstra soluções para problemas comuns"""
    print("\n" + "="*60)
    print("11. TROUBLESHOOTING COMUM")
    print("="*60)

    print("""
    Problema 1: Extensão não carrega
    Solução:
        con.execute("INSTALL iceberg")
        con.execute("LOAD iceberg")

    Problema 2: Versão incompatível
    Solução:
        # Iceberg requer DuckDB >= 1.4.0
        pip install --upgrade duckdb

    Problema 3: Problemas de rede/firewall
    Solução:
        # Verificar conectividade
        # Configurar proxy se necessário
        # Testar acesso a extensions.duckdb.org

    Problema 4: Permissões
    Solução:
        # Windows: Executar como administrador
        # Linux/Mac: Verificar permissões de escrita
    """)

def main():
    """Função principal"""
    print("="*60)
    print("CAPÍTULO 2: Instalação e Configuração")
    print("="*60)

    # Demonstrações
    instalacao_automatica()
    con = instalacao_manual()
    verificar_instalacao(con)
    atualizar_extensoes(con)
    instalar_extensoes_relacionadas(con)
    configuracoes_basicas(con)
    setup_desenvolvimento()
    setup_producao()
    classe_helper_configuracao()
    teste_instalacao()
    troubleshooting()

    print("\n" + "="*60)
    print("EXERCÍCIOS PRÁTICOS:")
    print("="*60)
    print("""
    1. Instale a extensão Iceberg no DuckDB
    2. Verifique se todas as extensões necessárias estão instaladas
    3. Configure um ambiente de desenvolvimento local
    4. Teste diferentes configurações de threads e memória
    5. Crie um módulo de configuração reutilizável
    """)

    print("\n✅ Capítulo 2 concluído!")
    print("📚 Próximo: Capítulo 3 - Leitura de Tabelas Iceberg")

if __name__ == "__main__":
    main()
