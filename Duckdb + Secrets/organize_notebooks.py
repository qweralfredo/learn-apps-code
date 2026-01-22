"""
Script para organizar notebooks DuckDB + Secrets
Adiciona células markdown separando blocos lógicos e instalação de dependências
"""

import json
import re
from pathlib import Path


def create_markdown_cell(content):
    """Cria uma célula markdown"""
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [content + "\n"]
    }


def create_code_cell(content):
    """Cria uma célula de código"""
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [content + "\n"]
    }


def split_code_into_blocks(code_content):
    """
    Divide o código em blocos lógicos baseado nos comentários # Exemplo/Bloco
    """
    blocks = []
    current_block = []
    current_block_num = None
    
    lines = code_content.split('\n')
    
    for line in lines:
        # Detecta início de um novo bloco
        if re.match(r'# Exemplo/Bloco \d+', line):
            # Salva o bloco anterior se existir
            if current_block and current_block_num is not None:
                blocks.append((current_block_num, '\n'.join(current_block)))
            
            # Inicia novo bloco
            match = re.search(r'# Exemplo/Bloco (\d+)', line)
            current_block_num = int(match.group(1))
            current_block = []
        elif current_block_num is not None:
            current_block.append(line)
    
    # Adiciona o último bloco
    if current_block and current_block_num is not None:
        blocks.append((current_block_num, '\n'.join(current_block)))
    
    return blocks


def organize_notebook(notebook_path, title, description):
    """
    Organiza um notebook adicionando:
    - Título e descrição
    - Instalação de dependências
    - Separadores markdown entre blocos lógicos
    """
    print(f"\n{'='*60}")
    print(f"Organizando: {notebook_path.name}")
    print(f"{'='*60}")
    
    # Ler notebook
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    # Encontrar a célula de código (segunda célula)
    code_cell_index = None
    code_content = ""
    
    for i, cell in enumerate(notebook['cells']):
        if cell['cell_type'] == 'code':
            code_cell_index = i
            code_content = ''.join(cell['source'])
            break
    
    if code_cell_index is None:
        print("  ❌ Não encontrou célula de código")
        return 0
    
    # Dividir código em blocos
    blocks = split_code_into_blocks(code_content)
    
    if not blocks:
        print("  ❌ Não encontrou blocos")
        return 0
    
    print(f"  ✓ Encontrados {len(blocks)} blocos de código")
    
    # Criar nova estrutura de células
    new_cells = []
    
    # 1. Título principal
    new_cells.append(create_markdown_cell(f"# {title}\n\n{description}"))
    
    # 2. Instalação de dependências
    new_cells.append(create_markdown_cell("## 📦 Instalação de Dependências"))
    new_cells.append(create_code_cell("!pip install -q duckdb"))
    
    # 3. Imports
    import_block = []
    for line in code_content.split('\n')[:10]:  # Primeiras 10 linhas
        if 'import' in line and not line.strip().startswith('#'):
            import_block.append(line)
    
    if import_block:
        new_cells.append(create_markdown_cell("## 📚 Imports"))
        new_cells.append(create_code_cell('\n'.join(import_block)))
    
    # 4. Adicionar blocos com separadores
    for i, (block_num, block_code) in enumerate(blocks, 1):
        # Adicionar separador markdown antes de cada bloco
        new_cells.append(create_markdown_cell(f"## 📝 Bloco {i}: Exemplo {block_num}"))
        
        # Adicionar o código do bloco
        new_cells.append(create_code_cell(block_code.strip()))
    
    # Substituir células no notebook
    notebook['cells'] = new_cells
    
    # Salvar notebook organizado
    with open(notebook_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, ensure_ascii=False, indent=1)
    
    print(f"  ✓ Notebook organizado com sucesso!")
    print(f"  ✓ Total de blocos: {len(blocks)}")
    
    return len(blocks)


def main():
    """Organiza todos os notebooks da série DuckDB + Secrets"""
    
    notebooks_config = [
        {
            "file": "capitulo_01_introducao_secrets.ipynb",
            "title": "Capítulo 01: Introdução ao Sistema de Secrets do DuckDB",
            "description": "Este notebook apresenta os fundamentos do sistema de gerenciamento de secrets do DuckDB, incluindo criação, listagem, tipos de secrets e gerenciamento básico."
        },
        {
            "file": "capitulo_02_tipos_secrets.ipynb",
            "title": "Capítulo 02: Tipos de Secrets Disponíveis",
            "description": "Explorando os diferentes tipos de secrets suportados pelo DuckDB: S3, R2, GCS, Azure, HTTP, Hugging Face, MySQL e PostgreSQL."
        },
        {
            "file": "capitulo_03_cloud_storage_secrets.ipynb",
            "title": "Capítulo 03: Secrets para Cloud Storage",
            "description": "Configuração detalhada de secrets para provedores de cloud storage: AWS S3, Cloudflare R2, Google Cloud Storage e Azure Blob Storage."
        },
        {
            "file": "capitulo_04_database_secrets.ipynb",
            "title": "Capítulo 04: Secrets para Databases",
            "description": "Gerenciamento de secrets para conexões com databases relacionais: MySQL e PostgreSQL, incluindo configurações SSL e queries cross-database."
        },
        {
            "file": "capitulo_05_persistent_secrets.ipynb",
            "title": "Capítulo 05: Persistent Secrets",
            "description": "Secrets persistentes: armazenamento em disco, backup, restore, segurança, permissões e melhores práticas para produção."
        }
    ]
    
    base_path = Path(__file__).parent
    total_blocks = 0
    
    print("\n" + "="*60)
    print("ORGANIZAÇÃO DE NOTEBOOKS - DUCKDB + SECRETS")
    print("="*60)
    
    results = []
    
    for config in notebooks_config:
        notebook_path = base_path / config["file"]
        
        if not notebook_path.exists():
            print(f"\n❌ Arquivo não encontrado: {config['file']}")
            results.append((config['file'], 0))
            continue
        
        num_blocks = organize_notebook(
            notebook_path,
            config["title"],
            config["description"]
        )
        
        total_blocks += num_blocks
        results.append((config['file'], num_blocks))
    
    # Resumo final
    print("\n" + "="*60)
    print("RESUMO DA ORGANIZAÇÃO")
    print("="*60)
    
    for filename, num_blocks in results:
        status = "✓" if num_blocks > 0 else "✗"
        print(f"{status} {filename:45} → {num_blocks:2} blocos")
    
    print(f"\n{'='*60}")
    print(f"Total de blocos organizados: {total_blocks}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
