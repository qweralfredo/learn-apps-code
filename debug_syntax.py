import ast
import json
import os

files = [
    r"Duckdb + Iceberg\capitulo_10_casos_uso_melhores_praticas.py",
    r"Duckdb + Delta\capitulo_02_instalacao_e_configuracao.ipynb",
    r"Duckdb + Delta\capitulo_08_particionamento_data_skipping.ipynb"
]

root = r"c:\projetos\learn-apps-code"

for f in files:
    path = os.path.join(root, f)
    print(f"Checking {path}...")
    try:
        if f.endswith('.py'):
            with open(path, 'r', encoding='utf-8') as file:
                content = file.read()
            ast.parse(content)
            print("  OK")
        else:
            with open(path, 'r', encoding='utf-8') as file:
                nb = json.load(file)
            
            code_content = ""
            for cell in nb['cells']:
                if cell['cell_type'] == 'code':
                    cell_source = "".join(cell['source'])
                    code_content += cell_source + "\n"
            
            try:
                ast.parse(code_content)
                print("  OK")
            except SyntaxError as e:
                print(f"  SyntaxError: {e}")
                print(f"  Line {e.lineno}: {e.text}")
                # Print context
                lines = code_content.splitlines()
                if e.lineno - 1 < len(lines):
                    print(f"  Context: {lines[e.lineno-1]}")

    except Exception as e:
        print(f"  Error: {e}")
