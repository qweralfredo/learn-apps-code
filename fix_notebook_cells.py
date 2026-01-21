import json
import ast

nb_path = r"c:\projetos\learn-apps-code\Duckdb + Iceberg\capitulo_10_casos_uso_melhores_praticas.ipynb"
print(f"Fixing {nb_path}...")

with open(nb_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

fixed_count = 0
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        source = "".join(cell['source'])
        try:
            ast.parse(source)
        except SyntaxError:
            # Check if it looks like SQL or explanation text
            keywords = ["CREATE TABLE", "SELECT", "INSERT INTO", "PARTITIONED BY", "WHERE ", "FROM "]
            if any(k in source for k in keywords) or "1. " in source: # Numbered lists often cause syntax errors
                print(f"Converting cell to markdown: {source[:30]}...")
                cell['cell_type'] = 'markdown'
                fixed_count += 1

with open(nb_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)

print(f"Converted {fixed_count} cells to markdown.")
