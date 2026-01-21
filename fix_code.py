import json
import re
import os

# Fix 1: .py file
py_file = r"c:\projetos\learn-apps-code\Duckdb + Iceberg\capitulo_10_casos_uso_melhores_praticas.py"
print(f"Fixing {py_file}...")
with open(py_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
for i, line in enumerate(lines):
    # Fix the raw SQL CREATE TABLE
    if "CREATE TABLE catalog.sales.orders (" in line:
        new_lines.append(f"# {line}")
    # Also fix closing parenthesis if it hangs
    elif line.strip() == ");" and "CREATE TABLE" not in lines[i-5:i]: # heuristic
         new_lines.append(f"# {line}")
    else:
        new_lines.append(line)

with open(py_file, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
print("Fixed .py file.")

# Fix 2: .ipynb file
nb_file = r"c:\projetos\learn-apps-code\Duckdb + Delta\capitulo_08_particionamento_data_skipping.ipynb"
print(f"Fixing {nb_file}...")
with open(nb_file, 'r', encoding='utf-8') as f:
    nb = json.load(f)

for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        new_source = []
        for line in cell['source']:
            # Fix dangling partition_by lines that are indented
            # e.g. "   partition_by=['country']"
            if re.match(r'^\s+partition_by=\[.*', line):
                 new_source.append(f"# {line}")
            # Also fix un-indented ones that might be floating
            elif re.match(r'^partition_by=\[.*\]\s*#', line):
                 new_source.append(f"# {line}")
            else:
                new_source.append(line)
        cell['source'] = new_source

with open(nb_file, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1)
print("Fixed .ipynb file.")
