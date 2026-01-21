import os

py_file = r"c:\projetos\learn-apps-code\Duckdb + Iceberg\capitulo_10_casos_uso_melhores_praticas.py"
print(f"Fixing {py_file}...")

with open(py_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

new_lines = []
in_sql_block = False

for line in lines:
    stripped = line.strip()
    
    # Detect start of block (already commented by previous script)
    if "# CREATE TABLE catalog.sales.orders (" in line:
        in_sql_block = True
        new_lines.append(line)
        continue
    
    # Logic for inside block
    if in_sql_block:
        if not stripped: # Empty line
            new_lines.append(line)
        elif line.startswith(" ") or line.startswith("\t"): # Indented
            new_lines.append("# " + line)
        elif stripped.startswith(")"): # Closing parenthesis of SQL
            new_lines.append("# " + line)
            in_sql_block = False
        else: # Found something unindented, end of block
            in_sql_block = False
            new_lines.append(line)
    else:
        new_lines.append(line)

with open(py_file, 'w', encoding='utf-8') as f:
    f.writelines(new_lines)
print("Fixed .py file v2.")
