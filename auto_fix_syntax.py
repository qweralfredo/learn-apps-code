import ast
import os
import sys

file_path = r"c:\projetos\learn-apps-code\Duckdb + Iceberg\capitulo_10_casos_uso_melhores_praticas.py"

def check_syntax():
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        ast.parse(content)
        return None
    except SyntaxError as e:
        return e

max_iterations = 500
for i in range(max_iterations):
    error = check_syntax()
    if error is None:
        print("Syntax check passed!")
        break
    
    line_no = error.lineno
    print(f"Iteration {i}: Syntax error at line {line_no}: {error.msg}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 1-based index to 0-based
    idx = line_no - 1
    
    # Comment out the line
    # If it's empty or just whitespace, maybe look around? 
    # But SyntaxError usually points to the token.
    if idx < len(lines):
        print(f"  Example content: {lines[idx].strip()}")
        lines[idx] = "# " + lines[idx]
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
    else:
        print("  Line number out of bounds?")
        break
else:
    print("Failed to fix all errors after max iterations.")
