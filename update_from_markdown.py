import os
import re
import json
import glob

SOURCE_ROOT = r"C:\projetos\Cursos"
DEST_ROOT = r"C:\projetos\learn-apps-code"

IGNORE_DIRS = [
    ".claude",
    "Code",
    "Apache Arrow + Duckdb",
    ".git",
    "__pycache__"
]

def normalize_name(filename):
    # Expected format: "Topic-01-chapter-name.md" or similar
    # We want: "capitulo_01_chapter_name"
    
    name_no_ext = os.path.splitext(filename)[0]
    
    # Try to find the number
    match = re.search(r'[-_ ](\d{2})[-_ ]', name_no_ext)
    if match:
        number = match.group(1)
        # Verify if it starts with the topic prefix
        # We can extract the part AFTER the number
        parts = re.split(r'[-_ ]\d{2}[-_ ]', name_no_ext)
        if len(parts) > 1:
            suffix = parts[1]
            suffix = suffix.replace('-', '_').replace(' ', '_').lower()
            return f"capitulo_{number}_{suffix}"
    
    # Fallback/specific cases handling if standard pattern fails
    return None

def extract_python_code(md_path):
    with open(md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    code_blocks = []
    # Regex to capture ```python content ```
    # Using dotall to match newlines
    matches = re.finditer(r'```python\s+(.*?)```', content, re.DOTALL)
    
    for match in matches:
        code = match.group(1).strip()
        if code:
             # simple filter to avoid empty or trivial shell commands formatted as python
            if "pip install" in code and len(code.splitlines()) == 1:
                continue
            code_blocks.append(code)
            
    return code_blocks

def create_py_file(dest_path, chapter_title, code_blocks):
    with open(dest_path, 'w', encoding='utf-8') as f:
        f.write("# -*- coding: utf-8 -*-\n")
        f.write(f'"""\n{chapter_title}\n"""\n\n')
        f.write(f"# {chapter_title}\n")
        f.write("import duckdb\n") # Default import to ensure it works for most lessons
        f.write("import os\n\n")
        
        for i, block in enumerate(code_blocks):
            f.write(f"# Exemplo/Bloco {i+1}\n")
            f.write(block)
            f.write("\n\n")

def py_to_ipynb(py_path, ipynb_path):
    with open(py_path, 'r', encoding='utf-8') as f:
        content = f.read()

    cells = []
    
    # Header docstring
    docstring_match = re.match(r'^\s*"""(.*?)"""', content, re.DOTALL)
    if docstring_match:
        docstring = docstring_match.group(1)
        cells.append({
            "cell_type": "markdown",
            "metadata": {},
            "source": docstring.strip().splitlines(True)
        })
        code_content = content[docstring_match.end():]
    else:
        code_content = content
    
    # Split by examples comments
    parts = re.split(r'(# Exemplo/Bloco \d+)', code_content)
    
    current_code = []
    
    # Pre-loop verify
    if parts and not parts[0].strip().startswith('# Exemplo'):
        # Usually imports
        if parts[0].strip():
             cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": parts[0].strip().splitlines(True)
            })
            
    for i in range(1, len(parts), 2):
        header = parts[i]
        code = parts[i+1]
        
        full_cell = header + "\n" + code.strip()
        cells.append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": full_cell.splitlines(True)
        })

    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    with open(ipynb_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2, ensure_ascii=False)

def process_course_dir(dir_name):
    source_dir = os.path.join(SOURCE_ROOT, dir_name)
    dest_dir = os.path.join(DEST_ROOT, dir_name)
    
    if not os.path.exists(source_dir):
        return

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    md_files = glob.glob(os.path.join(source_dir, "*.md"))
    
    # Track created files to cleanup old ones later?
    # User said: "caso não seja necessário arquivo de código, simplesmente não crie ou remova caso exista."
    
    # Strategy: Identify ALL valid capitulo files according to MD. 
    # Check existing code files in DEST. If one exists but isn't in valid list, delete it.
    
    valid_base_names = set()

    for md_path in md_files:
        filename = os.path.basename(md_path)
        if filename.lower() == "readme.md":
            continue
            
        new_name = normalize_name(filename)
        if not new_name:
            print(f"Skipping {filename} - could not normalize")
            continue
            
        code_blocks = extract_python_code(md_path)
        
        py_path = os.path.join(dest_dir, new_name + ".py")
        ipynb_path = os.path.join(dest_dir, new_name + ".ipynb")

        if len(code_blocks) > 0:
            print(f"Processing {filename} -> {new_name} (Has code)")
            create_py_file(py_path, filename.replace('.md', ''), code_blocks)
            py_to_ipynb(py_path, ipynb_path)
            valid_base_names.add(new_name)
        else:
            print(f"Processing {filename} -> {new_name} (NO code)")
            # Should NOT exist
            if os.path.exists(py_path):
                print(f"Removing {py_path}")
                os.remove(py_path)
            if os.path.exists(ipynb_path):
                print(f"Removing {ipynb_path}")
                os.remove(ipynb_path)

    # Cleanup orphans (files in dest that don't match any MD file)
    # WARNING: Be careful not to delete extra utility scripts
    # Only delete "capitulo_XX_..." files
    
    dest_files = os.listdir(dest_dir)
    for f in dest_files:
        if f.startswith("capitulo_") and (f.endswith(".py") or f.endswith(".ipynb")):
            base = os.path.splitext(f)[0]
            if base not in valid_base_names:
                # Double check: maybe the MD file naming convention was different?
                # But we constructed valid_base_names from ALL md files. 
                # If a manual file exists, it will be deleted.
                print(f"Found orphan: {f} - Removing...")
                os.remove(os.path.join(dest_dir, f))

def main():
    dirs = os.listdir(SOURCE_ROOT)
    for d in dirs:
        if d in IGNORE_DIRS:
            print(f"Skipping {d}")
            continue
            
        print(f"--- Course: {d} ---")
        process_course_dir(d)

if __name__ == "__main__":
    main()
