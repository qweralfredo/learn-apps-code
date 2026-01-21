import os

root = r"c:\projetos\learn-apps-code"

print("Fixing indentation of injected lines...")
for dirpath, dirnames, filenames in os.walk(root):
    for filename in filenames:
        if filename.endswith(".py"):
            filepath = os.path.join(dirpath, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            new_lines = []
            for i, line in enumerate(lines):
                # Check if this line is our injection
                is_injection = (
                    'con.execute("INSTALL delta;' in line or 
                    'con.execute("INSTALL iceberg;' in line or
                    'con.execute("INSTALL httpfs;' in line
                )
                
                if is_injection and i > 0:
                    prev_line = lines[i-1]
                    # Calculate indentation of prev line
                    prev_indent = len(prev_line) - len(prev_line.lstrip())
                    curr_indent = len(line) - len(line.lstrip())
                    
                    if curr_indent == 0 and prev_indent > 0:
                        # Fix it
                        new_lines.append(" " * prev_indent + line.lstrip())
                    else:
                        new_lines.append(line)
                else:
                    new_lines.append(line)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)

print("Indentation fixed.")

# Fix Iceberg file encoding/char issue
iceberg_file = r"c:\projetos\learn-apps-code\Duckdb + Iceberg\capitulo_01_introducao_apache_iceberg.py"
print(f"Sanitizing {iceberg_file}...")
with open(iceberg_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Filter out non-ascii to prevent 0xf3 issues in execution
clean_content = "".join(c for c in content if ord(c) < 128)

with open(iceberg_file, 'w', encoding='utf-8') as f:
    f.write(clean_content)

print("Sanitization complete.")
