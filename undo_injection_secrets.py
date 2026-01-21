import os

folder = r"c:\projetos\learn-apps-code\Duckdb + Secrets"
print(f"Reverting injections in {folder}...")

if os.path.exists(folder):
    for filename in os.listdir(folder):
        if filename.endswith(".py"):
            filepath = os.path.join(folder, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            new_lines = []
            for line in lines:
                if 'con.execute("INSTALL httpfs; LOAD httpfs;' in line:
                    continue # Skip the injected line
                new_lines.append(line)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.writelines(new_lines)

print("Reverted.")
