import os

root = r"c:\projetos\learn-apps-code"

mappings = {
    "Duckdb + Delta": 'con.execute("INSTALL delta; LOAD delta;")',
    "Duckdb + Iceberg": 'con.execute("INSTALL iceberg; LOAD iceberg;")',
    "Duckdb + S3": 'con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")',
    "Duckdb + Secrets": 'con.execute("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")'
}

for folder, injection in mappings.items():
    folder_path = os.path.join(root, folder)
    if not os.path.exists(folder_path):
        continue
    
    for filename in os.listdir(folder_path):
        if filename.endswith(".py"):
            filepath = os.path.join(folder_path, filename)
            
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Check if already injected
            if "LOAD delta" in content or "LOAD iceberg" in content or "LOAD httpfs" in content:
                # Already present (heuristic)
                # But allow check specific to folder
                if "LOAD " + folder.split(" + ")[1].lower() in content:
                     continue
            
            if "con = duckdb.connect()" in content:
                print(f"Injecting extensions into {filename}...")
                new_content = content.replace(
                    "con = duckdb.connect()",
                    f"con = duckdb.connect()\n{injection}"
                )
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)

print("Injection complete.")
