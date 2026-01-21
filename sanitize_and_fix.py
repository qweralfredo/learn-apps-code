import os

def sanitize_file(file_path):
    print(f"Sanitizing {file_path}...")
    try:
        with open(file_path, 'rb') as f:
            raw = f.read()
        
        # Decode as utf-8, replace errors
        text = raw.decode('utf-8', errors='replace')
        
        # Replace common problem chars explicitly
        text = text.replace('✅', '[OK]')
        text = text.replace('❌', '[X]')
        text = text.replace('⚠️', '[WARN]')
        text = text.replace('✓', '[OK]')
        text = text.replace('→', '->')
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(text)
            
        print("Done.")
    except Exception as e:
        print(f"Error: {e}")

folders = [
    r"C:\projetos\learn-apps-code\Duckdb + Secrets",
    r"C:\projetos\learn-apps-code\Duckdb + Iceberg", 
    r"C:\projetos\learn-apps-code\Duckdb + S3"
]

for folder in folders:
    if os.path.exists(folder):
        for f in os.listdir(folder):
            if f.endswith(".py"):
                sanitize_file(os.path.join(folder, f))

# Fix missing module in delta
delta_file = r"C:\projetos\learn-apps-code\Duckdb + Delta\capitulo_03_introducao_extensao_delta.py"
if os.path.exists(delta_file):
    with open(delta_file, 'r', encoding='utf-8') as f:
        c = f.read()
    if "import deltalake" in c and "try:" not in c:
        c = c.replace("import deltalake", "try:\n    import deltalake\nexcept ImportError:\n    print('Deltalake not installed, skipping')\n    deltalake = None")
        with open(delta_file, 'w', encoding='utf-8') as f:
            f.write(c)
