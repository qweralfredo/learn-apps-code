import os

def sanitize_file(file_path):
    print(f"Sanitizing {file_path}...")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        
        # Replace block drawing chars
        replacements = {
            "═": "=",
            "─": "-",
            "│": "|",
            "┌": "+",
            "┐": "+",
            "└": "+",
            "┘": "+",
            "├": "+",
            "┤": "+",
            "┬": "+",
            "┴": "+",
            "➜": "->",
            # Add previous ones just in case
            "✅": "[OK]",
            "❌": "[X]",
            "⚠️": "[WARN]",
            "✓": "[OK]",
        }
        
        new_text = text
        for old, new in replacements.items():
            new_text = new_text.replace(old, new)

        # Fix specific missing module patterns
        if "import deltalake" in new_text and "try:" not in new_text:
             new_text = new_text.replace("import deltalake", "try:\n    import deltalake\nexcept ImportError:\n    deltalake = None")

        if text != new_text:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_text)
            print("Modified.")
        else:
            print("No changes.")

    except Exception as e:
        print(f"Error: {e}")

folders = [
    r"C:\projetos\learn-apps-code\Duckdb + Secrets",
    r"C:\projetos\learn-apps-code\Duckdb + Delta"
]

for folder in folders:
    if os.path.exists(folder):
        for f in os.listdir(folder):
            if f.endswith(".py"):
                sanitize_file(os.path.join(folder, f))

# Fix arrow type error
arrow_file = r"C:\projetos\learn-apps-code\Apache Arrow + Duckdb\capitulo_01_introducao.py"
if os.path.exists(arrow_file):
    with open(arrow_file, 'r', encoding='utf-8') as f:
        c = f.read()
    if "'age': [30, 25, 35]" in c:
        # It seems DuckDB 32-bit vs PyArrow int64 issue or similar?
        # Force types
        c = c.replace("'age': [30, 25, 35]", "'age': pa.array([30, 25, 35], type=pa.int32())")
        with open(arrow_file, 'w', encoding='utf-8') as f:
            f.write(c)

