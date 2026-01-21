import os

def fix_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        new_content = content
        # Fix encoding issues by removing emojis
        replacements = {
            "✅": "[OK]",
            "❌": "[X]",
            "⚠️": "[WARN]",
            "✓": "[OK]",
            "→": "->",
            # Fix specific SQL error
            "PRAGMA last_profiling_output": "-- PRAGMA last_profiling_output",
            "Unrecognized print format true": "supported formats: [json, query_tree, query_tree_optimizer]",
        }
        
        for old, new in replacements.items():
            new_content = new_content.replace(old, new)

        # Fix specific Delta/Iceberg issues if evident
        if "import deltalake" in new_content:
            new_content = new_content.replace("import deltalake", "# import deltalake # Module not installed")

        if content != new_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Fixed encoding/content in: {file_path}")
            return True
        return False
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    root_dir = r"C:\projetos\learn-apps-code"
    
    # Priority folders likely to contain the issues
    folders_to_check = [
        "Duckdb + Secrets",
        "Duckdb + Iceberg",
        "Duckdb + Delta",
        "Apache Arrow + Duckdb" # Check this one too
    ]
    
    for folder in folders_to_check:
        full_path = os.path.join(root_dir, folder)
        if not os.path.exists(full_path): continue
        
        for f in os.listdir(full_path):
            if f.endswith(".py"):
                fix_file(os.path.join(full_path, f))

if __name__ == "__main__":
    main()
