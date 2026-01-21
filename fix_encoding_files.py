import os

root = r"c:\projetos\learn-apps-code"

corrected = 0
for dirpath, dirnames, filenames in os.walk(root):
    for filename in filenames:
        if filename.endswith(".py"):
            filepath = os.path.join(dirpath, filename)
            
            content = None
            encoding_used = None
            
            # Try UTF-8 first
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
                encoding_used = 'utf-8'
            except UnicodeDecodeError:
                # Try Latin-1
                try:
                    with open(filepath, 'r', encoding='latin-1') as f:
                        content = f.read()
                    encoding_used = 'latin-1'
                except Exception as e:
                    print(f"Failed to read {filepath}: {e}")
                    continue
            
            if content is not None:
                # If it was latin-1, or if we just want to ensure it's saved as utf-8 with signature or whatever
                # Just writing it back as utf-8 effectively converts it.
                if encoding_used != 'utf-8':
                    print(f"Converting {filepath} from {encoding_used} to utf-8...")
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(content)
                    corrected += 1
                else:
                    # Even if it was UTF-8, if it had BOM or mixed, this standardizes it.
                    # But if it passed utf-8 check, it's valid utf-8.
                    pass

print(f"Converted {corrected} files to UTF-8.")
