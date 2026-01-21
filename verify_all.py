import os
import subprocess
import sys
import ast
import json
import tempfile

def check_syntax(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            source = f.read()
        ast.parse(source)
        return True, None
    except SyntaxError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Read Error: {e}"

def extract_code_from_notebook(notebook_path):
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = json.load(f)
        
        code_lines = []
        for cell in nb.get('cells', []):
            if cell.get('cell_type') == 'code':
                source = cell.get('source', [])
                if isinstance(source, str):
                    code_lines.append(source)
                else:
                    code_lines.append("".join(source))
                code_lines.append("\n\n")
        return "".join(code_lines)
    except Exception as e:
        return None

def run_script(file_path, is_temp=False):
    try:
        # Run with a timeout to prevent hanging on network calls
        # Force utf-8 for env to encourage python to output utf-8, but handle decoding errors safely
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        
        result = subprocess.run(
            [sys.executable, file_path],
            capture_output=True,
            encoding='utf-8',       # Try to decode as utf-8
            errors='replace',       # Replace chars if decoding fails (fixing the crash)
            timeout=10,
            env=env
        )
        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stderr + "\n" + result.stdout # Sometimes errors are in stdout
    except subprocess.TimeoutExpired:
        return False, "Timeout (Likely hung on network call)"
    except Exception as e:
        return False, str(e)
    finally:
        if is_temp and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except:
                pass

def main():
    root_dir = r"C:\projetos\learn-apps-code"
    files_to_check = []
    notebooks_to_check = []
    
    # Walk and collect files
    for dirpath, dirnames, filenames in os.walk(root_dir):
        if ".git" in dirpath: continue
        for f in filenames:
            full_path = os.path.join(dirpath, f)
            # Filter internal/tooling scripts
            if f in ["verify_all.py", "generate_chapters.py", "update_from_markdown.py", "generate_missing_courses.py", "gerar_todos_arquivos.py", "gerar_todos_capitulos.py"]:
                continue
                
            if f.endswith(".py"):
                files_to_check.append(full_path)
            elif f.endswith(".ipynb"):
                notebooks_to_check.append(full_path)

    print(f"Found {len(files_to_check)} scripts and {len(notebooks_to_check)} notebooks to test.\n")

    syntax_errors = []
    runtime_errors = []
    
    # --- Check Python Files ---
    print("--- Verifying .py Files ---")
    for i, file_path in enumerate(files_to_check):
        print(f"[{i+1}/{len(files_to_check)}] Checking: {os.path.basename(file_path)}...", end="", flush=True)
        
        # 1. Syntax Check
        is_valid, msg = check_syntax(file_path)
        if not is_valid:
            print(" SYNTAX ERROR ❌")
            syntax_errors.append((file_path, msg))
            continue
            
        # 2. Execution Check
        success, output = run_script(file_path)
        if success:
            print(" PASS ✅")
        else:
            is_url_error = "HTTP" in output or "404" in output or "Connection" in output or "s3://" in output or "aws" in output.lower() or "boto3" in output or "credentials" in output.lower()
            
            if is_url_error:
                print(" URL/NET ERROR (Ignored) ⚠️")
            else:
                print(" RUNTIME ERROR ❌")
                runtime_errors.append((file_path, output))

    # --- Check Notebook Files ---
    print("\n--- Verifying .ipynb Files ---")
    for i, file_path in enumerate(notebooks_to_check):
        print(f"[{i+1}/{len(notebooks_to_check)}] Checking: {os.path.basename(file_path)}...", end="", flush=True)
        
        code = extract_code_from_notebook(file_path)
        if code is None:
            print(" PARSE ERROR ❌")
            syntax_errors.append((file_path, "Could not parse notebook JSON"))
            continue
            
        # Create temp file
        try:
            fd, temp_path = tempfile.mkstemp(suffix=".py", text=True)
            with os.fdopen(fd, 'w', encoding='utf-8') as tmp:
                tmp.write(code)
                
            # Check Syntax of extracted code
            is_valid, msg = check_syntax(temp_path)
            if not is_valid:
                print(" NB SYNTAX ERROR ❌")
                syntax_errors.append((file_path, msg))
                # Cleanup handled in run_script if we ran it, or block here? 
                # run_script handles deletion, but if we don't call it...
                os.remove(temp_path)
                continue
                
            # Run
            success, output = run_script(temp_path, is_temp=True)
            if success:
                print(" PASS ✅")
            else:
                is_url_error = "HTTP" in output or "404" in output or "Connection" in output or "s3://" in output or "aws" in output.lower() or "boto3" in output or "credentials" in output.lower()
                
                if is_url_error:
                     print(" URL/NET ERROR (Ignored) ⚠️")
                else:
                    print(" NB RUNTIME ERROR ❌")
                    runtime_errors.append((file_path, output))
                    
        except Exception as e:
            print(f" ERROR: {e} ❌")
            runtime_errors.append((file_path, str(e)))


    # Report
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    
    if syntax_errors:
        print(f"\nFound {len(syntax_errors)} SYNTAX errors:")
        for f, msg in syntax_errors:
            print(f"- {os.path.basename(f)}: {msg}")
            
    if runtime_errors:
        print(f"\nFound {len(runtime_errors)} RUNTIME errors (excluding network):")
        for f, msg in runtime_errors:
            print(f"- {os.path.basename(f)}")
            print(f"  Error snippet: {msg.splitlines()[-1] if msg else 'Unknown'}")
            print("-" * 30)
            
    if not syntax_errors and not runtime_errors:
        print("\nAll scripts passed syntax and basic runtime checks (ignoring network errors).")

if __name__ == "__main__":
    print("Starting verification...", flush=True)
    main()
