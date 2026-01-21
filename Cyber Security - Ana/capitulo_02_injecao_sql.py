# -*- coding: utf-8 -*-
"""
Capítulo 2: SQL Injection - O Ataque Que Me Assustou
Curso: Cyber Security - Ana Rovina

Demonstração de SQL Injection e suas defesas.
ATENÇÃO: Código educacional - NÃO use em produção sem adaptações
"""

import sys
import io
import sqlite3
import hashlib

# Configuração UTF-8 para Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("=" * 70)
print("CAPÍTULO 2: SQL INJECTION")
print("=" * 70)
print("\n'Prepared statements são como templates de design: você define a")
print("estrutura, e os dados apenas preenchem os espaços.' - Ana Rovina\n")
print("=" * 70)

# ==================== SETUP DO BANCO DE DADOS ====================

def criar_banco_demonstracao():
    """
    Cria um banco SQLite para demonstração
    """
    conn = sqlite3.connect(':memory:')  # Banco em memória
    cursor = conn.cursor()

    # Criar tabela de usuários
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            email TEXT,
            role TEXT DEFAULT 'user'
        )
    ''')

    # Inserir dados de exemplo
    usuarios = [
        (1, 'admin', hashlib.md5('admin123'.encode()).hexdigest(), 'admin@exemplo.com', 'admin'),
        (2, 'ana', hashlib.md5('senha123'.encode()).hexdigest(), 'ana@exemplo.com', 'user'),
        (3, 'joao', hashlib.md5('joao456'.encode()).hexdigest(), 'joao@exemplo.com', 'user'),
        (4, 'maria', hashlib.md5('maria789'.encode()).hexdigest(), 'maria@exemplo.com', 'editor')
    ]

    cursor.executemany(
        'INSERT INTO users (id, username, password_hash, email, role) VALUES (?, ?, ?, ?, ?)',
        usuarios
    )

    conn.commit()
    return conn

# ==================== CÓDIGO VULNERÁVEL ====================

print("\n\n### EXEMPLO 1: CÓDIGO VULNERÁVEL ❌ ###\n")

def login_vulneravel(conn, username, password):
    """
    ❌ VULNERÁVEL: Concatenação direta de strings na query
    """
    cursor = conn.cursor()

    # PERIGO! Concatenação direta
    query = f"SELECT * FROM users WHERE username = '{username}' AND password_hash = '{password}'"

    print(f"Query executada: {query}")

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Erro SQL: {e}")
        return []

# Teste com credenciais normais
print("TESTE 1: Login normal")
print("-" * 50)
conn = criar_banco_demonstracao()
resultado = login_vulneravel(conn, 'ana', hashlib.md5('senha123'.encode()).hexdigest())
if resultado:
    print(f"✓ Login bem-sucedido: {resultado[0][1]}")
else:
    print("✗ Login falhou")

# Teste com SQL Injection
print("\n\nTESTE 2: SQL Injection Attack 🚨")
print("-" * 50)
conn = criar_banco_demonstracao()
# Payload de ataque: ' OR '1'='1' --
resultado = login_vulneravel(conn, "admin' OR '1'='1' --", "qualquer_coisa")
if resultado:
    print(f"⚠️  ATAQUE BEM-SUCEDIDO! Logado como: {resultado[0][1]}")
    print(f"   Usuários retornados: {len(resultado)}")
    for user in resultado:
        print(f"   - {user[1]} ({user[4]})")
else:
    print("✗ Ataque bloqueado")

# ==================== ANATOMIA DO ATAQUE ====================

print("\n\n### ANATOMIA DO ATAQUE ###\n")

print("Query esperada:")
print("   SELECT * FROM users WHERE username = 'ana' AND password_hash = 'hash123'")

print("\nQuery manipulada (SQL Injection):")
print("   Input: username = admin' OR '1'='1' --")
print("   Resultado: SELECT * FROM users WHERE username = 'admin' OR '1'='1' --' AND password_hash = ''")

print("\nO que acontece:")
print("   1. Fecha a string do username com '")
print("   2. Adiciona OR '1'='1' (sempre verdadeiro)")
print("   3. Comenta o resto da query com --")
print("   4. Resultado: retorna TODOS os usuários!")

# ==================== CÓDIGO SEGURO ====================

print("\n\n### EXEMPLO 2: CÓDIGO SEGURO ✅ ###\n")

def login_seguro(conn, username, password):
    """
    ✅ SEGURO: Prepared statements (parametrized queries)
    """
    cursor = conn.cursor()

    # Prepared statement - valores são tratados como dados, não código
    query = "SELECT * FROM users WHERE username = ? AND password_hash = ?"

    print(f"Query preparada: {query}")
    print(f"Parâmetros: ['{username}', '{password}']")

    try:
        cursor.execute(query, (username, password))
        results = cursor.fetchall()
        return results
    except sqlite3.Error as e:
        print(f"Erro SQL: {e}")
        return []

# Teste com credenciais normais
print("\nTESTE 1: Login normal")
print("-" * 50)
conn = criar_banco_demonstracao()
resultado = login_seguro(conn, 'ana', hashlib.md5('senha123'.encode()).hexdigest())
if resultado:
    print(f"✓ Login bem-sucedido: {resultado[0][1]}")
else:
    print("✗ Login falhou")

# Teste com SQL Injection (agora bloqueado)
print("\n\nTESTE 2: Tentativa de SQL Injection 🛡️")
print("-" * 50)
conn = criar_banco_demonstracao()
resultado = login_seguro(conn, "admin' OR '1'='1' --", "qualquer_coisa")
if resultado:
    print(f"⚠️  Ataque bem-sucedido: {resultado[0][1]}")
else:
    print("✓ Ataque bloqueado! Prepared statement funcionou.")

# ==================== TIPOS DE SQL INJECTION ====================

print("\n\n### TIPOS DE SQL INJECTION ###\n")

def demonstrar_union_based(conn):
    """
    Union-Based SQL Injection
    """
    print("1. UNION-BASED SQL INJECTION")
    print("-" * 50)
    print("Objetivo: Combinar resultados de múltiplas queries\n")

    # Vulnerável
    def buscar_vulneravel(product_id):
        cursor = conn.cursor()
        query = f"SELECT name, price FROM products WHERE id = {product_id}"
        print(f"Query: {query}")
        cursor.execute(query)
        return cursor.fetchall()

    print("Payload malicioso: 1 UNION SELECT username, password_hash FROM users")
    print("Resultado: Expõe todos os usuários e senhas!\n")

def demonstrar_blind_sql():
    """
    Blind SQL Injection
    """
    print("2. BLIND SQL INJECTION")
    print("-" * 50)
    print("Objetivo: Inferir informações baseado no comportamento da aplicação\n")

    print("Exemplo de payloads:")
    print("   • ' AND (SELECT COUNT(*) FROM users) > 5 --")
    print("   • ' AND SUBSTRING(username, 1, 1) = 'a' --")
    print("   • ' AND 1=1 -- (retorna True)")
    print("   • ' AND 1=2 -- (retorna False)\n")
    print("Atacante testa condições e infere estrutura do banco!\n")

def demonstrar_time_based():
    """
    Time-Based SQL Injection
    """
    print("3. TIME-BASED SQL INJECTION")
    print("-" * 50)
    print("Objetivo: Usar delays para confirmar vulnerabilidade\n")

    print("Payloads comuns:")
    print("   • ' AND SLEEP(5) -- (MySQL)")
    print("   • ' WAITFOR DELAY '00:00:05' -- (SQL Server)")
    print("   • ' AND 1=(SELECT COUNT(*) FROM users) -- (Generic)\n")
    print("Se a página demora 5 segundos, confirma vulnerabilidade!\n")

# Executar demonstrações
demonstrar_union_based(conn)
demonstrar_blind_sql()
demonstrar_time_based()

# ==================== DEFESAS CONTRA SQL INJECTION ====================

print("\n\n### DEFESAS CONTRA SQL INJECTION ###\n")

defesas = {
    "1. Prepared Statements": {
        "descrição": "A SOLUÇÃO MAIS EFICAZ! Separa código de dados.",
        "exemplo": "cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))"
    },
    "2. ORMs": {
        "descrição": "Frameworks que abstraem SQL (SQLAlchemy, Django ORM).",
        "exemplo": "User.objects.filter(username=username)"
    },
    "3. Validação de Input": {
        "descrição": "Rejeitar caracteres suspeitos e validar tipos.",
        "exemplo": "if not username.isalnum(): raise ValueError()"
    },
    "4. Whitelist": {
        "descrição": "Aceitar apenas valores conhecidos.",
        "exemplo": "if campo not in ['nome', 'email']: raise Error"
    },
    "5. Princípio do Menor Privilégio": {
        "descrição": "Usuário do DB com permissões mínimas.",
        "exemplo": "GRANT SELECT, INSERT ON tabela TO app_user"
    },
    "6. WAF (Web Application Firewall)": {
        "descrição": "Camada adicional de proteção.",
        "exemplo": "Cloudflare, AWS WAF, ModSecurity"
    }
}

for titulo, info in defesas.items():
    print(f"{titulo}")
    print(f"   Descrição: {info['descrição']}")
    print(f"   Exemplo: {info['exemplo']}\n")

# ==================== IMPLEMENTAÇÃO COMPLETA SEGURA ====================

print("\n### IMPLEMENTAÇÃO COMPLETA: SISTEMA DE LOGIN SEGURO ###\n")

class SistemaLoginSeguro:
    def __init__(self, db_path=':memory:'):
        self.conn = sqlite3.connect(db_path)
        self._criar_tabelas()

    def _criar_tabelas(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                failed_attempts INTEGER DEFAULT 0,
                locked_until TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()

    def registrar_usuario(self, username, password):
        """
        ✅ SEGURO: Registro com hash e salt
        """
        # Validação de input
        if not username or len(username) < 3:
            return False, "Username inválido"

        if not password or len(password) < 8:
            return False, "Senha deve ter no mínimo 8 caracteres"

        # Gerar salt único
        import os
        salt = os.urandom(32).hex()

        # Hash com salt
        password_hash = hashlib.sha256((password + salt).encode()).hexdigest()

        cursor = self.conn.cursor()

        try:
            # Prepared statement - SEGURO!
            cursor.execute(
                'INSERT INTO users (username, password_hash, salt) VALUES (?, ?, ?)',
                (username, password_hash, salt)
            )
            self.conn.commit()
            return True, "Usuário registrado com sucesso"
        except sqlite3.IntegrityError:
            return False, "Username já existe"

    def login(self, username, password):
        """
        ✅ SEGURO: Login com prepared statements
        """
        cursor = self.conn.cursor()

        # Prepared statement - SEGURO!
        cursor.execute(
            'SELECT id, password_hash, salt, failed_attempts FROM users WHERE username = ?',
            (username,)
        )

        result = cursor.fetchone()

        if not result:
            return False, "Credenciais inválidas"

        user_id, stored_hash, salt, failed_attempts = result

        # Verificar hash
        password_hash = hashlib.sha256((password + salt).encode()).hexdigest()

        if password_hash == stored_hash:
            # Resetar tentativas falhadas
            cursor.execute(
                'UPDATE users SET failed_attempts = 0 WHERE id = ?',
                (user_id,)
            )
            self.conn.commit()
            return True, f"Login bem-sucedido! User ID: {user_id}"
        else:
            # Incrementar tentativas falhadas
            cursor.execute(
                'UPDATE users SET failed_attempts = failed_attempts + 1 WHERE id = ?',
                (user_id,)
            )
            self.conn.commit()
            return False, "Credenciais inválidas"

# Demonstração do sistema seguro
print("DEMONSTRAÇÃO DO SISTEMA SEGURO:\n")
sistema = SistemaLoginSeguro()

# Registrar usuários
print("1. Registrando usuários...")
sucesso, msg = sistema.registrar_usuario('ana_rovina', 'SenhaForte123!')
print(f"   {msg}")

sucesso, msg = sistema.registrar_usuario('joao', 'OutraSenha456@')
print(f"   {msg}")

# Tentar login correto
print("\n2. Login com credenciais corretas...")
sucesso, msg = sistema.login('ana_rovina', 'SenhaForte123!')
print(f"   {'✓' if sucesso else '✗'} {msg}")

# Tentar SQL Injection
print("\n3. Tentativa de SQL Injection...")
sucesso, msg = sistema.login("admin' OR '1'='1' --", "qualquer")
print(f"   {'✗ FALHA DE SEGURANÇA!' if sucesso else '✓ Bloqueado'} {msg}")

# ==================== PAYLOADS DE TESTE ====================

print("\n\n### PAYLOADS COMUNS PARA TESTE ###\n")

payloads_teste = [
    "' OR '1'='1' --",
    "' OR '1'='1' /*",
    "admin'--",
    "admin' #",
    "' UNION SELECT NULL--",
    "' UNION SELECT username, password FROM users--",
    "1' AND '1'='1",
    "1' AND SLEEP(5)--",
    "'; DROP TABLE users;--",
]

print("⚠️  ATENÇÃO: Use apenas em ambientes de teste!\n")
for i, payload in enumerate(payloads_teste, 1):
    print(f"{i:2}. {payload}")

# ==================== FERRAMENTAS ====================

print("\n\n### FERRAMENTAS PARA TESTE DE SQL INJECTION ###\n")

ferramentas = [
    ("SQLMap", "Ferramenta automática para detectar e explorar SQL injection", "sqlmap -u 'http://site.com/page?id=1'"),
    ("Burp Suite", "Proxy para testar manualmente", "GUI - Manual testing"),
    ("OWASP ZAP", "Scanner automático de vulnerabilidades", "zap-cli quick-scan http://site.com"),
    ("w3af", "Framework de auditoria web", "w3af_console")
]

for nome, descricao, exemplo in ferramentas:
    print(f"• {nome}")
    print(f"  {descricao}")
    print(f"  Exemplo: {exemplo}\n")

# ==================== CHECKLIST ====================

print("\n### CHECKLIST DE PROTEÇÃO CONTRA SQL INJECTION ###\n")

checklist = [
    "Usar prepared statements em 100% das queries",
    "Implementar ORM com sanitização automática",
    "Validar e sanitizar TODOS os inputs",
    "Usar whitelist de caracteres quando possível",
    "Aplicar princípio do menor privilégio no DB",
    "Nunca exibir erros detalhados do DB ao usuário",
    "Implementar logging de queries suspeitas",
    "Realizar testes de segurança regulares",
    "Manter bibliotecas atualizadas",
    "Usar WAF como camada adicional"
]

for i, item in enumerate(checklist, 1):
    print(f"   [ ] {i}. {item}")

# ==================== CONCLUSÃO ====================

print("\n\n" + "=" * 70)
print("LIÇÕES APRENDIDAS")
print("=" * 70)

licoes = [
    "Nunca concatene strings SQL com input do usuário",
    "Prepared statements são a defesa mais eficaz",
    "ORMs ajudam, mas ainda precisa validar inputs",
    "Validação no frontend NÃO é suficiente",
    "Mensagens genéricas protegem contra information disclosure",
    "Monitoramento é essencial para detectar tentativas"
]

for i, licao in enumerate(licoes, 1):
    print(f"{i}. {licao}")

print("\n" + "=" * 70)
print("FIM DO CAPÍTULO 2")
print("=" * 70)
print("\n✅ Agora você sabe se proteger contra SQL Injection!")
print("📚 Próximo: Capítulo 3 - Cross-Site Scripting (XSS)\n")
