# -*- coding: utf-8 -*-
"""
Capítulo 1: Introdução à Segurança de Aplicações Web
Curso: Cyber Security - Ana Rovina

Exemplos práticos de conceitos básicos de segurança web.
ATENÇÃO: Código educacional - demonstra vulnerabilidades e correções
"""

import sys
import io

# Configuração UTF-8 para Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("=" * 70)
print("CAPÍTULO 1: INTRODUÇÃO À SEGURANÇA DE APLICAÇÕES WEB")
print("=" * 70)
print("\nPor: Ana Rovina")
print("Curso: Cyber Security - Da Designer à Security-Minded Developer")
print("=" * 70)

# ==================== CONCEITOS BÁSICOS ====================

print("\n\n### OS TRÊS PILARES DA SEGURANÇA ###\n")

# 1. Confidencialidade
print("1. CONFIDENCIALIDADE 🔒")
print("   Garantir que apenas pessoas autorizadas acessem informações sensíveis")
print("   Exemplo: Criptografia de dados, controle de acesso")

# 2. Integridade
print("\n2. INTEGRIDADE ✓")
print("   Assegurar que os dados não sejam alterados sem autorização")
print("   Exemplo: Hashing, assinaturas digitais")

# 3. Disponibilidade
print("\n3. DISPONIBILIDADE ⚡")
print("   Manter o sistema acessível quando necessário")
print("   Exemplo: Redundância, DDoS protection")

# ==================== OWASP TOP 10 (2025) ====================

print("\n\n### OWASP TOP 10 - 2025 ###\n")

owasp_top_10 = [
    ("1", "Broken Access Control", "Controle de Acesso Quebrado"),
    ("2", "Cryptographic Failures", "Falhas Criptográficas"),
    ("3", "Injection", "Injeção"),
    ("4", "Insecure Design", "Design Inseguro"),
    ("5", "Security Misconfiguration", "Configuração Incorreta de Segurança"),
    ("6", "Vulnerable and Outdated Components", "Componentes Vulneráveis e Desatualizados"),
    ("7", "Identification and Authentication Failures", "Falhas de Identificação e Autenticação"),
    ("8", "Software and Data Integrity Failures", "Falhas de Integridade de Software e Dados"),
    ("9", "Security Logging and Monitoring Failures", "Falhas de Log e Monitoramento"),
    ("10", "Server-Side Request Forgery (SSRF)", "Falsificação de Solicitação do Lado do Servidor")
]

for rank, nome_en, nome_pt in owasp_top_10:
    print(f"   {rank}. {nome_en} ({nome_pt})")

# ==================== EXEMPLO PRÁTICO: VALIDAÇÃO DE INPUT ====================

print("\n\n### EXEMPLO PRÁTICO: VALIDAÇÃO DE INPUT ###\n")

def validar_email_inseguro(email):
    """
    ❌ VULNERÁVEL: Validação muito simples
    """
    return "@" in email

def validar_email_seguro(email):
    """
    ✅ SEGURO: Validação adequada
    """
    import re
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

# Testes
emails_teste = [
    "ana@exemplo.com",
    "usuario@dominio",
    "@exemplo.com",
    "usuario@@dominio.com",
    "ana.rovina@empresa.com.br"
]

print("VALIDAÇÃO INSEGURA ❌:")
for email in emails_teste:
    resultado = validar_email_inseguro(email)
    status = "✓ Válido" if resultado else "✗ Inválido"
    print(f"   {email:30} -> {status}")

print("\nVALIDAÇÃO SEGURA ✅:")
for email in emails_teste:
    resultado = validar_email_seguro(email)
    status = "✓ Válido" if resultado else "✗ Inválido"
    print(f"   {email:30} -> {status}")

# ==================== CHECKLIST BÁSICO ====================

print("\n\n### CHECKLIST RÁPIDO DE SEGURANÇA ###\n")

checklist = [
    "Validei todas as entradas do usuário?",
    "Sanitizei as saídas de dados?",
    "Implementei autenticação adequada?",
    "Configurei permissões corretas?",
    "Os dados sensíveis estão criptografados?",
    "Tenho logs de segurança?",
    "As dependências estão atualizadas?"
]

for i, item in enumerate(checklist, 1):
    print(f"   [ ] {i}. {item}")

# ==================== FERRAMENTAS ESSENCIAIS ====================

print("\n\n### FERRAMENTAS ESSENCIAIS ###\n")

ferramentas = {
    "Análise de Vulnerabilidades": [
        "OWASP ZAP - Scanner automático",
        "Burp Suite - Testes de penetração",
        "npm audit/yarn audit - Dependências"
    ],
    "Desenvolvimento Seguro": [
        "ESLint com plugins de segurança",
        "Bandit (Python) - Análise estática",
        "Safety - Verificação de dependências Python"
    ],
    "Monitoramento": [
        "Sentry - Rastreamento de erros",
        "Cloudflare - WAF",
        "AWS GuardDuty - Detecção de ameaças"
    ]
}

for categoria, tools in ferramentas.items():
    print(f"\n{categoria}:")
    for tool in tools:
        print(f"   • {tool}")

# ==================== EXEMPLO: VERIFICAÇÃO DE SEGURANÇA BÁSICA ====================

print("\n\n### EXEMPLO: ANÁLISE BÁSICA DE CÓDIGO ###\n")

def analisar_codigo_seguranca(codigo):
    """
    Análise básica de código para identificar possíveis problemas
    """
    problemas = []

    # Verificações simples
    if "eval(" in codigo:
        problemas.append("⚠️  CRÍTICO: Uso de eval() detectado (permite execução de código arbitrário)")

    if "password" in codigo.lower() and "=" in codigo:
        problemas.append("⚠️  ALERTA: Possível senha hardcoded no código")

    if "SELECT *" in codigo.upper() and "+" in codigo:
        problemas.append("⚠️  CRÍTICO: Possível SQL Injection (concatenação de string em SQL)")

    if ".innerHTML" in codigo and "user" in codigo.lower():
        problemas.append("⚠️  ALERTA: Possível XSS (uso de innerHTML com dados de usuário)")

    return problemas

# Exemplos de código vulnerável
codigos_exemplo = [
    'user_input = input("Enter code: ")\neval(user_input)  # PERIGO!',
    'password = "senha123"  # Hardcoded password',
    'query = "SELECT * FROM users WHERE id = " + user_id',
    'element.innerHTML = user_data'
]

print("ANÁLISE DE CÓDIGO:\n")
for i, codigo in enumerate(codigos_exemplo, 1):
    print(f"Exemplo {i}:")
    print(f"   Código: {codigo.split(chr(10))[0]}...")
    problemas = analisar_codigo_seguranca(codigo)
    if problemas:
        for problema in problemas:
            print(f"   {problema}")
    else:
        print("   ✓ Nenhum problema detectado")
    print()

# ==================== ESTATÍSTICAS DE SEGURANÇA ====================

print("\n### ESTATÍSTICAS DE SEGURANÇA (2026) ###\n")

estatisticas = {
    "Frequência de ataques": "Aplicações web são atacadas a cada 39 segundos",
    "Vulnerabilidades": "94% das aplicações têm pelo menos uma vulnerabilidade",
    "Custo médio de violação": "US$ 4.5 milhões por incidente",
    "Tempo médio de detecção": "207 dias para detectar uma violação",
    "Tempo de contenção": "73 dias para conter uma violação"
}

for metrica, valor in estatisticas.items():
    print(f"   • {metrica}: {valor}")

# ==================== PRÓXIMOS PASSOS ====================

print("\n\n### PRÓXIMOS PASSOS ###\n")

print("""
Nos próximos capítulos, você aprenderá:

   Capítulo 2: SQL Injection - Como atacantes manipulam seu banco de dados
   Capítulo 3: XSS - Scripts maliciosos na sua interface linda
   Capítulo 4: CSRF - Ataques invisíveis aos seus usuários
   Capítulo 5: Autenticação e Autorização - Quem é você e o que pode fazer
   Capítulo 6: Controle de Acesso - Portas e chaves digitais
   Capítulo 7: Segurança de APIs - Protegendo seus endpoints
   Capítulo 8: Criptografia - Tornando dados ilegíveis para invasores
   Capítulo 9: Configurações Seguras - DevSecOps na prática
   Capítulo 10: Checklist Final - Seu kit de segurança completo
""")

print("\n" + "=" * 70)
print("'A melhor hora para pensar em segurança foi no início do projeto.")
print("A segunda melhor hora é agora.' - Ana Rovina")
print("=" * 70)

# ==================== EXERCÍCIOS PRÁTICOS ====================

print("\n\n### EXERCÍCIOS PRÁTICOS ###\n")

def exercicio_1():
    """
    Exercício: Identificar problemas de segurança
    """
    print("EXERCÍCIO 1: Identifique os problemas de segurança\n")

    codigo_vulneravel = """
    def login(username, password):
        query = f"SELECT * FROM users WHERE username = '{username}' AND password = '{password}'"
        result = db.execute(query)
        return result
    """

    print("Código:")
    print(codigo_vulneravel)
    print("\nProblemas identificados:")
    print("   1. SQL Injection - Concatenação direta de strings na query")
    print("   2. Senha em texto plano - Password não está hasheada")
    print("   3. Sem validação de input - Username e password não são validados")

    print("\n✅ SOLUÇÃO CORRETA:")
    print("""
    import bcrypt

    def login_seguro(username, password):
        # Validar input
        if not username or not password:
            return None

        # Prepared statement (previne SQL Injection)
        query = "SELECT id, password_hash FROM users WHERE username = ?"
        result = db.execute(query, (username,))

        if result:
            # Verificar senha com bcrypt
            if bcrypt.checkpw(password.encode('utf-8'), result['password_hash']):
                return result

        return None
    """)

# Executar exercício
exercicio_1()

print("\n" + "=" * 70)
print("FIM DO CAPÍTULO 1")
print("=" * 70)
print("\n✅ Agora você entende os conceitos básicos de segurança web!")
print("📚 Próximo: Capítulo 2 - SQL Injection\n")
