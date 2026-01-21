# Cyber Security - Ana: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **Cyber Security - Ana**.

## ⚠️ AVISO IMPORTANTE

Estes códigos são **EXCLUSIVAMENTE EDUCACIONAIS**. Demonstram vulnerabilidades comuns e suas correções.

**NUNCA** use técnicas de ataque para fins maliciosos ou ilegais.

## 📚 Estrutura

Cada capítulo possui:
- **arquivo.py**: Script demonstrando vulnerabilidade e correção
- **arquivo.ipynb**: Jupyter Notebook interativo

## 🚀 Como Usar

### Pré-requisitos

```bash
pip install flask sqlite3 hashlib jupyter cryptography
```

### Executando os Exemplos

```bash
cd "C:\projetos\Cursos\Cyber Security - Ana\code"

python capitulo_01_introducao_seguranca_web.py
```

## 📖 Capítulos Disponíveis

| Capítulo | Vulnerabilidade | Arquivos |
|----------|-----------------|----------|
| 01 | Introdução à Segurança Web | `capitulo_01_*.py/ipynb` |
| 02 | SQL Injection | `capitulo_02_*.py/ipynb` |
| 03 | Cross-Site Scripting (XSS) | `capitulo_03_*.py/ipynb` |
| 04 | Cross-Site Request Forgery (CSRF) | `capitulo_04_*.py/ipynb` |
| 05 | Autenticação e Autorização | `capitulo_05_*.py/ipynb` |
| 06 | Controle de Acesso | `capitulo_06_*.py/ipynb` |
| 07 | Segurança de APIs | `capitulo_07_*.py/ipynb` |
| 08 | Criptografia e Proteção de Dados | `capitulo_08_*.py/ipynb` |
| 09 | Configurações Seguras | `capitulo_09_*.py/ipynb` |
| 10 | Checklist e Próximos Passos | `capitulo_10_*.py/ipynb` |

## 🎯 Formato dos Códigos

Cada script segue este formato:

```python
# ❌ VULNERÁVEL - NÃO USE EM PRODUÇÃO
def codigo_vulneravel():
    # Código inseguro demonstrando a vulnerabilidade
    pass

# ✅ SEGURO - FORMA CORRETA
def codigo_seguro():
    # Código corrigido e seguro
    pass

# Demonstração lado a lado
```

## 💡 Conceitos-Chave

### OWASP Top 10
Os códigos cobrem as principais vulnerabilidades:
1. **Injection** (SQL, Command)
2. **Broken Authentication**
3. **Sensitive Data Exposure**
4. **XML External Entities (XXE)**
5. **Broken Access Control**
6. **Security Misconfiguration**
7. **Cross-Site Scripting (XSS)**
8. **Insecure Deserialization**
9. **Using Components with Known Vulnerabilities**
10. **Insufficient Logging & Monitoring**

## 🎯 Ordem Recomendada

1. **Cap 01**: Introdução e OWASP Top 10
2. **Cap 02**: SQL Injection (mais comum)
3. **Cap 03**: XSS (Cross-Site Scripting)
4. **Cap 04**: CSRF
5. **Cap 05**: Autenticação segura
6. **Cap 06**: Controle de acesso
7. **Cap 07**: APIs seguras
8. **Cap 08**: Criptografia
9. **Cap 09**: Configurações seguras
10. **Cap 10**: Checklist final

## 📝 Exemplos Práticos

### SQL Injection

```python
# ❌ VULNERÁVEL
def buscar_usuario_vulneravel(username):
    query = f"SELECT * FROM users WHERE username = '{username}'"
    # Permite: username = "admin' OR '1'='1"
    return db.execute(query)

# ✅ SEGURO
def buscar_usuario_seguro(username):
    query = "SELECT * FROM users WHERE username = ?"
    return db.execute(query, (username,))
```

### XSS

```python
# ❌ VULNERÁVEL
@app.route('/search')
def search_vulnerable():
    query = request.args.get('q')
    return f"<h1>Resultados para: {query}</h1>"

# ✅ SEGURO
from markupsafe import escape

@app.route('/search')
def search_secure():
    query = request.args.get('q')
    return f"<h1>Resultados para: {escape(query)}</h1>"
```

### Senha Segura

```python
# ❌ VULNERÁVEL
def salvar_senha_vulneravel(senha):
    # NUNCA faça isso!
    return senha  # Texto plano

# ✅ SEGURO
import hashlib
import os

def salvar_senha_segura(senha):
    salt = os.urandom(32)
    hash = hashlib.pbkdf2_hmac('sha256',
        senha.encode('utf-8'), salt, 100000)
    return salt + hash
```

## 🔒 Boas Práticas

### ✅ Sempre Faça
- Validar e sanitizar inputs
- Usar prepared statements
- Escapar outputs
- Hashear senhas (bcrypt, Argon2)
- Implementar HTTPS
- Usar tokens CSRF
- Logging adequado

### ❌ Nunca Faça
- Confiar em inputs do usuário
- Armazenar senhas em texto plano
- Usar MD5 ou SHA1 para senhas
- Expor mensagens de erro detalhadas
- Desabilitar validações em produção

## 🛠️ Ferramentas Úteis

Os códigos demonstram uso de:
- **OWASP ZAP**: Scanner de vulnerabilidades
- **Burp Suite**: Testing de segurança
- **SQLMap**: Teste de SQL injection
- **Bandit**: Análise estática de código Python

## ⚖️ Responsabilidade Legal

- Teste APENAS em sistemas próprios ou autorizados
- Nunca teste vulnerabilidades em produção sem permissão
- Respeite leis locais sobre segurança cibernética
- Use conhecimento para DEFENDER, não atacar

## 📚 Recursos Adicionais

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [OWASP Cheat Sheets](https://cheatsheetseries.owasp.org/)
- [Web Security Academy](https://portswigger.net/web-security)

---

**Curso**: Cyber Security - Ana
**Nível**: Iniciante a Intermediário
**Objetivo**: Aprender a DEFENDER sistemas web
