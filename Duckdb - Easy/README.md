# DuckDB - Easy: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **DuckDB - Easy**.

## 📚 Estrutura

Cada capítulo possui:
- **arquivo.py**: Script Python executável com todos os exemplos e exercícios
- **arquivo.ipynb**: Jupyter Notebook interativo para aprendizado hands-on

## 🚀 Como Usar

### Pré-requisitos

Instale as dependências necessárias:

```bash
pip install duckdb pandas jupyter
```

### Executando os Scripts Python

```bash
# Navegar até o diretório
cd "C:\projetos\Cursos\Duckdb - Easy\code"

# Executar um capítulo específico
python capitulo_01_introducao_sql.py
python capitulo_02_instalacao_configuracao.py
python capitulo_03_importacao_exportacao_csv.py
```

### Executando os Jupyter Notebooks

```bash
# Iniciar Jupyter Lab
jupyter lab

# Ou Jupyter Notebook
jupyter notebook
```

Depois, abra o arquivo `.ipynb` desejado no navegador.

## 📖 Capítulos Disponíveis

| Capítulo | Descrição | Arquivos |
|----------|-----------|----------|
| 01 | Introdução ao SQL no DuckDB | `capitulo_01_*.py/ipynb` |
| 02 | Instalação e Configuração | `capitulo_02_*.py/ipynb` |
| 03 | Importação/Exportação CSV | `capitulo_03_*.py/ipynb` |
| 04 | Trabalhando com Parquet | `capitulo_04_*.py/ipynb` |
| 05 | Importação/Exportação JSON | `capitulo_05_*.py/ipynb` |
| 06 | Integração com Python | `capitulo_06_*.py/ipynb` |
| 07 | Tipos de Dados DuckDB | `capitulo_07_*.py/ipynb` |
| 08 | Consultas em Arquivos Remotos | `capitulo_08_*.py/ipynb` |
| 09 | Meta Queries | `capitulo_09_*.py/ipynb` |
| 10 | Performance e Boas Práticas | `capitulo_10_*.py/ipynb` |

## 💡 Dicas

### Executar Todos os Testes

```bash
# Windows
for %f in (capitulo_*.py) do python "%f"

# Linux/Mac
for file in capitulo_*.py; do python "$file"; done
```

### Limpar Arquivos Temporários

Os scripts criam arquivos temporários (`.db`, `.csv`, `.parquet`, `.json`) durante a execução. Eles são automaticamente removidos, mas caso queira limpar manualmente:

```bash
# Windows
del *.db *.csv *.parquet *.json

# Linux/Mac
rm -f *.db *.csv *.parquet *.json
```

## 🎯 Ordem Recomendada de Estudo

1. **Capítulo 01**: Fundamentos de SQL
2. **Capítulo 02**: Setup e Configuração
3. **Capítulo 03**: Trabalhar com CSV
4. **Capítulo 04**: Trabalhar com Parquet (formato recomendado)
5. **Capítulo 05**: Trabalhar com JSON
6. **Capítulo 06**: Integração Python/Pandas
7. **Capítulo 07**: Entender tipos de dados
8. **Capítulo 08**: Acessar dados remotos
9. **Capítulo 09**: Meta-informações do banco
10. **Capítulo 10**: Otimizações e performance

## 📝 Notas

- Todos os scripts são **auto-contidos** e podem ser executados independentemente
- Os exercícios práticos estão incluídos no final de cada script
- Dados de exemplo são criados programaticamente (não precisam de arquivos externos)
- Compatible com **Python 3.9+** e **DuckDB 0.10.0+**

## 🐛 Troubleshooting

### Erro de Encoding no Windows

Se você encontrar erros de encoding, os scripts já incluem configuração automática UTF-8.

### DuckDB não instalado

```bash
pip install --upgrade duckdb
```

### Jupyter não inicia

```bash
pip install --upgrade jupyter jupyterlab
```

## 📚 Recursos Adicionais

- [Documentação Oficial DuckDB](https://duckdb.org/docs/)
- [DuckDB Python API](https://duckdb.org/docs/api/python/overview)
- [SQL Reference](https://duckdb.org/docs/sql/introduction)

---

**Curso**: DuckDB - Easy
**Autor**: Curso completo com códigos práticos
**Versão**: 1.0
