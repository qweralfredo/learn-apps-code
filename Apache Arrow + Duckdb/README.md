# Apache Arrow + DuckDB: Códigos do Curso

Este diretório contém todos os códigos práticos para o curso **Apache Arrow + DuckDB**.

## 📚 Estrutura

Cada capítulo possui:
- **arquivo.py**: Script Python executável com todos os exemplos e exercícios
- **arquivo.ipynb**: Jupyter Notebook interativo para aprendizado hands-on

## 🚀 Como Usar

### Pré-requisitos

Instale as dependências necessárias:

```bash
pip install duckdb pyarrow pandas polars jupyter
```

### Executando os Scripts Python

```bash
cd "C:\projetos\Cursos\Apache Arrow + Duckdb\code"

python capitulo_01_introducao_apache_arrow.py
python capitulo_02_integracao_duckdb_arrow.py
```

## 📖 Capítulos Disponíveis

| Capítulo | Descrição | Arquivos |
|----------|-----------|----------|
| 00 | Índice do Curso | `capitulo_00_*.py/ipynb` |
| 01 | Introdução ao Apache Arrow | `capitulo_01_*.py/ipynb` |
| 02 | Integração DuckDB + Arrow | `capitulo_02_*.py/ipynb` |
| 03 | Arrow Tables e Datasets | `capitulo_03_*.py/ipynb` |
| 04 | Zero-Copy e Performance | `capitulo_04_*.py/ipynb` |
| 05 | Arrow Flight SQL | `capitulo_05_*.py/ipynb` |
| 06 | Streaming e Batches | `capitulo_06_*.py/ipynb` |
| 07 | Arrow IPC e Serialização | `capitulo_07_*.py/ipynb` |
| 08 | Integração Pandas/Polars | `capitulo_08_*.py/ipynb` |
| 09 | Arrow Compute Functions | `capitulo_09_*.py/ipynb` |
| 10 | Casos de Uso e Otimizações | `capitulo_10_*.py/ipynb` |

## 💡 Conceitos-Chave

### Apache Arrow
- Formato colunar em memória para análise de dados
- Zero-copy operations entre sistemas
- Interoperabilidade entre linguagens
- Performance excepcional

### Integração com DuckDB
- Conversão zero-copy entre Arrow e DuckDB
- Query direto em Arrow Tables
- Export de resultados como Arrow
- Streaming de grandes datasets

## 🎯 Ordem Recomendada de Estudo

1. **Capítulo 01**: Fundamentos do Apache Arrow
2. **Capítulo 02**: Como DuckDB e Arrow trabalham juntos
3. **Capítulo 03**: Arrow Tables e Datasets
4. **Capítulo 04**: Zero-Copy e otimizações de memória
5. **Capítulo 05**: Arrow Flight SQL (RPC)
6. **Capítulo 06**: Streaming e processamento em batches
7. **Capítulo 07**: Serialização IPC
8. **Capítulo 08**: Integração com Pandas/Polars
9. **Capítulo 09**: Compute Functions do Arrow
10. **Capítulo 10**: Casos de uso práticos

## 📊 Exemplos de Performance

Os códigos incluem benchmarks demonstrando:
- Zero-copy vs cópia tradicional (até 100x mais rápido)
- Arrow vs Pandas (até 10x menos memória)
- Streaming vs carregamento completo

## 📝 Notas

- Requer **PyArrow 14.0+** e **DuckDB 0.10.0+**
- Exemplos de zero-copy demonstram economia real de memória
- Benchmarks incluídos para comparação de performance
- Compatible com **Python 3.9+**

## 🐛 Troubleshooting

### PyArrow não instalado
```bash
pip install --upgrade pyarrow
```

### Problemas de memória
Os scripts usam datasets pequenos por padrão. Para testar com dados maiores, ajuste os parâmetros.

## 📚 Recursos Adicionais

- [Apache Arrow Docs](https://arrow.apache.org/docs/)
- [DuckDB Arrow Integration](https://duckdb.org/docs/guides/python/sql_on_arrow)
- [PyArrow Python API](https://arrow.apache.org/docs/python/)

---

**Curso**: Apache Arrow + DuckDB
**Nível**: Iniciante a Avançado
**Duração estimada**: 35-45 horas
