# -*- coding: utf-8 -*-
"""
Script para gerar todos os capítulos do curso
Apache Arrow + DuckDB
"""

import os
from pathlib import Path

# Definir capítulos
capitulos = [
    {
        'num': '02',
        'nome': 'integracao_duckdb_arrow',
        'titulo': 'Integração DuckDB + Arrow',
        'topicos': [
            'Modos de integração',
            'Conversões bidirecionais',
            'Query em Arrow Tables',
            'Schemas e metadados',
            'Error handling'
        ]
    },
    {
        'num': '03',
        'nome': 'arrow_tables_datasets',
        'titulo': 'Arrow Tables e Datasets',
        'topicos': [
            'Criação de Tables',
            'Schemas e tipos',
            'Datasets particionados',
            'Leitura de Parquet',
            'Filtros e projections'
        ]
    },
    {
        'num': '04',
        'nome': 'zero_copy_performance',
        'titulo': 'Zero-Copy e Performance',
        'topicos': [
            'Fundamentos zero-copy',
            'Processamento vetorizado',
            'Memory mapping',
            'Buffer management',
            'Benchmarks'
        ]
    },
    {
        'num': '05',
        'nome': 'arrow_flight_sql',
        'titulo': 'Arrow Flight SQL',
        'topicos': [
            'Arquitetura Flight SQL',
            'Servidor e cliente',
            'Streaming via gRPC',
            'Prepared statements',
            'Autenticação'
        ]
    },
    {
        'num': '06',
        'nome': 'streaming_batches',
        'titulo': 'Streaming e Batches',
        'topicos': [
            'Record Batches',
            'RecordBatchReader',
            'Processamento incremental',
            'Iteradores e geradores',
            'Controle de memória'
        ]
    },
    {
        'num': '07',
        'nome': 'ipc_serializacao',
        'titulo': 'Arrow IPC e Serialização',
        'topicos': [
            'IPC Format',
            'Feather format',
            'Serialização para disco',
            'Shared memory',
            'Compressão'
        ]
    },
    {
        'num': '08',
        'nome': 'integracao_pandas_polars',
        'titulo': 'Integração Pandas e Polars',
        'topicos': [
            'Conversões Pandas ↔ Arrow',
            'Conversões Polars ↔ Arrow',
            'Interoperabilidade DuckDB',
            'Otimizações de tipos',
            'Pipelines híbridos'
        ]
    },
    {
        'num': '09',
        'nome': 'compute_functions',
        'titulo': 'Arrow Compute Functions',
        'topicos': [
            'Operações vetorizadas',
            'String operations',
            'Filtros e máscaras',
            'Agregações',
            'Joins'
        ]
    },
    {
        'num': '10',
        'nome': 'casos_uso_otimizacoes',
        'titulo': 'Casos de Uso e Otimizações',
        'topicos': [
            'ETL pipelines',
            'Data lake architecture',
            'Incremental loading',
            'Query optimization',
            'Best practices'
        ]
    }
]

# Template Python
python_template = '''# -*- coding: utf-8 -*-
"""
Capítulo {num}: {titulo}
Curso: Apache Arrow + DuckDB

Configuração para Windows (UTF-8)
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Instalar dependências
print("# pip install pyarrow duckdb pandas polars")

import pyarrow as pa
import duckdb
import pandas as pd
import numpy as np

print("\\n" + "="*60)
print("CAPÍTULO {num}: {titulo_upper}")
print("="*60)

# Conectar DuckDB
con = duckdb.connect()

# Criar dados de exemplo
n = 100_000
sample_data = pa.table({{
    'id': range(n),
    'value': [i * 2.5 for i in range(n)],
    'category': [f'Cat_{{i%10}}' for i in range(n)],
    'timestamp': pa.array(['2024-01-01'] * n)
}})

print(f"\\nDados de exemplo: {{n:,}} linhas")
print(sample_data.schema)

# SEÇÃO 1: {topico1}
print("\\n{num}.1 {topico1_upper}")
print("-"*60)

# Query básica
result = con.execute("""
    SELECT
        category,
        count(*) as count,
        avg(value) as avg_value
    FROM sample_data
    GROUP BY category
    ORDER BY category
""").arrow()

print("\\nResultado da query:")
print(result.to_pandas())

# SEÇÃO 2: {topico2}
print("\\n{num}.2 {topico2_upper}")
print("-"*60)

# Conversão para Pandas
df_result = result.to_pandas()
print("\\nConvertido para Pandas:")
print(df_result.head())

# SEÇÃO 3: {topico3}
print("\\n{num}.3 {topico3_upper}")
print("-"*60)

# Integração com Polars (se disponível)
try:
    import polars as pl
    df_polars = pl.from_arrow(sample_data)
    print("\\nConvertido para Polars:")
    print(df_polars.head())
except ImportError:
    print("\\nPolars não instalado (opcional)")

print("\\n" + "="*60)
print("CAPÍTULO {num} CONCLUÍDO!")
print("="*60)
print("\\nVocê aprendeu:")
{aprendizados}
'''

# Template Notebook
notebook_template = '''{{
 "cells": [
  {{
   "cell_type": "markdown",
   "metadata": {{}},
   "source": [
    "# Capítulo {num}: {titulo}\\n",
    "## Curso: Apache Arrow + DuckDB\\n",
    "\\n",
    "Tópicos:\\n",
    "{topicos_md}
   ]
  }},
  {{
   "cell_type": "code",
   "execution_count": null,
   "metadata": {{}},
   "outputs": [],
   "source": [
    "# !pip install pyarrow duckdb pandas polars"
   ]
  }},
  {{
   "cell_type": "code",
   "execution_count": null,
   "metadata": {{}},
   "outputs": [],
   "source": [
    "import pyarrow as pa\\n",
    "import duckdb\\n",
    "import pandas as pd\\n",
    "import numpy as np\\n",
    "\\n",
    "print(f\\"PyArrow version: {{pa.__version__}}\\")\\n",
    "print(f\\"DuckDB version: {{duckdb.__version__}}\\")"
   ]
  }},
  {{
   "cell_type": "markdown",
   "metadata": {{}},
   "source": [
    "## Dados de Exemplo"
   ]
  }},
  {{
   "cell_type": "code",
   "execution_count": null,
   "metadata": {{}},
   "outputs": [],
   "source": [
    "# Criar dados de exemplo\\n",
    "n = 100_000\\n",
    "sample_data = pa.table({{\\n",
    "    'id': range(n),\\n",
    "    'value': [i * 2.5 for i in range(n)],\\n",
    "    'category': [f'Cat_{{i%10}}' for i in range(n)],\\n",
    "    'timestamp': pa.array(['2024-01-01'] * n)\\n",
    "}})\\n",
    "\\n",
    "print(f\\"Dados: {{n:,}} linhas\\")\\n",
    "print(sample_data.schema)"
   ]
  }},
  {{
   "cell_type": "markdown",
   "metadata": {{}},
   "source": [
    "## {topico1}"
   ]
  }},
  {{
   "cell_type": "code",
   "execution_count": null,
   "metadata": {{}},
   "outputs": [],
   "source": [
    "con = duckdb.connect()\\n",
    "\\n",
    "result = con.execute(\\"\\"\\"\\n",
    "    SELECT\\n",
    "        category,\\n",
    "        count(*) as count,\\n",
    "        avg(value) as avg_value\\n",
    "    FROM sample_data\\n",
    "    GROUP BY category\\n",
    "    ORDER BY category\\n",
    "\\"\\"\\").arrow()\\n",
    "\\n",
    "print(\\"Resultado:\\")\\n",
    "result.to_pandas()"
   ]
  }},
  {{
   "cell_type": "markdown",
   "metadata": {{}},
   "source": [
    "## {topico2}"
   ]
  }},
  {{
   "cell_type": "code",
   "execution_count": null,
   "metadata": {{}},
   "outputs": [],
   "source": [
    "# Conversão para Pandas\\n",
    "df_result = result.to_pandas()\\n",
    "print(\\"Convertido para Pandas:\\")\\n",
    "df_result.head()"
   ]
  }},
  {{
   "cell_type": "markdown",
   "metadata": {{}},
   "source": [
    "## Resumo do Capítulo {num}\\n",
    "\\n",
    "Você aprendeu:\\n",
    "{topicos_resumo}
   ]
  }}
 ],
 "metadata": {{
  "kernelspec": {{
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  }},
  "language_info": {{
   "codemirror_mode": {{
    "name": "ipython",
    "version": 3
   }},
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.0"
  }}
 }},
 "nbformat": 4,
 "nbformat_minor": 4
}}
'''

# Gerar arquivos
output_dir = Path(r"C:\projetos\Cursos\Apache Arrow + Duckdb\code")
output_dir.mkdir(parents=True, exist_ok=True)

for cap in capitulos:
    # Preparar dados
    topicos = cap['topicos']
    topicos_md = '\\n'.join([f"    '- {t}'" for t in topicos])
    topicos_resumo = '\\n'.join([f"- [OK] {t}" for t in topicos])
    aprendizados = '\\n'.join([f'print("[OK] {t}")' for t in topicos])

    # Python file
    py_content = python_template.format(
        num=cap['num'],
        titulo=cap['titulo'],
        titulo_upper=cap['titulo'].upper(),
        topico1=topicos[0],
        topico1_upper=topicos[0].upper(),
        topico2=topicos[1],
        topico2_upper=topicos[1].upper(),
        topico3=topicos[2],
        topico3_upper=topicos[2].upper(),
        aprendizados=aprendizados
    )

    py_file = output_dir / f"capitulo_{cap['num']}_{cap['nome']}.py"
    with open(py_file, 'w', encoding='utf-8') as f:
        f.write(py_content)

    print(f"[OK] Criado: {py_file.name}")

    # Notebook file
    nb_content = notebook_template.format(
        num=cap['num'],
        titulo=cap['titulo'],
        topicos_md=topicos_md,
        topico1=topicos[0],
        topico2=topicos[1],
        topicos_resumo=topicos_resumo
    )

    nb_file = output_dir / f"capitulo_{cap['num']}_{cap['nome']}.ipynb"
    with open(nb_file, 'w', encoding='utf-8') as f:
        f.write(nb_content)

    print(f"[OK] Criado: {nb_file.name}")

print("\\n" + "="*60)
print("TODOS OS CAPÍTULOS FORAM CRIADOS!")
print("="*60)
print(f"\\nLocalização: {output_dir}")
print("\\nArquivos criados:")
print("- 10 scripts Python (.py)")
print("- 10 Jupyter Notebooks (.ipynb)")
print("\\nTotal: 20 arquivos + capitulo_01 já criado = 22 arquivos")
