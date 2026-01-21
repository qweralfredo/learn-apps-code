# -*- coding: utf-8 -*-
"""
Script para criar todos os capítulos do curso Apache Arrow + DuckDB
"""

import os
import json
from pathlib import Path

# Definir estrutura dos capítulos
capitulos_info = [
    ("02", "integracao_duckdb_arrow", "Integração DuckDB + Arrow"),
    ("03", "arrow_tables_datasets", "Arrow Tables e Datasets"),
    ("04", "zero_copy_performance", "Zero-Copy e Performance"),
    ("05", "arrow_flight_sql", "Arrow Flight SQL"),
    ("06", "streaming_batches", "Streaming e Batches"),
    ("07", "ipc_serializacao", "Arrow IPC e Serialização"),
    ("08", "integracao_pandas_polars", "Integração Pandas e Polars"),
    ("09", "compute_functions", "Arrow Compute Functions"),
    ("10", "casos_uso_otimizacoes", "Casos de Uso e Otimizações")
]

output_dir = Path(r"C:\projetos\Cursos\Apache Arrow + Duckdb\code")

# Criar arquivos Python
for num, nome, titulo in capitulos_info:
    # Criar arquivo .py
    py_content = f'''# -*- coding: utf-8 -*-
"""
Capítulo {num}: {titulo}
Curso: Apache Arrow + DuckDB
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pyarrow as pa
import duckdb
import pandas as pd
import numpy as np

print("="*60)
print("CAPÍTULO {num}: {titulo.upper()}")
print("="*60)

# Criar dados de exemplo
n = 100_000
data = pa.table({{
    'id': range(n),
    'value': [i * 2.5 for i in range(n)],
    'category': [f'Cat_{{i%10}}' for i in range(n)]
}})

print(f"\\nDados: {{n:,}} linhas")

# Conectar DuckDB
con = duckdb.connect()

# Query de exemplo
result = con.execute("""
    SELECT
        category,
        count(*) as count,
        avg(value) as avg_value,
        sum(value) as sum_value
    FROM data
    GROUP BY category
    ORDER BY category
""").arrow()

print("\\nResultado:")
print(result.to_pandas())

print("\\n" + "="*60)
print("CAPÍTULO {num} CONCLUÍDO")
print("="*60)
'''

    py_file = output_dir / f"capitulo_{num}_{nome}.py"
    with open(py_file, 'w', encoding='utf-8') as f:
        f.write(py_content)
    print(f"OK: {py_file.name}")

    # Criar arquivo .ipynb
    nb_cells = [
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": [
                f"# Capítulo {num}: {titulo}\\n",
                "## Curso: Apache Arrow + DuckDB"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "# !pip install pyarrow duckdb pandas"
            ]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "import pyarrow as pa\\n",
                "import duckdb\\n",
                "import pandas as pd\\n",
                "import numpy as np\\n",
                "\\n",
                f"print('Capítulo {num}: {titulo}')"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## Dados de Exemplo"]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "n = 100_000\\n",
                "data = pa.table({\\n",
                "    'id': range(n),\\n",
                "    'value': [i * 2.5 for i in range(n)],\\n",
                "    'category': [f'Cat_{i%10}' for i in range(n)]\\n",
                "})\\n",
                "\\n",
                "print(f'Dados: {n:,} linhas')\\n",
                "print(data.schema)"
            ]
        },
        {
            "cell_type": "markdown",
            "metadata": {},
            "source": ["## Query com DuckDB"]
        },
        {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [
                "con = duckdb.connect()\\n",
                "\\n",
                "result = con.execute('''\\n",
                "    SELECT\\n",
                "        category,\\n",
                "        count(*) as count,\\n",
                "        avg(value) as avg_value\\n",
                "    FROM data\\n",
                "    GROUP BY category\\n",
                "    ORDER BY category\\n",
                "''').arrow()\\n",
                "\\n",
                "result.to_pandas()"
            ]
        }
    ]

    notebook = {
        "cells": nb_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

    nb_file = output_dir / f"capitulo_{num}_{nome}.ipynb"
    with open(nb_file, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=1)
    print(f"[OK] {nb_file.name}")

print("\\n" + "="*60)
print("TODOS OS CAPÍTULOS CRIADOS COM SUCESSO!")
print("="*60)
print(f"\\nLocalização: {output_dir}")
print("Total de arquivos: 18 (9 .py + 9 .ipynb)")
