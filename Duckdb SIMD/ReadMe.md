Aqui estão exemplos práticos e conceituais para cada um dos 10 capítulos propostos, ilustrando como o DuckDB implementa essas otimizações internamente.

---

### Capítulo 1: O Fim do Modelo "Volcano"
**Exemplo:** *A Soma de Colunas*
Imagine uma consulta `SELECT a + b FROM table`.

*   **Modelo Volcano (Postgres/MySQL clássico):**
    ```cpp
    // Pseudo-código: Loop externo chama função virtual por linha
    while (row = iterator.next()) {
        result = row.get("a") + row.get("b"); // Overhead de chamada de função + Cache Miss
    }
    ```
*   **Modelo Vetorizado (DuckDB):**
    ```cpp
    // Pseudo-código: Loop interno processa array (SIMD friendly)
    // O compilador traduz isso para instruções vetorizadas automaticamente
    for (int i = 0; i < 1024; i++) {
        result_vector[i] = vector_a[i] + vector_b[i];
    }
    ```
    *Resultado:* O DuckDB gasta a maior parte do tempo calculando, enquanto o modelo Volcano gasta tempo alternando entre funções.

---

### Capítulo 2: Anatomia de um `DataChunk`
**Exemplo:** *Compressão Lógica de Constantes*
Cenário: Uma tabela com 1 milhão de linhas onde a coluna `status` é sempre "ACTIVE".

*   **Representação Padrão:** Alocação de um array com 1 milhão de strings "ACTIVE".
*   **DuckDB `ConstantVector`:**
    *   O `DataChunk` carrega um lote de 1024 linhas.
    *   Ele detecta que todos os valores são iguais.
    *   Em vez de alocar memória para 1024 strings, ele define o tipo do vetor como `CONSTANT` e aponta para **um único valor** "ACTIVE" na memória.
    *   Qualquer operação de leitura nesse vetor sempre retorna o índice 0, reduzindo o uso de RAM drasticamente.

---

### Capítulo 3: Selection Vectors
**Exemplo:** *Filtragem sem Cópia*
Consulta: `SELECT * FROM users WHERE age > 18` em um bloco de 4 pessoas.

*   **Dados Originais (Flat Vector):** `[10, 25, 15, 30]` (índices 0, 1, 2, 3).
*   **Operação:** O filtro gera um **Selection Vector**.
*   **Resultado (Selection Vector):** `[1, 3]` (aponta para 25 e 30).
*   **Otimização:** O DuckDB **não** cria um novo vetor de dados contendo `[25, 30]`. Ele passa o vetor original `[10, 25, 15, 30]` adiante, mas anexa a lista de seleção `[1, 3]`. A próxima operação saberá que deve ignorar os índices 0 e 2.
    *   *Ganho:* Zero movimentação de dados (memcopy) pesada.

---

### Capítulo 4: Primitivas SIMD e Execução Branchless
**Exemplo:** *Eliminando o `if`*
Código SQL: `SELECT CASE WHEN val > 50 THEN 1 ELSE 0 END ...`

*   **Com Branch (Lento):**
    ```cpp
    if (val[i] > 50) res[i] = 1; else res[i] = 0; // CPU tenta adivinhar o caminho (Branch Prediction)
    ```
*   **Sem Branch (DuckDB/SIMD):**
    Usa instruções de comparação que geram uma máscara de bits (0 ou 1) diretamente no registrador.
    ```cpp
    // Pseudo-instrução SIMD
    mask = _mm256_cmp_ps(val, threshold, _CMP_GT_OQ); // Gera vetor de 0s e 1s
    result = _mm256_and_ps(mask, ones); // Aplica a máscara
    ```
    *Resultado:* O fluxo de instruções é linear, sem saltos na CPU, evitando "pipeline flushes".

---

### Capítulo 5: Avaliação de Expressões
**Exemplo:** *A Árvore de Expressão*
Consulta: `(colA + colB) * colC`

*   O DuckDB não compila isso para código de máquina (JIT) na hora. Ele tem funções pré-compiladas C++ para `VectorAdd` e `VectorMultiply`.
*   **Passo 1:** Chama `VectorAdd(colA, colB)` para o bloco de 1024 tuplas. O resultado vai para um vetor temporário `Tmp1` (que fica no Cache L1/L2).
*   **Passo 2:** Chama `VectorMultiply(Tmp1, colC)`.
*   *Por que é rápido?* Porque os dados de `Tmp1` ainda estão "quentes" no cache da CPU quando o passo 2 começa.

---

### Capítulo 6: Agregações Hash em Paralelo
**Exemplo:** *Colisão de Hash com Scatter/Gather*
Operação: `GROUP BY id` construindo uma Hash Table.

1.  **Cálculo de Hash:** Usa SIMD para calcular o hash de 1024 chaves de uma vez.
2.  **Scatter (Espalhar):** As chaves calculadas apontam para locais de memória aleatórios na Tabela Hash. CPUs normais odeiam isso.
3.  **Otimização DuckDB:** Usa instruções AVX-512 `vscatter` (se disponível) ou agrupa acessos para minimizar cache misses, escrevendo as contagens de agregação em paralelo onde possível, usando máscaras para lidar com colisões (quando dois itens caem no mesmo bucket).

---

### Capítulo 7: Vetorização de Joins
**Exemplo:** *Prefetching (Escondendo Latência)*
Durante um Hash Join (Probe phase):

*   Temos um vetor de chaves de busca entrando.
*   Em vez de buscar a chave 1, esperar a RAM, buscar a chave 2...
*   **Pipeline DuckDB:**
    1.  Calcula hashes para as linhas 0-7.
    2.  Emite instruções de **Prefetch** (`_mm_prefetch`) para as linhas 0-7 (diz para a RAM: "Traga esses dados para o Cache, vou usar logo").
    3.  Enquanto a memória busca os dados de 0-7, a CPU processa a lógica de Join das linhas que foram pré-carregadas no ciclo anterior.
    *Resultado:* A CPU nunca fica parada esperando dados da memória RAM.

---

### Capítulo 8: Operações em Strings (German Strings)
**Exemplo:** *Comparação Rápida de Strings Longas*
Comparar: `"EngenhariaDeDados"` = `"EngenhariaDeSoftware"`

*   **String C++ Padrão:** Segue ponteiro -> compara byte a byte -> descobre diferença no final. Lento.
*   **DuckDB String View (German Style):**
    *   Estrutura (16 bytes): `[4 bytes tamanho] [4 bytes prefixo] [8 bytes ponteiro/inline]`
    *   A string armazena "Enge" (os 4 primeiros bytes) diretamente na estrutura principal.
*   **Comparação:**
    1.  Compara tamanho. (Iguais).
    2.  Compara prefixo (inteiro de 32 bits). `"Enge"` vs `"Enge"`. (Iguais).
    3.  Só então segue o ponteiro para comparar o resto.
    *Cenário:* Se fosse `"Banana"` vs `"Bacana"`, o prefixo (`"Bana"` vs `"Baca"`) já daria `False` instantaneamente sem nunca acessar a memória heap (ponteiro), economizando Cache Misses.

---

### Capítulo 9: Scanners Vetorizados
**Exemplo:** *Lendo Parquet Inteiros*
Arquivo Parquet usa *Bit-Packing*: Inteiros de 0 a 7 ocupam apenas 3 bits cada, compactados.

*   **Leitura Linha a Linha:** Ler byte -> extrair bits -> shift -> próxima linha.
*   **Leitura Vetorizada:**
    *   O Scanner carrega 128 bits do arquivo (contendo ~42 inteiros de 3 bits).
    *   Usa uma instrução SIMD especial (shuffle) que "explode" esses bits compactados diretamente para 42 inteiros de 32 bits em registradores SIMD.
    *   Isso permite ler e descomprimir Gigabytes por segundo, limitado apenas pela largura de banda da memória, não pela CPU.

---

### Capítulo 10: Cache Locality e TLB
**Exemplo:** *O "Número Mágico" 2048*
Por que o DuckDB processa (geralmente) blocos de ~2048 linhas e não 1 milhão?

*   **Cálculo:**
    *   3 colunas de inteiros (4 bytes) x 2048 linhas = 24 KB.
    *   Cache L1 de uma CPU moderna = 32 KB ou 48 KB.
*   **Cenário:** O bloco inteiro de dados e os vetores auxiliares cabem inteiramente no Cache L1 (o mais rápido).
*   Se o bloco fosse de 100.000 linhas, os dados transbordariam para o L2/L3 ou RAM principal durante o processamento da consulta, fazendo a CPU esperar ("stall") para buscar dados frios novamente. O tamanho do vetor é ajustado para manter os dados "próximos" da CPU.