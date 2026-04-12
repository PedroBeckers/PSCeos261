# 📊 Consulta de Dados do CNPJ

Desenvolvido como desafio do processo seletivo do Projeto Céos 26.1, este projeto tem com o objetivo de construir uma solução completa de coleta, processamento, ingestão e visualização de dados públicos do CNPJ.

---

## 🎯 Objetivo

Desenvolver uma solução completa para tratamento de dados do CNPJ, contemplando:

- coleta de arquivos oficiais disponibilizados em formato CSV compactado  
- processamento e normalização dos dados, incluindo tratamento de inconsistências de encoding e estrutura  
- ingestão em um banco de dados analítico para consulta eficiente  
- disponibilização de uma interface web para exploração e visualização dos dados  

---

## 🏗️ Arquitetura do Projeto

O sistema foi dividido em três etapas principais:

### 1. Coleta (Collector)

- Identifica arquivos `.zip` em `data/raw`
- Processa **um lote por vez**
- Realiza a coleta dos dados conforme a estrutura hierárquica definida pela base oficial da Receita Federal
- Extrai os arquivos do escopo do enunciado (empresas, estabelecimentos, sócios, CNAEs, naturezas jurídicas e municípios) em `data/staged`
- Evita reextrações pesadas através do controle dos diretórios `data`

### 2. Processamento (Processor)

- Evita reprocessamento através da tabela de controle processed_files
- Classifica arquivos por entidade:
  - empresas
  - estabelecimentos
  - sócios
  - CNAEs
  - naturezas jurídicas
  - municípios
- Normaliza arquivos para **UTF-8**
- Prepara dados para ingestão

### 3. Ingestão (Ingestor)

- Realiza o parse dos arquivos de maneira eficiente com o **DuckDB** 
- Carrega dados no **DuckDB**
- Controle de exceções sem interromper a ingestão do lote:
  - `INSERT OR IGNORE` → evita linhas comuns a diferentes snapshots
  - `ignore_errors=true` → ignora linhas corrompidas sem interromper a ingestão do lote
- Garante robustez mesmo com dados inconsistentes

---

## ⚙️ Processamento em Lotes

O pipeline foi projetado para trabalhar com **processamento incremental**:

- um lote é representado por um arquivo compactado no formato `AAAA-MM.zip` do banco, por exemplo `2026-03.zip`
- um lote é processado por vez
- após ingestão:
  - o lote é removido de `raw` e `staged` é limpo
  - o `.zip` é movido para `processed_raw`

Isso permite:

- retomada após falhas
- menor uso de memória e disco
- melhor controle do fluxo
- armazenar o histórico de lotes processados

---

## 🧠 Decisões Técnicas

### DuckDB

Escolhido por:

- alto desempenho em leitura analítica
- simplicidade de uso
- suporte a grandes volumes de dados locais

---

### Tratamento de Dados Problemáticos

A base oficial contém inconsistências, como:

- encoding mal definido
- linhas CSV malformadas
- duplicidade entre snapshots

Soluções adotadas:

- normalização para UTF-8
- uso de `ignore_errors=true` para linhas inválidas
- uso de `INSERT OR IGNORE` para garantir idempotência

---

### Idempotência

O pipeline pode ser executado múltiplas vezes sem corromper os dados:

- registros duplicados são ignorados
- arquivos parcialmente ingeridos podem ser reprocessados com segurança

---

## 🌐 Interface Web

A interface foi desenvolvida com **Streamlit** e permite:

### 🔎 Buscar empresas por razão social
- busca textual com `LIKE`
- exibe:
  - CNPJ básico
  - razão social
  - natureza jurídica
  - porte
  - capital social

---

### 🏢 Buscar estabelecimentos por CNPJ
- entrada de CNPJ completo (14 dígitos)
- exibe:
  - dados da empresa associada
  - estabelecimentos vinculados
  - sócios 

---

### 🔍 Buscar estabelecimentos com filtros

Filtros disponíveis:

- UF
- município
- situação cadastral
- CNAE principal

---

## 📦 Estrutura do Projeto

    app/
      core/
      database/
      pipeline/
      web/

    data/
      raw/
      staged/
      processed_raw/
      db/

---

## 📥 Obtenção dos Dados

Devido ao grande volume dos dados disponibilizados pela Receita Federal (arquivos que podem ultrapassar gigabytes), não foi implementado um mecanismo de download automático no projeto.

Essa decisão foi tomada para:

- evitar longos tempos de execução durante testes  
- não sobrecarregar o repositório com arquivos pesados  
- manter o projeto leve e portátil  

### ▶️ Como testar a aplicação

Para executar o pipeline, é necessário baixar manualmente um lote de dados do CNPJ:

1. Acesse a base oficial da Receita Federal  
2. Baixe um arquivo `.zip` referente a um snapshot (ex: `2026-03.zip`)  
3. Coloque o arquivo na pasta:

```
data/raw/
```

4. Execute o pipeline normalmente:

```
python3 -m app.main pipeline
```

O sistema irá:

- extrair o lote  
- processar os arquivos relevantes  
- ingerir os dados no banco  
- disponibilizar para consulta na interface  

Após a ingestão dos lotes, execute a interface web:

```
python3 -m app.main web
```

A interface será iniciada e estará disponível no navegador para consulta dos dados.

---

## 📈 Observabilidade

O sistema registra:

- arquivos processados
- erros de ingestão
- tempo por lote (ex: `12m 47s`)

---

## ⚠️ Considerações

- os dados podem conter inconsistências provenientes da fonte oficial  
- algumas linhas podem ser descartadas durante a ingestão devido a erros estruturais  

---

## 🚀 Possíveis melhorias

- aprimoramento da paginação e da experiência de navegação na interface  

---

## 📌 Conclusão

O projeto atende aos requisitos do desafio, entregando:

- pipeline robusto de ingestão
- armazenamento eficiente com DuckDB
- interface web funcional para consulta

Além disso, incorpora práticas importantes como:

- processamento em lotes
- Resistente a interrupções de processamento com possibilidade de retomada
- separação de responsabilidades

---

## 👤 Autor

Projeto desenvolvido por **Pedro Becker**