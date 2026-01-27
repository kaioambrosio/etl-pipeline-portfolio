# 🔄 ETL Pipeline - Portfolio Project

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

**Pipeline ETL profissional para processamento de dados financeiros**

```
Excel/CSV  →  Python (Pandas)  →  PostgreSQL  →  Power BI
```

---

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Tecnologias](#-tecnologias)
- [Arquitetura](#-arquitetura)
- [Instalação](#-instalação)
- [Como Usar](#-como-usar)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Modelagem de Dados](#-modelagem-de-dados)
- [Funcionalidades](#-funcionalidades)
- [Autor](#-autor)

---

## 🎯 Sobre o Projeto

Este projeto demonstra a construção de um **pipeline ETL completo e profissional** aplicado a um cenário real de negócio. O objetivo é transformar dados brutos de transações financeiras em informação confiável e visualmente analisável.

### Objetivos

- ✅ Demonstrar domínio em **Python aplicado a dados**
- ✅ Evidenciar conhecimento em **SQL e modelagem relacional**
- ✅ Aplicar conceitos de **automação e logging**
- ✅ Criar um projeto **facilmente explicável em entrevistas**

### Cenário de Negócio

Processamento de transações financeiras (vendas/cobrança) com:
- Ingestão de múltiplos arquivos CSV/Excel
- Tratamento e padronização de dados
- Persistência em banco relacional
- Visualização em dashboards analíticos

---

## 🛠 Tecnologias

| Categoria | Tecnologia | Uso |
|-----------|------------|-----|
| **Linguagem** | Python 3.12+ | Motor de processamento |
| **Dados** | Pandas, NumPy | Manipulação e análise |
| **Banco** | PostgreSQL | Persistência relacional |
| **ORM** | SQLAlchemy | Abstração de banco |
| **Validação** | Pydantic | Validação de dados |
| **Logging** | Loguru | Sistema de logs |
| **Visualização** | Power BI | Dashboards analíticos |
| **Testes** | Pytest | Testes automatizados |

---

## 🏗 Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌──────────┐    ┌──────────────┐    ┌───────────────────┐    │
│   │  SOURCE  │    │   EXTRACT    │    │    TRANSFORM      │    │
│   │──────────│    │──────────────│    │───────────────────│    │
│   │ CSV      │───▶│ Leitura      │───▶│ Padronização      │    │
│   │ Excel    │    │ Validação    │    │ Limpeza           │    │
│   │ (.xlsx)  │    │ Hash MD5     │    │ Campos derivados  │    │
│   └──────────┘    └──────────────┘    └─────────┬─────────┘    │
│                                                 │               │
│                                                 ▼               │
│   ┌──────────┐    ┌──────────────┐    ┌───────────────────┐    │
│   │  OUTPUT  │    │   CONSUME    │    │      LOAD         │    │
│   │──────────│    │──────────────│    │───────────────────│    │
│   │ Power BI │◀───│ Views SQL    │◀───│ Bulk Insert       │    │
│   │ Reports  │    │ Agregações   │    │ Controle duplicatas│   │
│   └──────────┘    └──────────────┘    │ Logging           │    │
│                                       └───────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Instalação

### Pré-requisitos

- Python 3.12 ou superior
- PostgreSQL 15 ou superior
- Git

### Passo a Passo

1. **Clone o repositório**
```bash
git clone https://github.com/KaioAmbrosio/etl-pipeline-portfolio.git
cd etl-pipeline-portfolio
```

2. **Crie o ambiente virtual**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **Instale as dependências**
```bash
pip install -r requirements.txt
```

4. **Configure as variáveis de ambiente**
```bash
# Copie o arquivo de exemplo
cp .env.example .env

# Edite o arquivo .env com suas configurações
```

5. **Configure o banco de dados PostgreSQL**
```sql
-- Crie o banco de dados
CREATE DATABASE etl_portfolio;

-- Execute o schema (opcional, o pipeline cria automaticamente)
psql -d etl_portfolio -f sql/schema.sql
```

---

## 📖 Como Usar

### Gerar Dados de Exemplo

```bash
# Gera 1000 registros de exemplo
python scripts/main.py --generate-sample

# Gera quantidade específica
python scripts/main.py -g -n 5000
```

### Executar o Pipeline

```bash
# Processa todos os arquivos em data/raw
python scripts/main.py

# Processa arquivo específico
python scripts/main.py data/raw/transacoes.csv
```

### Exemplo de Saída

```
============================================================
🚀 INICIANDO PIPELINE ETL
Data/Hora: 2024-01-25 10:30:00
============================================================
Arquivos a processar: 1

📁 Arquivo 1/1: transacoes_exemplo.csv
----------------------------------------
📥 ETAPA 1: Extração
✓ Extraídos 1000 registros
🔄 ETAPA 2: Transformação
✓ Transformados: 1000 → 987 registros
📤 ETAPA 3: Carga
✓ Carregados: 987 inseridos, 0 ignorados
----------------------------------------
✓ Arquivo processado com sucesso em 2.34s

============================================================
📊 RESUMO DO PIPELINE
============================================================
Arquivos processados: 1/1
Total de registros extraídos: 1000
Total de registros carregados: 987
Tempo total: 2.34s
============================================================
✅ Pipeline concluído com sucesso!
```

---

## 📁 Estrutura do Projeto

```
etl-pipeline-portfolio/
│
├── 📂 config/                    # Configurações
│   ├── __init__.py
│   └── settings.py               # Gerenciamento de configurações
│
├── 📂 data/                      # Dados
│   ├── raw/                      # Dados brutos (entrada)
│   └── processed/                # Dados processados
│
├── 📂 scripts/                   # Scripts ETL
│   ├── __init__.py
│   ├── extract.py                # Módulo de extração
│   ├── transform.py              # Módulo de transformação
│   ├── load.py                   # Módulo de carga
│   ├── models.py                 # Modelos SQLAlchemy
│   └── main.py                   # Orquestrador principal
│
├── 📂 sql/                       # Scripts SQL
│   └── schema.sql                # Schema do banco de dados
│
├── 📂 dashboard/                 # Arquivos Power BI
│   └── etl_dashboard.pbix
│
├── 📂 logs/                      # Logs de execução
│   └── etl.log
│
├── 📂 tests/                     # Testes automatizados
│   └── __init__.py
│
├── .env                          # Variáveis de ambiente (não versionado)
├── .env.example                  # Exemplo de variáveis
├── .gitignore                    # Arquivos ignorados pelo Git
├── requirements.txt              # Dependências Python
└── README.md                     # Este arquivo
```

---

## 🗃 Modelagem de Dados

### Diagrama ER

```
┌─────────────────────┐     ┌─────────────────────┐
│     transacoes      │     │      logs_etl       │
├─────────────────────┤     ├─────────────────────┤
│ id (PK)             │     │ id_log (PK)         │
│ id_transacao        │     │ data_execucao       │
│ data_transacao      │     │ arquivo_processado  │
│ cliente             │     │ qtd_registros_*     │
│ produto             │     │ status_execucao     │
│ categoria           │     │ tempo_execucao_seg  │
│ valor               │     │ mensagem_erro       │
│ status_pagamento    │     │ detalhes (JSON)     │
│ data_pagamento      │     └─────────────────────┘
│ ano_transacao       │
│ mes_transacao       │     ┌─────────────────────┐
│ dia_semana          │     │arquivos_processados │
│ trimestre           │     ├─────────────────────┤
│ arquivo_origem      │     │ id (PK)             │
│ data_processamento  │     │ nome_arquivo        │
└─────────────────────┘     │ hash_arquivo        │
                            │ data_processamento  │
                            │ id_log_etl (FK)     │
                            └─────────────────────┘
```

### Views Analíticas

- **`vw_resumo_transacoes`**: Agregação por ano/mês/categoria
- **`vw_status_etl`**: Monitoramento diário do pipeline

---

## ✨ Funcionalidades

### Extração (Extract)
- ✅ Leitura de arquivos CSV e Excel (.xlsx, .xls)
- ✅ Detecção automática de encoding
- ✅ Validação de estrutura obrigatória
- ✅ Cálculo de hash MD5 para controle

### Transformação (Transform)
- ✅ Padronização de nomes de colunas (snake_case)
- ✅ Conversão de tipos de dados
- ✅ Tratamento de valores nulos
- ✅ Normalização de status de pagamento
- ✅ Criação de campos derivados (ano, mês, trimestre)
- ✅ Remoção de duplicatas
- ✅ Validação de qualidade dos dados

### Carga (Load)
- ✅ Criação automática de tabelas
- ✅ Inserção em lote (bulk insert)
- ✅ Controle de duplicatas (upsert)
- ✅ Rastreamento de arquivos processados
- ✅ Logging completo de execução

### Infraestrutura
- ✅ Configuração via variáveis de ambiente
- ✅ Sistema de logging rotativo
- ✅ Tratamento de erros robusto
- ✅ Código modular e testável

---

## 📊 Power BI

Após executar o pipeline, conecte o Power BI ao PostgreSQL para criar dashboards.

### Conexão Rápida

1. **Power BI Desktop** → **Obter Dados** → **PostgreSQL**
2. **Servidor**: `localhost` | **Banco**: `etl_portfolio`
3. Selecione as views otimizadas

### Views Disponíveis

| View | Descrição |
|------|-----------|
| `vw_fato_transacoes` | Tabela fato principal com todas as dimensões |
| `vw_kpi_resumo` | KPIs para cards do dashboard |
| `vw_analise_mensal` | Dados para gráficos de tendência |
| `vw_top_produtos` | Ranking de produtos por valor/quantidade |
| `vw_resumo_transacoes` | Resumo agregado por período |

### Dashboards Sugeridos

- **Executivo**: KPIs, tendência mensal, distribuição por status
- **Vendas**: Top produtos, análise por categoria, ticket médio
- **Monitoramento**: Saúde do pipeline, logs de execução

📖 **Guia completo**: [docs/powerbi_connection_guide.md](docs/powerbi_connection_guide.md)

---

## 👨‍💻 Autor

**Kaio Ambrosio**

[![GitHub](https://img.shields.io/badge/GitHub-KaioAmbrosio-181717?style=flat&logo=github)](https://github.com/KaioAmbrosio)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Kaio%20Ambrosio-0077B5?style=flat&logo=linkedin)](https://linkedin.com/in/kaioambrosio)

---

## 📄 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.

---

<div align="center">

**⭐ Se este projeto foi útil, considere dar uma estrela!**

</div>
