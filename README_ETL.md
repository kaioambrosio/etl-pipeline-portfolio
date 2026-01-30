# 🔄 ETL Pipeline - Projeto de Portfólio

[![Python](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Pipeline ETL profissional para processamento de transações financeiras com **5M de registros**, catálogo de produtos e itens por pedido.

---

## 📋 Índice

- [Sobre o Projeto](#-sobre-o-projeto)
- [Arquitetura](#-arquitetura)
- [Tecnologias](#-tecnologias)
- [Instalação](#-instalação)
- [Variáveis de Ambiente](#-variáveis-de-ambiente)
- [Como Usar](#-como-usar)
- [Modo COPY (alta performance)](#-modo-copy-alta-performance)
- [Modelagem de Dados](#-modelagem-de-dados)
- [Logs e Monitoramento](#-logs-e-monitoramento)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Autor](#-autor)

---

## 🎯 Sobre o Projeto

Este projeto demonstra a construção de um **pipeline ETL completo e profissional** aplicado a um cenário real de negócio. O objetivo é transformar dados brutos (CSV/Excel) em **informação confiável e analisável**, com foco em **volume, consistência e performance**.

### Objetivos

- Demonstrar domínio em **Python aplicado a dados**
- Evidenciar conhecimento em **SQL e modelagem relacional**
- Aplicar conceitos de **automação, logging e rastreabilidade**
- Entregar um projeto **claro para avaliação técnica e portfólio**

---

## 🏗 Arquitetura

```
CSV/Excel → Python (Pandas) → PostgreSQL → API FastAPI → Dashboard / Power BI
```

---

## 🛠 Tecnologias

| Categoria | Tecnologia | Uso |
|-----------|------------|-----|
| Linguagem | Python 3.12+ | Motor de processamento |
| Dados | Pandas, NumPy | Transformação e validação |
| Banco | PostgreSQL | Persistência relacional |
| ORM | SQLAlchemy | Abstração de banco |
| API | FastAPI | Camada de consulta |
| Logging | Loguru | Logs estruturados |
| Visualização | React + Power BI | Dashboard e BI |
| Testes | Pytest | Testes automatizados |

---

## 🚀 Instalação

### Pré-requisitos

- Python 3.12 ou superior
- PostgreSQL 15 ou superior
- Git

### Passo a passo

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
cp .env.example .env
```

5. **Configure o banco de dados**
```bash
psql -d etl_portfolio -f sql/setup.sql
psql -d etl_portfolio -f sql/optimizations.sql
```

---

## 🔧 Variáveis de Ambiente

`.env` (ETL/API):

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `ETL_USE_COPY` (true/false) – habilita carga via COPY
- `ETL_COPY_THRESHOLD` (ex: 200000) – mínimo de linhas para usar COPY
- `API_CORS_ORIGINS` (opcional)
- `API_SNAPSHOT_LIMIT` (opcional)

---

## 📦 Como Usar

### Gerar dados de exemplo

```bash
python scripts/main.py --generate-sample
python scripts/main.py
```

### Gerar dados realistas (portfólio)

```bash
python scripts/generate_portfolio_data.py --rows 5000000 --years 5
python scripts/main.py
```

Gera **catálogo, transações e itens**, com sazonalidade, variação por dia da semana e nomes em pt-BR. As datas são limitadas aos últimos 5 anos (sem datas futuras).

---

## ⚡ Modo COPY (alta performance)

Para volumes grandes, o ETL pode carregar dados via **COPY nativo** do PostgreSQL:

```env
ETL_USE_COPY=true
ETL_COPY_THRESHOLD=200000
```

- `transacoes` são carregadas via arquivo temporário CSV + COPY.
- `transacao_itens` usa staging table + COPY e join com `produtos`.
- Para o benchmark com 5M, veja: `docs/performance_report.md`.

---

## 🧱 Modelagem de Dados

Tabelas principais:

- `categorias`
- `produtos`
- `transacoes`
- `transacao_itens`
- `logs_etl`
- `arquivos_processados`

Status de pagamento padronizados: **PAGO, PENDENTE, CANCELADO, ATRASADO, ERRO**.

---

## 📝 Logs e Monitoramento

- Logs gerais em `logs/etl.log`
- Registro de execução em `logs_etl`
- Controle de arquivos processados em `arquivos_processados`

---

## 📁 Estrutura do Projeto

```
ETL/
├── api/                       # API FastAPI
├── config/                    # Configurações
├── data/                      # CSVs brutos e processados
├── dashboard/                 # Front-end React
├── docs/                      # Documentação e relatórios
├── logs/                      # Logs do ETL
├── scripts/                   # ETL, geradores e utilitários
├── sql/                       # Schema, views e otimizações
├── tests/                     # Testes automatizados
├── .env.example               # Exemplo de variáveis
├── README.md                  # Visão geral
└── README_ETL.md              # Detalhes do ETL
```

---

## 👤 Autor

**Kaio Ambrosio**  
GitHub: https://github.com/KaioAmbrosio
