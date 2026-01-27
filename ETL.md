# PRD – Product Requirements Document
## Projeto ETL Integrado de Dados
**Excel / CSV → Python → PostgreSQL → Power BI**

---

## 1. Visão Geral do Projeto

### 1.1 Contexto
Este projeto tem como objetivo demonstrar, de forma prática e profissional, a construção de um **pipeline de dados completo (ETL)** aplicado a um cenário real de negócio. O foco é transformar dados brutos (Excel/CSV) em **informação confiável e visualmente analisável**, utilizando Python como motor de processamento, PostgreSQL como base relacional e Power BI como camada analítica.

O projeto foi concebido para **portfólio profissional**, alinhado a vagas de **Dados, BI, Automação e Engenharia Analítica**, com ênfase em boas práticas, organização e clareza.

---

## 2. Objetivos

### 2.1 Objetivo Principal
Construir um pipeline ETL robusto, reproduzível e documentado, capaz de:
- Ingerir dados brutos
- Tratar e padronizar informações
- Persistir dados em banco relacional
- Disponibilizar análises via Power BI

### 2.2 Objetivos Secundários
- Demonstrar domínio de Python aplicado a dados
- Evidenciar conhecimento em SQL e modelagem relacional
- Aplicar conceitos de automação e logging
- Criar um projeto facilmente explicável em entrevistas

---

## 3. Escopo do Projeto

### 3.1 Dentro do Escopo
- ETL em Python
- Banco de dados PostgreSQL local
- Modelagem relacional simples
- Dashboards analíticos no Power BI
- Documentação técnica (README + PRD)

### 3.2 Fora do Escopo
- Interface web
- Deploy em cloud
- Autenticação de usuários
- Machine Learning

---

## 4. Público-Alvo (Stakeholders)

- Recrutadores técnicos
- Gestores de dados / BI
- Tech Leads
- O próprio desenvolvedor (como base evolutiva)

---

## 5. Dataset e Domínio de Negócio

### 5.1 Tema do Dataset
**Financeiro / Vendas / Cobrança** (dados simulados ou públicos)

### 5.2 Campos Esperados
- id_transacao
- data_transacao
- cliente
- produto
- categoria
- valor
- status_pagamento
- data_pagamento (opcional)

---

## 6. Arquitetura da Solução

```
┌────────────┐
│ Excel/CSV │
└─────┬──────┘
      ↓
┌────────────┐
│  Python    │  ← ETL (pandas)
└─────┬──────┘
      ↓
┌────────────┐
│ PostgreSQL │  ← Persistência
└─────┬──────┘
      ↓
┌────────────┐
│ Power BI   │  ← Visualização
└────────────┘
```

---

## 7. Requisitos Funcionais

### RF01 – Ingestão de Dados
- Ler arquivos CSV ou Excel de uma pasta `/data/raw`
- Validar estrutura mínima esperada

### RF02 – Tratamento de Dados
- Padronizar nomes de colunas
- Converter tipos (datas, valores)
- Tratar valores nulos
- Remover duplicidades

### RF03 – Transformações
- Criar colunas derivadas (ex: mês, ano)
- Normalizar status de pagamento

### RF04 – Persistência no Banco
- Criar tabelas automaticamente se não existirem
- Inserir dados em lote
- Evitar reprocessamento duplicado

### RF05 – Logging
- Registrar execução do pipeline
- Armazenar status, data, quantidade de registros

### RF06 – Disponibilização para BI
- Dados prontos para consumo direto pelo Power BI

---

## 8. Requisitos Não Funcionais

- Código organizado e legível
- Uso de virtual environment
- Separação por camadas (ingestão, transformação, carga)
- Scripts reutilizáveis
- Performance aceitável para até 100k registros

---

## 9. Modelagem de Dados (PostgreSQL)

### 9.1 Tabela: transacoes
- id (PK)
- data_transacao
- cliente
- produto
- categoria
- valor
- status_pagamento

### 9.2 Tabela: logs_etl
- id_log (PK)
- data_execucao
- arquivo_processado
- qtd_registros
- status_execucao
- mensagem_erro

---

## 10. Estrutura de Pastas

```
project-etl/
│
├── data/
│   ├── raw/
│   └── processed/
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
│
├── sql/
│   └── schema.sql
│
├── dashboard/
│   └── powerbi.pbix
│
├── logs/
│   └── etl.log
│
├── requirements.txt
└── README.md
```

---

## 11. Fluxo de Execução do ETL

1. Executar `main.py`
2. Identificar arquivos novos
3. Extrair dados
4. Transformar dados
5. Carregar no PostgreSQL
6. Registrar log

---

## 12. Uso do Claude Code (Copiloto)

### Exemplos de Uso
- Gerar funções de ETL
- Revisar código Python
- Otimizar queries SQL
- Sugerir validações de dados
- Auxiliar na escrita do README

### Prompt Base Recomendado
> "Atue como um engenheiro de dados sênior. Gere código Python limpo, modular e documentado para um pipeline ETL usando pandas e PostgreSQL."

---

## 13. Critérios de Aceite

- Pipeline executa sem erros
- Dados persistidos corretamente
- Logs registrados
- Power BI conectado ao PostgreSQL
- README claro e profissional

---

## 14. Possíveis Evoluções Futuras

- Agendamento (cron / n8n)
- API como fonte de dados
- Incremental load
- Dockerização
- Deploy em cloud

---

## 15. Indicadores de Sucesso

- Projeto compreendido em até 5 minutos por recrutador
- Código organizado e versionado
- Capacidade de explicar decisões técnicas

---

## 16. Conclusão

Este projeto representa um **case realista de dados**, alinhado ao mercado, ao currículo do desenvolvedor e às melhores práticas de engenharia analítica. Ele serve tanto como **portfólio técnico** quanto como **base evolutiva para projetos mais complexos**.

