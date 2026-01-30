# PRD – ETL Portfólio Integrado

**Excel/CSV → Python → PostgreSQL → API → Dashboard/Power BI**

---

## 1. Visão Geral

### 1.1 Contexto
Este projeto demonstra, de forma prática e profissional, a construção de um **pipeline de dados completo (ETL)** aplicado a um cenário real de negócio. O foco é transformar dados brutos em informação confiável e analisável, com desempenho adequado para grandes volumes.

### 1.2 Propósito
Servir como **portfólio técnico** para vagas de Dados, BI e Engenharia Analítica, evidenciando organização, qualidade de código, documentação e resultados mensuráveis.

---

## 2. Objetivos

### 2.1 Objetivo principal
Construir um pipeline ETL robusto, reproduzível e documentado, capaz de:
- Ingerir dados brutos (CSV/Excel)
- Padronizar e validar informações
- Persistir dados em banco relacional
- Disponibilizar métricas via API e dashboard

### 2.2 Objetivos secundários
- Demonstrar domínio de Python aplicado a dados
- Evidenciar conhecimento em SQL e modelagem relacional
- Aplicar logging, rastreabilidade e reprocessamento controlado
- Entregar um projeto claro para avaliação técnica

---

## 3. Escopo

### 3.1 Dentro do escopo
- ETL em Python (extração, transformação, carga)
- PostgreSQL local
- API FastAPI para filtros e métricas
- Dashboard React para visualização
- Views para Power BI
- Relatório de performance

### 3.2 Fora do escopo
- Deploy em cloud
- Autenticação de usuários
- Monitoramento em produção
- Machine Learning

---

## 4. Dataset e Domínio de Negócio

### 4.1 Tema
Transações financeiras de vendas/cobrança (dados simulados).

### 4.2 Volume-alvo
- **5.000.000 transações**
- **8.150.000+ itens**
- **Catálogo com ~75 produtos**
- Período de **5 anos**, sem datas futuras

### 4.3 Status de pagamento
- PAGO
- PENDENTE
- CANCELADO
- ATRASADO
- ERRO (raro, para simulação de inconsistências)

---

## 5. Arquitetura da Solução

```
CSV/Excel → Python (ETL) → PostgreSQL → API FastAPI → Dashboard / Power BI
```

Componentes principais:
- **ETL**: scripts de extração, transformação e carga
- **Banco**: schema relacional + views analíticas
- **API**: endpoints para filtros, métricas e detalhamento
- **Dashboard**: filtros globais, comparativos e drill-down

---

## 6. Requisitos Funcionais

- **RF01**: Ingestão de arquivos CSV/Excel
- **RF02**: Padronização de colunas e tipos
- **RF03**: Cálculo de campos derivados (ano, mês, trimestre, dia da semana)
- **RF04**: Carga em lote no PostgreSQL
- **RF05**: Modo de carga acelerada via COPY (opcional)
- **RF06**: Registro de logs e rastreabilidade
- **RF07**: API para filtros, métricas e transações
- **RF08**: Snapshot automático para fallback do dashboard

---

## 7. Requisitos Não Funcionais

- Performance adequada para 5M registros
- Código organizado e legível
- Reprodutibilidade do pipeline
- Documentação clara e atualizada

---

## 8. Modelagem de Dados (resumo)

- `categorias`
- `produtos`
- `transacoes`
- `transacao_itens`
- `logs_etl`
- `arquivos_processados`

---

## 9. Fluxo de Execução do ETL

1. Identificar arquivos em `data/raw`
2. Extrair dados
3. Transformar e validar
4. Carregar no banco (batch ou COPY)
5. Registrar logs e arquivos processados

---

## 10. Métricas de Sucesso

- ETL completo com 5M transações em tempo aceitável
- Métricas comparativas corretas no dashboard
- Dados consistentes entre transações e itens
- Documentação completa para avaliação

---

## 11. Artefatos Entregáveis

- Pipeline ETL completo
- API FastAPI
- Dashboard React
- Views Power BI
- Relatório de performance (`docs/performance_report.md`)
- Guia Power BI (`docs/powerbi_connection_guide.md`)

---

## 12. Riscos e Mitigações

- **Carga lenta** → uso de COPY + índices
- **Dados inconsistentes** → validações e reconciliação
- **Baixa legibilidade** → documentação detalhada

---

**Autor:** Kaio Ambrosio
