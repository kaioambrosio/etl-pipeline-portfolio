# Guia de Conexão - Power BI ao PostgreSQL

## Visão Geral

Este documento descreve como conectar o Power BI Desktop ao banco PostgreSQL do pipeline ETL e utilizar as views já otimizadas para análise.

## Pré-requisitos

1. **Power BI Desktop** instalado (versão mais recente recomendada)
2. **PostgreSQL** rodando com o banco `etl_portfolio` populado
3. **Views criadas**:
   ```bash
   psql -d etl_portfolio -f sql/setup.sql
   psql -d etl_portfolio -f sql/optimizations.sql
   ```

## Dados de Conexão

| Parâmetro | Valor |
|-----------|-------|
| **Servidor** | localhost |
| **Porta** | 5432 |
| **Banco de Dados** | etl_portfolio |
| **Usuário** | postgres |
| **Senha** | (sua senha do PostgreSQL) |

## Passo a Passo

### 1. Abrir Power BI Desktop

Inicie o Power BI Desktop.

### 2. Obter Dados

1. Clique em **Obter Dados**
2. Selecione **Banco de Dados**
3. Escolha **Banco de dados PostgreSQL**
4. Clique em **Conectar**

### 3. Configurar Conexão

Preencha:

- **Servidor**: `localhost`
- **Banco de dados**: `etl_portfolio`
- **Modo**:
  - **Importar** (recomendado para performance)
  - **DirectQuery** (para dados em tempo real)

### 4. Autenticação

1. Selecione **Banco de dados** no menu lateral
2. Usuário: `postgres`
3. Senha: (sua senha)
4. Clique em **Conectar**

### 5. Selecionar Views

Views recomendadas para análise:

| View | Descrição | Uso |
|------|-----------|-----|
| `vw_fato_transacoes` | Fato principal | Base das análises |
| `vw_dim_calendario` | Dimensão de datas | Relacionamentos de tempo |
| `vw_dim_produto` | Dimensão de produtos | Análises por produto |
| `vw_kpi_resumo` | KPIs principais | Cards do dashboard |
| `vw_analise_mensal` | Evolução mensal | Gráficos de tendência |
| `vw_top_clientes` | Ranking de clientes | Top N |
| `vw_top_produtos` | Ranking de produtos | Top N |
| `vw_analise_categoria` | Participação por categoria | Barras/pizza |
| `vw_resumo_transacoes` | Resumo agregado | Visão geral |
| `vw_status_etl` | Status do ETL | Monitoramento |

## Modelo de Dados Sugerido

- **Fato**: `vw_fato_transacoes`
- **Dimensões**: `vw_dim_calendario`, `vw_dim_produto`
- **Cards**: `vw_kpi_resumo`

Relacionamento principal: `vw_fato_transacoes[data_transacao]` → `vw_dim_calendario[data]`.

## Medidas DAX Sugeridas

### Valor Total
```dax
Valor Total = SUM(vw_fato_transacoes[valor])
```

### Ticket Médio
```dax
Ticket Medio = AVERAGE(vw_fato_transacoes[valor])
```

### Total de Transações
```dax
Total Transacoes = COUNTROWS(vw_fato_transacoes)
```

### Taxa de Conversão
```dax
Taxa Conversao =
DIVIDE(
    CALCULATE(COUNTROWS(vw_fato_transacoes), vw_fato_transacoes[status_pagamento] = "PAGO"),
    COUNTROWS(vw_fato_transacoes),
    0
)
```

### Crescimento MoM
```dax
Crescimento MoM =
VAR ValorAtual = [Valor Total]
VAR ValorAnterior = CALCULATE([Valor Total], DATEADD('Date'[Date], -1, MONTH))
RETURN
DIVIDE(ValorAtual - ValorAnterior, ValorAnterior, 0)
```

## Visualizações Sugeridas

### Dashboard Executivo

| Visualização | Dados | Tipo |
|--------------|-------|------|
| Card KPI | Total Transações | Cartão |
| Card KPI | Valor Total | Cartão |
| Card KPI | Ticket Médio | Cartão |
| Card KPI | Taxa de Conversão | Cartão |
| Gráfico de Área | Valor por Mês | Tendência |
| Gráfico de Rosca | Valor por Status | Distribuição |
| Gráfico de Barras | Top Produtos | Ranking |

## Refresh de Dados

### Atualização Manual

- Power BI Desktop: **Página Inicial > Atualizar**
- Atalho: **Ctrl + Alt + F5**

### Atualização Automática

1. Publique o relatório no Power BI Service
2. Configure o Gateway para conexão local
3. Defina frequência de atualização

## Solução de Problemas

### Erro de Conexão

- Verifique se o PostgreSQL está rodando
- Confirme a porta 5432
- Libere no firewall, se necessário

### Dados não aparecem

- Execute o ETL e gere os dados
- Verifique se as views existem no banco
- Teste a conexão via pgAdmin ou psql

### Performance lenta

- Prefira **Importar** em vez de DirectQuery
- Filtre período de datas
- Use views agregadas (ex: `vw_analise_mensal`)

---

**Autor**: Kaio Ambrosio  
GitHub: https://github.com/KaioAmbrosio
