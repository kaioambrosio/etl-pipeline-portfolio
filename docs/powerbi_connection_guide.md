# Guia de Conexão - Power BI ao PostgreSQL

## Visão Geral

Este documento descreve como conectar o Power BI Desktop ao banco de dados PostgreSQL do pipeline ETL.

## Pré-requisitos

1. **Power BI Desktop** instalado (versão mais recente recomendada)
2. **Driver PostgreSQL ODBC** (Npgsql) - geralmente já incluído no Power BI
3. **PostgreSQL** rodando com o banco `etl_portfolio` populado
4. **Views criadas**: execute `psql -d etl_portfolio -f sql/setup.sql`

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

Inicie o Power BI Desktop no seu computador.

### 2. Obter Dados

1. Clique em **"Obter Dados"** na barra de ferramentas
2. Selecione **"Banco de Dados"** na lista de categorias
3. Escolha **"Banco de dados PostgreSQL"**
4. Clique em **"Conectar"**

### 3. Configurar Conexão

Na janela de conexão, preencha:

- **Servidor**: `localhost`
- **Banco de dados**: `etl_portfolio`
- **Modo de conectividade de dados**:
  - **Importar** (recomendado para análises)
  - **DirectQuery** (para dados em tempo real)

### 4. Autenticação

1. Selecione **"Banco de dados"** no menu lateral esquerdo
2. Digite o **Usuário**: `postgres`
3. Digite a **Senha**: (sua senha)
4. Clique em **"Conectar"**

### 5. Selecionar Tabelas/Views

Selecione as seguintes views otimizadas para análise:

#### Views Recomendadas

| View | Descrição | Uso |
|------|-----------|-----|
| `vw_fato_transacoes` | Tabela fato principal | Base de todas as análises |
| `vw_dim_calendario` | Dimensão calendário | Relacionamentos de tempo |
| `vw_dim_produto` | Dimensão de produtos | Análises por produto |
| `vw_kpi_resumo` | KPIs principais | Cards do dashboard |
| `vw_analise_mensal` | Análise por mês | Gráficos de tendência |
| `vw_top_clientes` | Ranking de clientes | Top N análises |
| `vw_top_produtos` | Ranking de produtos | Top N análises |
| `vw_analise_categoria` | Participação por categoria | Gráfico de pizza/barras |
| `vw_resumo_transacoes` | Resumo agregado | Visão geral |

### 6. Transformar Dados (Opcional)

No Power Query Editor, você pode:

- Renomear colunas para português
- Formatar tipos de dados
- Criar colunas calculadas

## Modelo de Dados Sugerido

```
┌─────────────────────┐
│  vw_fato_transacoes │ (Tabela Fato)
├─────────────────────┤
│ id_registro         │
│ data_transacao      │──┐
│ cliente             │  │
│ produto             │  │
│ categoria           │  │
│ valor               │  │
│ status_pagamento    │  │
│ ano, mes, trimestre │  │
└─────────────────────┘  │
                         │
┌─────────────────────┐  │
│   vw_kpi_resumo     │  │
├─────────────────────┤  │
│ total_transacoes    │  │
│ valor_total_bruto   │  │
│ ticket_medio        │  │
└─────────────────────┘  │
                         │
┌─────────────────────┐  │
│  vw_analise_mensal  │←─┘ (Relacionar por ano/mes)
├─────────────────────┤
│ ano, mes            │
│ total_transacoes    │
│ valor_bruto         │
└─────────────────────┘
```

## Medidas DAX Sugeridas

### Valor Total
```dax
Valor Total = SUM(vw_fato_transacoes[valor])
```

### Ticket Médio
```dax
Ticket Medio = AVERAGE(vw_fato_transacoes[valor])
```

### Quantidade de Transações
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

### Valor Recebido
```dax
Valor Recebido =
CALCULATE(
    SUM(vw_fato_transacoes[valor]),
    vw_fato_transacoes[status_pagamento] = "PAGO"
)
```

### Crescimento MoM (Month over Month)
```dax
Crescimento MoM =
VAR ValorAtual = [Valor Total]
VAR ValorAnterior = CALCULATE([Valor Total], DATEADD('Date'[Date], -1, MONTH))
RETURN
DIVIDE(ValorAtual - ValorAnterior, ValorAnterior, 0)
```

## Visualizações Sugeridas

### 1. Dashboard Executivo

| Visualização | Dados | Tipo |
|--------------|-------|------|
| Card KPI | Total Transações | Cartão |
| Card KPI | Valor Total | Cartão |
| Card KPI | Ticket Médio | Cartão |
| Card KPI | Taxa Conversão | Cartão |
| Gráfico de Área | Valor por Mês | Tendência |
| Gráfico de Rosca | Valor por Status | Distribuição |
| Gráfico de Barras | Top 10 Produtos | Ranking |

### 2. Análise de Vendas

| Visualização | Dados | Tipo |
|--------------|-------|------|
| Gráfico de Colunas | Vendas por Categoria | Comparação |
| Gráfico de Linha | Tendência Mensal | Evolução |
| Matriz | Categoria x Mês | Detalhamento |
| Mapa de Calor | Dia da Semana x Hora | Padrões |

### 3. Análise de Clientes

| Visualização | Dados | Tipo |
|--------------|-------|------|
| Tabela | Top 20 Clientes | Ranking |
| Gráfico de Dispersão | Ticket vs Frequência | Segmentação |
| Gráfico de Funil | Status de Pagamento | Conversão |

## Refresh de Dados

### Configurar Atualização Automática

1. Publique o relatório no Power BI Service
2. Configure o Gateway para conexão local
3. Defina a frequência de atualização

### Atualização Manual

1. No Power BI Desktop: **Página Inicial > Atualizar**
2. Ou pressione **Ctrl + Alt + F5**

## Solução de Problemas

### Erro de Conexão

1. Verifique se o PostgreSQL está rodando
2. Confirme que a porta 5432 está acessível
3. Verifique firewall do Windows

### Dados não Aparecem

1. Execute o pipeline ETL para popular o banco
2. Verifique se as views existem no banco
3. Teste a conexão via pgAdmin ou psql

### Performance Lenta

1. Use modo **Importar** em vez de DirectQuery
2. Filtre datas para período específico
3. Use views agregadas em vez de tabela principal

## Contato

- **Autor**: Kaio Ambrosio
- **GitHub**: https://github.com/KaioAmbrosio
- **Projeto**: ETL Pipeline Portfolio
