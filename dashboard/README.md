# Dashboard de Performance Financeira

Dashboard financeiro interativo desenvolvido em React + TypeScript + TailwindCSS, servindo como referência visual e de interação para replicação em Power BI.

## ?? Objetivo

Este dashboard foi criado para:
- Servir como **referência visual** para implementação em Power BI
- Demonstrar **padrões de interação** (cross-filtering, slicers)
- Apresentar **métricas financeiras** com clareza executiva
- Facilitar a **replicação** de componentes em ferramentas de BI

## ?? Métricas Implementadas

| Métrica | Fórmula | Componente |
|---------|---------|------------|
| Valor Total Transacionado | `SUM(valor)` | Hero KPI Card |
| Quantidade de Transações | `COUNT(id_transacao)` | KPI Card |
| Ticket Médio | `Valor Total / Quantidade` | Indicadores Card |
| Qtd Pagas | `COUNT WHERE status = 'Pago'` | KPI Card |
| % Pagas | `Qtd Pagas / Quantidade` | Indicadores Card |
| Tempo Médio Processamento | `AVG(data_processamento - data_transacao)` | Indicadores Card |
| Tempo Médio até Pagamento | `AVG(data_pagamento - data_transacao)` | Indicadores Card |
| Variação vs Período Anterior | `(Atual - Anterior) / Anterior` | Hero KPI Badge |

## ??? Schema de Dados

```sql
CREATE TABLE public.transacoes (
  id SERIAL PRIMARY KEY,
  id_transacao VARCHAR(50) UNIQUE,
  cliente VARCHAR(255),
  produto VARCHAR(255),
  categoria VARCHAR(100),
  valor NUMERIC(12,2),
  status_pagamento VARCHAR(20), -- 'Pago' | 'Pendente' | 'Atrasado' | 'Cancelado' | 'Erro'
  arquivo_origem VARCHAR(255),
  data_transacao TIMESTAMP,
  data_processamento TIMESTAMP,
  data_pagamento TIMESTAMP,
  dia_semana INT,
  mes_transacao INT,
  trimestre INT,
  ano_transacao INT
);

CREATE TABLE public.categorias (
  id SERIAL PRIMARY KEY,
  nome VARCHAR(100) UNIQUE,
  descricao TEXT
);

CREATE TABLE public.produtos (
  id SERIAL PRIMARY KEY,
  categoria_id INT REFERENCES categorias(id),
  nome VARCHAR(255) UNIQUE,
  descricao TEXT,
  preco_base NUMERIC(12,2),
  preco_min NUMERIC(12,2),
  preco_max NUMERIC(12,2),
  ativo BOOLEAN
);

CREATE TABLE public.transacao_itens (
  id SERIAL PRIMARY KEY,
  id_transacao VARCHAR(50) REFERENCES transacoes(id_transacao),
  produto_id INT REFERENCES produtos(id),
  quantidade INT,
  valor_unitario NUMERIC(12,2),
  valor_total NUMERIC(12,2)
);
```

## ?? Como Executar

### Modo Mock (Desenvolvimento)

Sem necessidade de banco de dados. Os dados são gerados automaticamente:

```bash
npm install
npm run dev
```

### Modo PostgreSQL (Produção)

Configure as variáveis de ambiente:

```env
VITE_API_URL=http://localhost:8000
VITE_USE_MOCK=false
```

> A API FastAPI na raiz do projeto é responsável por conectar no PostgreSQL.

### Snapshot automático (fallback)

Ao iniciar a API, um snapshot é gerado em `public/mock.json`.
Se a API estiver indisponível, o dashboard usa esse snapshot automaticamente.

### Endpoints da API utilizados

- `GET /filtros`
- `GET /metricas`
- `GET /metricas/comparativo`
- `GET /agregados/categorias`
- `GET /agregados/volume-mensal`
- `GET /agregados/dia-semana`
- `GET /transacoes` (paginações)
- `GET /transacoes/total`
- `GET /transacoes/{id_transacao}` (detalhamento)

## ??? Mapeamento para Power BI

| Componente React | Visual Power BI |
|-----------------|-----------------|
| Hero KPI Card | Card com Sparkline |
| KPI Card Crescimento | Card com Comparação |
| KPI Card Atividade | Card + Mini Gráfico de Barras |
| DistribuicaoCategoriaChart | Gráfico de Barras Horizontal |
| VolumeFinanceiroChart | Gráfico de Área |
| IndicadoresFinanceirosCard | Card Personalizado |
| TransacoesTable | Tabela/Matrix |
| FiltersBar | Slicers (Dropdown) |

## ?? Design System

- **Cores**: Verde primário (#16a34a), Lime accent (#84cc16)
- **Background**: #f6f7f9
- **Cards**: Brancos com border-radius 16-24px
- **Sombras**: Sutis, baixa opacidade
- **Espaçamento**: 16px / 24px / 32px
- **Fonte**: Inter

## ?? Interações

### Filtros Globais (Slicers)
- Ano, Mês, Categoria, Status, Produto
- Afetam todos os visuais simultaneamente

### Cross-Filtering
- Clique em barra de categoria ? filtra Volume Financeiro e Tabela
- Comportamento similar ao Power BI

### Detalhamento de Transação
- Clique em uma linha da tabela para expandir o card e ver itens e descrição

## ?? Estrutura de Arquivos

```
src/
+-- components/
¦   +-- dashboard/
¦       +-- Sidebar.tsx
¦       +-- Topbar.tsx
¦       +-- FiltersBar.tsx
¦       +-- KpiCards.tsx
¦       +-- DistribuicaoCategoriaChart.tsx
¦       +-- VolumeFinanceiroChart.tsx
¦       +-- IndicadoresFinanceirosCard.tsx
¦       +-- TransacoesTable.tsx
¦       +-- EmptyState.tsx
+-- lib/
¦   +-- data/
¦   ¦   +-- mockGenerator.ts  # Gera transações
¦   ¦   +-- metrics.ts        # Cálculos de KPIs
¦   ¦   +-- filters.ts        # Lógica de filtros
¦   +-- formatters.ts         # Formatação pt-BR
+-- pages/
    +-- Dashboard.tsx         # Página principal
```

## ?? Dicas de Replicação no Power BI

1. **KPIs**: Use Cards com formatação condicional para setas
2. **Cross-filter**: Configure interações entre visuais
3. **Cores**: Use tema personalizado com as cores do design system
4. **Tooltips**: Replique o formato de tooltip do React
5. **Status Pills**: Use formatação condicional com ícones

---

Desenvolvido com ?? para servir como referência Power BI
