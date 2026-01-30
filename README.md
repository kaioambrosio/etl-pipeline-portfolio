# ETL Portfólio + Dashboard Financeiro

Projeto de portfólio integrado: ETL (Python) + PostgreSQL + API FastAPI + Dashboard (React).

## O que este projeto demonstra

- Pipeline ETL completo: extração, transformação, validação e carga em lote.
- Catálogo de produtos e itens por transação (modelagem mais próxima de um cenário real).
- API com filtros e métricas comparativas (mês/ano anterior).
- Dashboard interativo com cross-filtering e detalhamento de transações.

## Capturas do dashboard

![Visão geral do dashboard](docs/images/dashboard-visao-geral.png)

![Filtros aplicados](docs/images/dashboard-filtros.png)

![Detalhamento de transação](docs/images/dashboard-transacoes-detalhe.png)

## Pré-requisitos

- Python 3.12+
- PostgreSQL 15+
- Node.js 18+

## Setup rápido

1) Banco de dados
```bash
psql -d etl_portfolio -f sql/setup.sql
psql -d etl_portfolio -f sql/optimizations.sql
```

2) ETL
```bash
cp .env.example .env
pip install -r requirements.txt
python scripts/main.py --generate-sample
python scripts/main.py
```

3) Geração de dados realistas (portfólio)
```bash
python scripts/generate_portfolio_data.py --rows 5000000 --years 5
python scripts/main.py
```
Gera catálogo, transações e itens. Os dados possuem distribuição sazonal, variações por dia da semana e nomes acentuados em pt-BR.

4) API (FastAPI)
```bash
uvicorn api.main:app --reload --port 8000
```
Ao subir, a API gera `dashboard/public/mock.json` como snapshot.

5) Dashboard
```bash
cd dashboard
npm install
npm run dev
```

## Variáveis de ambiente

### ETL/API (`.env`)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `API_CORS_ORIGINS` (opcional)
- `API_SNAPSHOT_LIMIT` (opcional)

### Dashboard (`dashboard/.env`)
```
VITE_API_URL=http://localhost:8000
VITE_USE_MOCK=false
```
Se `VITE_USE_MOCK=true`, o dashboard usa o snapshot ou o gerador local.

## Documentação específica

- ETL detalhado: `README_ETL.md`
- Dashboard: `dashboard/README.md`
- Power BI: `docs/powerbi_connection_guide.md`
