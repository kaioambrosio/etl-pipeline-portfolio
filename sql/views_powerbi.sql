-- =============================================================================
-- ETL Pipeline - Views Otimizadas para Power BI
-- =============================================================================
-- Autor: Kaio Ambrosio
-- Descrição: Views criadas especificamente para consumo pelo Power BI
-- =============================================================================

-- -----------------------------------------------------------------------------
-- VIEW: vw_fato_transacoes
-- -----------------------------------------------------------------------------
-- Tabela fato principal para análises no Power BI
-- Inclui todas as métricas e dimensões necessárias
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_fato_transacoes AS
SELECT
    -- Identificadores
    t.id AS id_registro,
    t.id_transacao,

    -- Dimensões de Data
    t.data_transacao,
    t.ano_transacao AS ano,
    t.mes_transacao AS mes,
    t.trimestre,
    t.dia_semana,
    TO_CHAR(t.data_transacao, 'YYYY-MM') AS ano_mes,
    TO_CHAR(t.data_transacao, 'Month') AS nome_mes,
    CASE t.dia_semana
        WHEN 0 THEN 'Segunda'
        WHEN 1 THEN 'Terça'
        WHEN 2 THEN 'Quarta'
        WHEN 3 THEN 'Quinta'
        WHEN 4 THEN 'Sexta'
        WHEN 5 THEN 'Sábado'
        WHEN 6 THEN 'Domingo'
    END AS nome_dia_semana,

    -- Dimensões de Produto
    t.produto,
    t.categoria,

    -- Dimensões de Cliente
    t.cliente,

    -- Dimensões de Pagamento
    t.status_pagamento,
    t.data_pagamento,
    CASE
        WHEN t.status_pagamento = 'PAGO' THEN 'Realizado'
        WHEN t.status_pagamento = 'PENDENTE' THEN 'Aguardando'
        WHEN t.status_pagamento = 'CANCELADO' THEN 'Cancelado'
        WHEN t.status_pagamento = 'ATRASADO' THEN 'Em Atraso'
        WHEN t.status_pagamento = 'ERRO' THEN 'Erro'
    END AS status_descritivo,

    -- Métricas
    t.valor,
    CASE WHEN t.status_pagamento = 'PAGO' THEN t.valor ELSE 0 END AS valor_recebido,
    CASE WHEN t.status_pagamento = 'PENDENTE' THEN t.valor ELSE 0 END AS valor_pendente,
    CASE WHEN t.status_pagamento = 'CANCELADO' THEN t.valor ELSE 0 END AS valor_cancelado,
    CASE WHEN t.status_pagamento = 'ATRASADO' THEN t.valor ELSE 0 END AS valor_atrasado,

    -- Metadados
    t.arquivo_origem,
    t.data_processamento

FROM transacoes t;

COMMENT ON VIEW vw_fato_transacoes IS 'Tabela fato principal para Power BI com todas as dimensões e métricas';


-- -----------------------------------------------------------------------------
-- VIEW: vw_dim_calendario
-- -----------------------------------------------------------------------------
-- Dimensão de calendário para análises temporais
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_dim_calendario AS
SELECT DISTINCT
    DATE(data_transacao) AS data,
    ano_transacao AS ano,
    mes_transacao AS mes,
    trimestre,
    TO_CHAR(data_transacao, 'YYYY-MM') AS ano_mes,
    TO_CHAR(data_transacao, 'Month') AS nome_mes,
    EXTRACT(WEEK FROM data_transacao) AS semana_ano,
    CASE WHEN mes_transacao <= 6 THEN 1 ELSE 2 END AS semestre
FROM transacoes
ORDER BY data;

COMMENT ON VIEW vw_dim_calendario IS 'Dimensão de calendário para análises temporais no Power BI';


-- -----------------------------------------------------------------------------
-- VIEW: vw_dim_produto
-- -----------------------------------------------------------------------------
-- Dimensão de produtos
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_dim_produto AS
SELECT DISTINCT
    produto,
    categoria,
    COUNT(*) OVER (PARTITION BY produto) AS total_vendas,
    SUM(valor) OVER (PARTITION BY produto) AS valor_total_produto
FROM transacoes
ORDER BY categoria, produto;

COMMENT ON VIEW vw_dim_produto IS 'Dimensão de produtos para Power BI';


-- -----------------------------------------------------------------------------
-- VIEW: vw_kpi_resumo
-- -----------------------------------------------------------------------------
-- KPIs principais para cards do Power BI
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_kpi_resumo AS
SELECT
    COUNT(*) AS total_transacoes,
    COUNT(DISTINCT cliente) AS total_clientes,
    COUNT(DISTINCT produto) AS total_produtos,
    SUM(valor) AS valor_total_bruto,
    SUM(CASE WHEN status_pagamento = 'PAGO' THEN valor ELSE 0 END) AS valor_total_recebido,
    SUM(CASE WHEN status_pagamento = 'PENDENTE' THEN valor ELSE 0 END) AS valor_total_pendente,
    SUM(CASE WHEN status_pagamento = 'CANCELADO' THEN valor ELSE 0 END) AS valor_total_cancelado,
    ROUND(AVG(valor)::numeric, 2) AS ticket_medio,
    ROUND(
        100.0 * SUM(CASE WHEN status_pagamento = 'PAGO' THEN 1 ELSE 0 END) / COUNT(*)::numeric,
        2
    ) AS taxa_conversao_pct,
    MIN(data_transacao) AS primeira_transacao,
    MAX(data_transacao) AS ultima_transacao
FROM transacoes;

COMMENT ON VIEW vw_kpi_resumo IS 'KPIs principais para dashboard executivo no Power BI';


-- -----------------------------------------------------------------------------
-- VIEW: vw_analise_mensal
-- -----------------------------------------------------------------------------
-- Análise mensal para gráficos de tendência
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_analise_mensal AS
SELECT
    ano_transacao AS ano,
    mes_transacao AS mes,
    TO_CHAR(DATE_TRUNC('month', MIN(data_transacao)), 'YYYY-MM') AS periodo,
    COUNT(*) AS total_transacoes,
    COUNT(DISTINCT cliente) AS clientes_unicos,
    SUM(valor) AS valor_bruto,
    SUM(CASE WHEN status_pagamento = 'PAGO' THEN valor ELSE 0 END) AS valor_recebido,
    ROUND(AVG(valor)::numeric, 2) AS ticket_medio,
    -- Crescimento em relação ao mês anterior (calculado no Power BI via DAX)
    LAG(SUM(valor)) OVER (ORDER BY ano_transacao, mes_transacao) AS valor_mes_anterior
FROM transacoes
GROUP BY ano_transacao, mes_transacao
ORDER BY ano_transacao, mes_transacao;

COMMENT ON VIEW vw_analise_mensal IS 'Análise mensal para gráficos de tendência no Power BI';


-- -----------------------------------------------------------------------------
-- VIEW: vw_top_clientes
-- -----------------------------------------------------------------------------
-- Top clientes por valor
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_top_clientes AS
SELECT
    cliente,
    COUNT(*) AS total_compras,
    SUM(valor) AS valor_total,
    ROUND(AVG(valor)::numeric, 2) AS ticket_medio,
    MIN(data_transacao) AS primeira_compra,
    MAX(data_transacao) AS ultima_compra,
    RANK() OVER (ORDER BY SUM(valor) DESC) AS ranking
FROM transacoes
WHERE status_pagamento NOT IN ('CANCELADO', 'ERRO')
GROUP BY cliente
ORDER BY valor_total DESC;

COMMENT ON VIEW vw_top_clientes IS 'Ranking de clientes por valor total de compras';


-- -----------------------------------------------------------------------------
-- VIEW: vw_top_produtos
-- -----------------------------------------------------------------------------
-- Top produtos por vendas
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_top_produtos AS
SELECT
    produto,
    categoria,
    COUNT(*) AS quantidade_vendida,
    SUM(valor) AS valor_total,
    ROUND(AVG(valor)::numeric, 2) AS preco_medio,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking_quantidade,
    RANK() OVER (ORDER BY SUM(valor) DESC) AS ranking_valor
FROM transacoes
WHERE status_pagamento NOT IN ('CANCELADO', 'ERRO')
GROUP BY produto, categoria
ORDER BY valor_total DESC;

COMMENT ON VIEW vw_top_produtos IS 'Ranking de produtos por quantidade e valor';


-- -----------------------------------------------------------------------------
-- VIEW: vw_analise_categoria
-- -----------------------------------------------------------------------------
-- Análise por categoria para gráficos de pizza/barras
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_analise_categoria AS
SELECT
    categoria,
    COUNT(*) AS total_transacoes,
    SUM(valor) AS valor_total,
    ROUND(AVG(valor)::numeric, 2) AS ticket_medio,
    COUNT(DISTINCT cliente) AS clientes_unicos,
    COUNT(DISTINCT produto) AS produtos_unicos,
    ROUND(
        100.0 * SUM(valor) / SUM(SUM(valor)) OVER ()::numeric,
        2
    ) AS participacao_pct
FROM transacoes
WHERE status_pagamento NOT IN ('CANCELADO', 'ERRO')
GROUP BY categoria
ORDER BY valor_total DESC;

COMMENT ON VIEW vw_analise_categoria IS 'Análise por categoria para gráficos de participação';


-- =============================================================================
-- FIM DAS VIEWS
-- =============================================================================
