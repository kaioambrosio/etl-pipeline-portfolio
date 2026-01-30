-- =============================================================================
-- ETL Pipeline - Schema do Banco de Dados
-- =============================================================================
-- Autor: Kaio Ambrosio
-- Descrição: Schema completo para o pipeline ETL de transações financeiras
-- Banco: PostgreSQL
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Configurações Iniciais
-- -----------------------------------------------------------------------------

-- Criar extensão para UUID (opcional, mas profissional)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------------------------------------------------------
-- TABELA: transacoes
-- -----------------------------------------------------------------------------
-- Armazena todas as transações financeiras processadas pelo pipeline ETL.
-- Esta é a tabela principal de dados do sistema.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS transacao_itens CASCADE;
DROP TABLE IF EXISTS produtos CASCADE;
DROP TABLE IF EXISTS categorias CASCADE;
DROP TABLE IF EXISTS transacoes CASCADE;

-- -----------------------------------------------------------------------------
-- TABELA: categorias
-- -----------------------------------------------------------------------------
-- Dimensão de categorias de produtos.
-- -----------------------------------------------------------------------------

CREATE TABLE categorias (
    id                  SERIAL PRIMARY KEY,
    nome                VARCHAR(100) NOT NULL UNIQUE,
    descricao           TEXT
);

COMMENT ON TABLE categorias IS 'Dimensão de categorias de produtos';
COMMENT ON COLUMN categorias.nome IS 'Nome da categoria';

-- -----------------------------------------------------------------------------
-- TABELA: produtos
-- -----------------------------------------------------------------------------
-- Dimensão de produtos com faixa de preço realista.
-- -----------------------------------------------------------------------------

CREATE TABLE produtos (
    id                  SERIAL PRIMARY KEY,
    categoria_id        INTEGER NOT NULL REFERENCES categorias(id),
    nome                VARCHAR(255) NOT NULL UNIQUE,
    descricao           TEXT,
    preco_base          DECIMAL(15, 2) NOT NULL CHECK (preco_base >= 0),
    preco_min           DECIMAL(15, 2) NOT NULL CHECK (preco_min >= 0),
    preco_max           DECIMAL(15, 2) NOT NULL CHECK (preco_max >= 0),
    ativo               BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_produtos_categoria ON produtos(categoria_id);
CREATE INDEX idx_produtos_nome ON produtos(nome);

COMMENT ON TABLE produtos IS 'Dimensão de produtos';
COMMENT ON COLUMN produtos.preco_base IS 'Preço médio/base sugerido do produto';

CREATE TABLE transacoes (
    -- Identificador único da transação
    id                  SERIAL PRIMARY KEY,

    -- Identificador original do arquivo fonte
    id_transacao        VARCHAR(50) NOT NULL,

    -- Data e hora da transação
    data_transacao      TIMESTAMP NOT NULL,

    -- Informações do cliente
    cliente             VARCHAR(255) NOT NULL,

    -- Informações do produto
    produto             VARCHAR(255) NOT NULL,
    categoria           VARCHAR(100) NOT NULL,

    -- Valor da transação
    valor               DECIMAL(15, 2) NOT NULL CHECK (valor >= 0),

    -- Status do pagamento
    status_pagamento    VARCHAR(50) NOT NULL,

    -- Data do pagamento (pode ser nula se ainda não foi pago)
    data_pagamento      TIMESTAMP NULL,

    -- Campos derivados (calculados durante transformação)
    ano_transacao       INTEGER NOT NULL,
    mes_transacao       INTEGER NOT NULL CHECK (mes_transacao BETWEEN 1 AND 12),
    dia_semana          INTEGER CHECK (dia_semana BETWEEN 0 AND 6),
    trimestre           INTEGER CHECK (trimestre BETWEEN 1 AND 4),

    -- Metadados de controle
    arquivo_origem      VARCHAR(255) NOT NULL,
    data_processamento  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Constraint para evitar duplicatas
    CONSTRAINT uk_transacao_id UNIQUE (id_transacao)
);

-- Índices para otimização de consultas
CREATE INDEX idx_transacoes_data ON transacoes(data_transacao);
CREATE INDEX idx_transacoes_cliente ON transacoes(cliente);
CREATE INDEX idx_transacoes_categoria ON transacoes(categoria);
CREATE INDEX idx_transacoes_produto ON transacoes(produto);
CREATE INDEX idx_transacoes_status ON transacoes(status_pagamento);
CREATE INDEX idx_transacoes_ano_mes ON transacoes(ano_transacao, mes_transacao);
CREATE INDEX idx_transacoes_ano_mes_data ON transacoes(ano_transacao, mes_transacao, data_transacao);
CREATE INDEX idx_transacoes_ano_mes_categoria_data ON transacoes(ano_transacao, mes_transacao, categoria, data_transacao);
CREATE INDEX idx_transacoes_ano_mes_status_data ON transacoes(ano_transacao, mes_transacao, status_pagamento, data_transacao);
CREATE INDEX idx_transacoes_id_transacao ON transacoes(id_transacao);

-- Comentários na tabela e colunas
COMMENT ON TABLE transacoes IS 'Tabela principal de transações financeiras processadas pelo ETL';
COMMENT ON COLUMN transacoes.id IS 'Chave primária auto-incrementada';
COMMENT ON COLUMN transacoes.id_transacao IS 'ID original da transação no arquivo fonte';
COMMENT ON COLUMN transacoes.valor IS 'Valor monetário da transação em BRL';
COMMENT ON COLUMN transacoes.status_pagamento IS 'Status: PAGO, PENDENTE, CANCELADO, ATRASADO, ERRO';

-- -----------------------------------------------------------------------------
-- TABELA: transacao_itens
-- -----------------------------------------------------------------------------
-- Itens da transação (permite múltiplos produtos por transação).
-- -----------------------------------------------------------------------------

CREATE TABLE transacao_itens (
    id                  SERIAL PRIMARY KEY,
    id_transacao        VARCHAR(50) NOT NULL REFERENCES transacoes(id_transacao) ON DELETE CASCADE,
    produto_id          INTEGER NOT NULL REFERENCES produtos(id),
    quantidade          INTEGER NOT NULL CHECK (quantidade > 0),
    valor_unitario      DECIMAL(15, 2) NOT NULL CHECK (valor_unitario >= 0),
    valor_total         DECIMAL(15, 2) NOT NULL CHECK (valor_total >= 0)
);

CREATE INDEX idx_itens_transacao ON transacao_itens(id_transacao);
CREATE INDEX idx_itens_produto ON transacao_itens(produto_id);

COMMENT ON TABLE transacao_itens IS 'Itens da transação (produtos vendidos por pedido)';


-- -----------------------------------------------------------------------------
-- TABELA: logs_etl
-- -----------------------------------------------------------------------------
-- Registra todas as execuções do pipeline ETL para auditoria e monitoramento.
-- Essencial para rastreabilidade e debugging.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS logs_etl CASCADE;

CREATE TABLE logs_etl (
    -- Identificador único do log
    id_log              SERIAL PRIMARY KEY,

    -- Timestamp da execução
    data_execucao       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Informações do arquivo processado
    arquivo_processado  VARCHAR(255) NOT NULL,

    -- Métricas de processamento
    qtd_registros_lidos     INTEGER NOT NULL DEFAULT 0,
    qtd_registros_inseridos INTEGER NOT NULL DEFAULT 0,
    qtd_registros_rejeitados INTEGER NOT NULL DEFAULT 0,
    qtd_duplicatas_ignoradas INTEGER NOT NULL DEFAULT 0,

    -- Status da execução
    status_execucao     VARCHAR(20) NOT NULL CHECK (
        status_execucao IN ('SUCESSO', 'ERRO', 'PARCIAL', 'EM_ANDAMENTO')
    ),

    -- Tempo de processamento em segundos
    tempo_execucao_seg  DECIMAL(10, 3),

    -- Mensagem de erro (se houver)
    mensagem_erro       TEXT,

    -- Detalhes adicionais em JSON
    detalhes            JSONB,

    -- Hash do arquivo para verificação de integridade
    arquivo_hash        VARCHAR(64)
);

-- Índices para consultas frequentes
CREATE INDEX idx_logs_data ON logs_etl(data_execucao DESC);
CREATE INDEX idx_logs_status ON logs_etl(status_execucao);
CREATE INDEX idx_logs_arquivo ON logs_etl(arquivo_processado);

-- Comentários
COMMENT ON TABLE logs_etl IS 'Registro de todas as execuções do pipeline ETL';
COMMENT ON COLUMN logs_etl.status_execucao IS 'SUCESSO: processado sem erros; ERRO: falha total; PARCIAL: alguns registros falharam';


-- -----------------------------------------------------------------------------
-- TABELA: arquivos_processados
-- -----------------------------------------------------------------------------
-- Controla quais arquivos já foram processados para evitar reprocessamento.
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS arquivos_processados CASCADE;

CREATE TABLE arquivos_processados (
    id                  SERIAL PRIMARY KEY,
    nome_arquivo        VARCHAR(255) NOT NULL UNIQUE,
    caminho_completo    VARCHAR(500) NOT NULL,
    tamanho_bytes       BIGINT,
    hash_arquivo        VARCHAR(64) NOT NULL,
    data_processamento  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status              VARCHAR(20) NOT NULL DEFAULT 'PROCESSADO',
    id_log_etl          INTEGER REFERENCES logs_etl(id_log)
);

CREATE INDEX idx_arquivos_hash ON arquivos_processados(hash_arquivo);
CREATE INDEX idx_arquivos_nome ON arquivos_processados(nome_arquivo);

COMMENT ON TABLE arquivos_processados IS 'Controle de arquivos já processados para evitar duplicação';


-- -----------------------------------------------------------------------------
-- VIEW: vw_resumo_transacoes
-- -----------------------------------------------------------------------------
-- View para análise rápida no Power BI
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_resumo_transacoes AS
SELECT
    ano_transacao AS ano,
    mes_transacao AS mes,
    categoria,
    status_pagamento,
    COUNT(*) AS total_transacoes,
    SUM(valor) AS valor_total,
    AVG(valor) AS valor_medio,
    MIN(valor) AS valor_minimo,
    MAX(valor) AS valor_maximo
FROM transacoes
GROUP BY ano_transacao, mes_transacao, categoria, status_pagamento
ORDER BY ano_transacao DESC, mes_transacao DESC;

COMMENT ON VIEW vw_resumo_transacoes IS 'Resumo agregado de transações por período e categoria';


-- -----------------------------------------------------------------------------
-- VIEW: vw_status_etl
-- -----------------------------------------------------------------------------
-- View para monitoramento do pipeline
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_status_etl AS
SELECT
    DATE(data_execucao) AS data,
    COUNT(*) AS total_execucoes,
    SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END) AS sucessos,
    SUM(CASE WHEN status_execucao = 'ERRO' THEN 1 ELSE 0 END) AS erros,
    SUM(qtd_registros_inseridos) AS total_registros_inseridos,
    AVG(tempo_execucao_seg) AS tempo_medio_seg
FROM logs_etl
GROUP BY DATE(data_execucao)
ORDER BY data DESC;

COMMENT ON VIEW vw_status_etl IS 'Resumo diário de execuções do pipeline ETL';


-- -----------------------------------------------------------------------------
-- FUNÇÃO: fn_verificar_arquivo_processado
-- -----------------------------------------------------------------------------
-- Verifica se um arquivo já foi processado pelo hash
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION fn_verificar_arquivo_processado(p_hash VARCHAR(64))
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM arquivos_processados
        WHERE hash_arquivo = p_hash AND status = 'PROCESSADO'
    );
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION fn_verificar_arquivo_processado IS 'Retorna TRUE se o arquivo já foi processado';


-- =============================================================================
-- FIM DO SCHEMA
-- =============================================================================
