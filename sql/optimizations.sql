-- =============================================================================
-- ETL Pipeline - Otimizacoes pontuais
-- =============================================================================
-- Execute apos o setup para adicionar indices extras sem recriar tabelas.
-- Uso: psql -d etl_portfolio -f sql/optimizations.sql
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_transacoes_produto ON transacoes(produto);

ANALYZE transacoes;
