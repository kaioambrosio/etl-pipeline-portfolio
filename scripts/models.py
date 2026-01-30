"""
ETL Pipeline - Modelos do Banco de Dados
========================================

Modelos SQLAlchemy para as tabelas do PostgreSQL.
Mapeamento objeto-relacional (ORM) das entidades do sistema.

Autor: Kaio Ambrosio
"""

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from sqlalchemy import (
    DECIMAL,
    JSON,
    Boolean,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Adiciona o diretório raiz ao path para importar config
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings


class Base(DeclarativeBase):
    """Classe base para todos os modelos SQLAlchemy."""

    pass


class Transacao(Base):
    """
    Modelo para a tabela de transações financeiras.

    Representa uma transação processada pelo pipeline ETL,
    incluindo informações do cliente, produto e pagamento.
    """

    __tablename__ = "transacoes"

    # Chave primária
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Identificador original
    id_transacao: Mapped[str] = mapped_column(String(50), nullable=False)

    # Data da transação
    data_transacao: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # Informações do cliente e produto
    cliente: Mapped[str] = mapped_column(String(255), nullable=False)
    produto: Mapped[str] = mapped_column(String(255), nullable=False)
    categoria: Mapped[str] = mapped_column(String(100), nullable=False)

    # Valor
    valor: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)

    # Status do pagamento
    status_pagamento: Mapped[str] = mapped_column(String(50), nullable=False)
    data_pagamento: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Campos derivados
    ano_transacao: Mapped[int] = mapped_column(Integer, nullable=False)
    mes_transacao: Mapped[int] = mapped_column(Integer, nullable=False)
    dia_semana: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    trimestre: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Metadados
    arquivo_origem: Mapped[str] = mapped_column(String(255), nullable=False)
    data_processamento: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=datetime.now,
        server_default=text("CURRENT_TIMESTAMP"),
    )

    # Constraints
    __table_args__ = (
        UniqueConstraint("id_transacao", name="uk_transacao_id"),
        CheckConstraint("valor >= 0", name="ck_valor_positivo"),
        CheckConstraint("mes_transacao BETWEEN 1 AND 12", name="ck_mes_valido"),
        CheckConstraint("dia_semana BETWEEN 0 AND 6", name="ck_dia_semana_valido"),
        CheckConstraint("trimestre BETWEEN 1 AND 4", name="ck_trimestre_valido"),
        Index("idx_transacoes_data", "data_transacao"),
        Index("idx_transacoes_cliente", "cliente"),
        Index("idx_transacoes_categoria", "categoria"),
        Index("idx_transacoes_produto", "produto"),
        Index("idx_transacoes_status", "status_pagamento"),
        Index("idx_transacoes_ano_mes", "ano_transacao", "mes_transacao"),
        Index(
            "idx_transacoes_ano_mes_data",
            "ano_transacao",
            "mes_transacao",
            "data_transacao",
        ),
        Index(
            "idx_transacoes_ano_mes_categoria_data",
            "ano_transacao",
            "mes_transacao",
            "categoria",
            "data_transacao",
        ),
        Index(
            "idx_transacoes_ano_mes_status_data",
            "ano_transacao",
            "mes_transacao",
            "status_pagamento",
            "data_transacao",
        ),
        Index("idx_transacoes_id_transacao", "id_transacao"),
    )

    def __repr__(self) -> str:
        return (
            f"<Transacao(id={self.id}, id_transacao='{self.id_transacao}', "
            f"cliente='{self.cliente}', valor={self.valor})>"
        )

    def to_dict(self) -> dict:
        """Converte o objeto para dicionário."""
        return {
            "id": self.id,
            "id_transacao": self.id_transacao,
            "data_transacao": self.data_transacao.isoformat() if self.data_transacao else None,
            "cliente": self.cliente,
            "produto": self.produto,
            "categoria": self.categoria,
            "valor": float(self.valor),
            "status_pagamento": self.status_pagamento,
            "data_pagamento": self.data_pagamento.isoformat() if self.data_pagamento else None,
            "ano_transacao": self.ano_transacao,
            "mes_transacao": self.mes_transacao,
        }


class LogETL(Base):
    """
    Modelo para a tabela de logs do pipeline ETL.

    Registra todas as execuções do pipeline para auditoria
    e monitoramento do sistema.
    """

    __tablename__ = "logs_etl"

    # Chave primária
    id_log: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Timestamp
    data_execucao: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.now)

    # Arquivo processado
    arquivo_processado: Mapped[str] = mapped_column(String(255), nullable=False)

    # Métricas
    qtd_registros_lidos: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qtd_registros_inseridos: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qtd_registros_rejeitados: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    qtd_duplicatas_ignoradas: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Status
    status_execucao: Mapped[str] = mapped_column(String(20), nullable=False)

    # Tempo de execução
    tempo_execucao_seg: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(10, 3), nullable=True)

    # Mensagem de erro
    mensagem_erro: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Detalhes em JSON
    detalhes: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Hash do arquivo
    arquivo_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Constraints
    __table_args__ = (
        CheckConstraint(
            "status_execucao IN ('SUCESSO', 'ERRO', 'PARCIAL', 'EM_ANDAMENTO')",
            name="ck_status_valido",
        ),
        Index("idx_logs_data", "data_execucao"),
        Index("idx_logs_status", "status_execucao"),
        Index("idx_logs_arquivo", "arquivo_processado"),
    )

    def __repr__(self) -> str:
        return (
            f"<LogETL(id_log={self.id_log}, arquivo='{self.arquivo_processado}', "
            f"status='{self.status_execucao}')>"
        )


class ArquivoProcessado(Base):
    """
    Modelo para controle de arquivos já processados.

    Evita reprocessamento de arquivos já carregados no sistema.
    """

    __tablename__ = "arquivos_processados"

    # Chave primária
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Informações do arquivo
    nome_arquivo: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    caminho_completo: Mapped[str] = mapped_column(String(500), nullable=False)
    tamanho_bytes: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    hash_arquivo: Mapped[str] = mapped_column(String(64), nullable=False)

    # Controle
    data_processamento: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.now
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PROCESSADO")

    # Relacionamento com log
    id_log_etl: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("logs_etl.id_log"), nullable=True
    )

    # Constraints
    __table_args__ = (
        Index("idx_arquivos_hash", "hash_arquivo"),
        Index("idx_arquivos_nome", "nome_arquivo"),
    )

    def __repr__(self) -> str:
        return f"<ArquivoProcessado(nome='{self.nome_arquivo}', status='{self.status}')>"


class Categoria(Base):
    """Modelo para a dimensão de categorias."""

    __tablename__ = "categorias"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nome: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    descricao: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<Categoria(id={self.id}, nome='{self.nome}')>"


class Produto(Base):
    """Modelo para a dimensão de produtos."""

    __tablename__ = "produtos"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    categoria_id: Mapped[int] = mapped_column(Integer, ForeignKey("categorias.id"), nullable=False)
    nome: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    descricao: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    preco_base: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    preco_min: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    preco_max: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    ativo: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index("idx_produtos_categoria", "categoria_id"),
        Index("idx_produtos_nome", "nome"),
    )

    def __repr__(self) -> str:
        return f"<Produto(id={self.id}, nome='{self.nome}')>"


class TransacaoItem(Base):
    """Modelo para itens da transação."""

    __tablename__ = "transacao_itens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_transacao: Mapped[str] = mapped_column(
        String(50), ForeignKey("transacoes.id_transacao"), nullable=False
    )
    produto_id: Mapped[int] = mapped_column(Integer, ForeignKey("produtos.id"), nullable=False)
    quantidade: Mapped[int] = mapped_column(Integer, nullable=False)
    valor_unitario: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    valor_total: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)

    __table_args__ = (
        CheckConstraint("quantidade > 0", name="ck_item_quantidade"),
        CheckConstraint("valor_unitario >= 0", name="ck_item_valor_unitario"),
        CheckConstraint("valor_total >= 0", name="ck_item_valor_total"),
        Index("idx_itens_transacao", "id_transacao"),
        Index("idx_itens_produto", "produto_id"),
    )

    def __repr__(self) -> str:
        return f"<TransacaoItem(id={self.id}, transacao='{self.id_transacao}')>"


def get_engine():
    """
    Cria e retorna o engine SQLAlchemy para conexão com o banco.

    Returns:
        Engine: Objeto de conexão SQLAlchemy.
    """
    settings = get_settings()
    return create_engine(
        settings.database_url,
        echo=settings.etl_log_level == "DEBUG",
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


def create_tables():
    """
    Cria todas as tabelas no banco de dados.

    Útil para inicialização do banco em ambiente de desenvolvimento.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("Tabelas criadas com sucesso!")


if __name__ == "__main__":
    # Teste de criação das tabelas
    print("Testando conexão e criação de tabelas...")
    try:
        create_tables()
    except Exception as e:
        print(f"Erro ao criar tabelas: {e}")
        print("Verifique se o PostgreSQL está rodando e as credenciais estão corretas.")
