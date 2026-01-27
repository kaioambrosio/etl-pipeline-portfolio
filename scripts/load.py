"""
ETL Pipeline - Módulo de Carga
==============================

Responsável pela carga de dados no PostgreSQL.
Implementa inserção em lote, controle de duplicatas e logging.

Autor: Kaio Ambrosio
"""

import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
from loguru import logger
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings
from scripts.models import (
    ArquivoProcessado,
    Base,
    LogETL,
    Transacao,
    get_engine,
)


@dataclass
class LoadResult:
    """
    Resultado da carga de dados.

    Attributes:
        success: Se a carga foi bem-sucedida.
        records_inserted: Quantidade de registros inseridos.
        records_updated: Quantidade de registros atualizados.
        records_skipped: Quantidade de registros ignorados (duplicatas).
        records_failed: Quantidade de registros com falha.
        error_message: Mensagem de erro, se houver.
        log_id: ID do log criado no banco.
        execution_time: Tempo de execução em segundos.
    """

    success: bool
    records_inserted: int = 0
    records_updated: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    error_message: Optional[str] = None
    log_id: Optional[int] = None
    execution_time: float = 0.0


class DataLoader:
    """
    Classe responsável pela carga de dados no PostgreSQL.

    Gerencia conexões, inserções em lote e controle de duplicatas.
    """

    def __init__(self):
        """Inicializa o carregador de dados."""
        self.settings = get_settings()
        self.engine = None
        self.Session = None
        self._connected = False

        logger.info("DataLoader inicializado")

    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados.

        Returns:
            True se conectou com sucesso, False caso contrário.
        """
        try:
            self.engine = get_engine()
            self.Session = sessionmaker(bind=self.engine)

            # Testar conexão
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self._connected = True
            logger.info(f"Conectado ao banco: {self.settings.database_url_masked}")
            return True

        except OperationalError as e:
            logger.error(f"Erro de conexão com o banco: {str(e)}")
            self._connected = False
            return False

    def ensure_tables_exist(self) -> bool:
        """
        Garante que as tabelas existam no banco.

        Cria as tabelas se não existirem usando SQLAlchemy.

        Returns:
            True se as tabelas existem/foram criadas.
        """
        if not self._connected:
            if not self.connect():
                return False

        try:
            Base.metadata.create_all(self.engine)
            logger.info("Tabelas verificadas/criadas com sucesso")
            return True

        except Exception as e:
            logger.error(f"Erro ao criar tabelas: {str(e)}")
            return False

    def check_file_processed(self, file_hash: str) -> bool:
        """
        Verifica se um arquivo já foi processado.

        Args:
            file_hash: Hash MD5 do arquivo.

        Returns:
            True se o arquivo já foi processado.
        """
        if not self._connected:
            return False

        with self.Session() as session:
            result = (
                session.query(ArquivoProcessado)
                .filter(
                    ArquivoProcessado.hash_arquivo == file_hash,
                    ArquivoProcessado.status == "PROCESSADO",
                )
                .first()
            )

            return result is not None

    def register_file_processed(
        self,
        session: Session,
        file_name: str,
        file_path: str,
        file_hash: str,
        file_size: int,
        log_id: int,
    ) -> None:
        """
        Registra um arquivo como processado.

        Args:
            session: Sessão do SQLAlchemy.
            file_name: Nome do arquivo.
            file_path: Caminho completo do arquivo.
            file_hash: Hash MD5 do arquivo.
            file_size: Tamanho em bytes.
            log_id: ID do log relacionado.
        """
        arquivo = ArquivoProcessado(
            nome_arquivo=file_name,
            caminho_completo=file_path,
            hash_arquivo=file_hash,
            tamanho_bytes=file_size,
            id_log_etl=log_id,
        )
        session.add(arquivo)

    def create_log_entry(
        self, session: Session, file_name: str, file_hash: str, status: str = "EM_ANDAMENTO"
    ) -> LogETL:
        """
        Cria uma entrada de log no banco.

        Args:
            session: Sessão do SQLAlchemy.
            file_name: Nome do arquivo sendo processado.
            file_hash: Hash do arquivo.
            status: Status inicial do log.

        Returns:
            Objeto LogETL criado.
        """
        log = LogETL(
            arquivo_processado=file_name,
            arquivo_hash=file_hash,
            status_execucao=status,
        )
        session.add(log)
        session.flush()  # Obter o ID antes do commit
        return log

    def update_log_entry(
        self,
        session: Session,
        log: LogETL,
        status: str,
        records_read: int = 0,
        records_inserted: int = 0,
        records_rejected: int = 0,
        duplicates_ignored: int = 0,
        execution_time: float = 0.0,
        error_message: Optional[str] = None,
        details: Optional[Dict] = None,
    ) -> None:
        """
        Atualiza uma entrada de log existente.

        Args:
            session: Sessão do SQLAlchemy.
            log: Objeto LogETL a ser atualizado.
            status: Novo status.
            records_read: Quantidade de registros lidos.
            records_inserted: Quantidade de registros inseridos.
            records_rejected: Quantidade de registros rejeitados.
            duplicates_ignored: Quantidade de duplicatas ignoradas.
            execution_time: Tempo de execução em segundos.
            error_message: Mensagem de erro, se houver.
            details: Detalhes adicionais em JSON.
        """
        log.status_execucao = status
        log.qtd_registros_lidos = records_read
        log.qtd_registros_inseridos = records_inserted
        log.qtd_registros_rejeitados = records_rejected
        log.qtd_duplicatas_ignoradas = duplicates_ignored
        log.tempo_execucao_seg = Decimal(str(round(execution_time, 3)))
        log.mensagem_erro = error_message
        log.detalhes = details

    def insert_batch(
        self, session: Session, df: pd.DataFrame, batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Insere dados em lote usando upsert (insert on conflict).

        Args:
            session: Sessão do SQLAlchemy.
            df: DataFrame com os dados a inserir.
            batch_size: Tamanho do lote.

        Returns:
            Dicionário com estatísticas da inserção.
        """
        stats = {
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
        }

        # Converter DataFrame para lista de dicionários
        records = df.to_dict("records")

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            try:
                for record in batch:
                    # Preparar dados para inserção
                    transacao = Transacao(
                        id_transacao=str(record.get("id_transacao")),
                        data_transacao=record.get("data_transacao"),
                        cliente=str(record.get("cliente")),
                        produto=str(record.get("produto")),
                        categoria=str(record.get("categoria")),
                        valor=Decimal(str(record.get("valor", 0))),
                        status_pagamento=str(record.get("status_pagamento")),
                        data_pagamento=record.get("data_pagamento"),
                        ano_transacao=int(record.get("ano_transacao", 0)),
                        mes_transacao=int(record.get("mes_transacao", 0)),
                        dia_semana=record.get("dia_semana"),
                        trimestre=record.get("trimestre"),
                        arquivo_origem=str(record.get("arquivo_origem", "")),
                    )

                    # Tentar inserir
                    try:
                        session.add(transacao)
                        session.flush()
                        stats["inserted"] += 1
                    except IntegrityError:
                        session.rollback()
                        stats["skipped"] += 1
                        logger.debug(f"Duplicata ignorada: {record.get('id_transacao')}")

            except Exception as e:
                logger.error(f"Erro no lote {i//batch_size + 1}: {str(e)}")
                stats["failed"] += len(batch) - stats["inserted"] - stats["skipped"]
                session.rollback()

            logger.debug(f"Lote {i//batch_size + 1}: {len(batch)} registros processados")

        return stats

    def insert_batch_bulk(
        self, session: Session, df: pd.DataFrame, batch_size: int = 1000
    ) -> Dict[str, int]:
        """
        Insere dados em lote usando bulk insert com ON CONFLICT DO NOTHING.

        Mais eficiente que insert_batch para grandes volumes.

        Args:
            session: Sessão do SQLAlchemy.
            df: DataFrame com os dados a inserir.
            batch_size: Tamanho do lote.

        Returns:
            Dicionário com estatísticas da inserção.
        """
        stats = {
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
        }

        # Converter DataFrame para lista de dicionários
        records = df.to_dict("records")

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            # Preparar dados para inserção
            values = []
            for record in batch:
                values.append(
                    {
                        "id_transacao": str(record.get("id_transacao")),
                        "data_transacao": record.get("data_transacao"),
                        "cliente": str(record.get("cliente")),
                        "produto": str(record.get("produto")),
                        "categoria": str(record.get("categoria")),
                        "valor": Decimal(str(record.get("valor", 0))),
                        "status_pagamento": str(record.get("status_pagamento")),
                        "data_pagamento": (
                            record.get("data_pagamento")
                            if pd.notna(record.get("data_pagamento"))
                            else None
                        ),
                        "ano_transacao": int(record.get("ano_transacao", 0)),
                        "mes_transacao": int(record.get("mes_transacao", 0)),
                        "dia_semana": (
                            record.get("dia_semana") if pd.notna(record.get("dia_semana")) else None
                        ),
                        "trimestre": (
                            record.get("trimestre") if pd.notna(record.get("trimestre")) else None
                        ),
                        "arquivo_origem": str(record.get("arquivo_origem", "")),
                    }
                )

            try:
                # Usar insert com on_conflict_do_nothing
                stmt = insert(Transacao).values(values)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["id_transacao", "arquivo_origem"]
                )

                result = session.execute(stmt)
                inserted = result.rowcount
                stats["inserted"] += inserted
                stats["skipped"] += len(batch) - inserted

                logger.debug(
                    f"Lote {i//batch_size + 1}: {inserted} inseridos, "
                    f"{len(batch) - inserted} ignorados"
                )

            except Exception as e:
                logger.error(f"Erro no lote {i//batch_size + 1}: {str(e)}")
                stats["failed"] += len(batch)
                session.rollback()

        return stats

    def load(
        self,
        df: pd.DataFrame,
        file_name: str,
        file_path: str,
        file_hash: str,
        file_size: int = 0,
        use_bulk: bool = True,
    ) -> LoadResult:
        """
        Carrega os dados no banco de dados.

        Args:
            df: DataFrame com os dados transformados.
            file_name: Nome do arquivo de origem.
            file_path: Caminho completo do arquivo.
            file_hash: Hash MD5 do arquivo.
            file_size: Tamanho do arquivo em bytes.
            use_bulk: Se True, usa bulk insert (mais eficiente).

        Returns:
            LoadResult com o resultado da carga.
        """
        start_time = datetime.now()

        logger.info(f"Iniciando carga de {len(df)} registros de {file_name}")

        # Verificar conexão
        if not self._connected:
            if not self.connect():
                return LoadResult(
                    success=False, error_message="Não foi possível conectar ao banco de dados"
                )

        # Garantir que as tabelas existem
        if not self.ensure_tables_exist():
            return LoadResult(
                success=False, error_message="Não foi possível criar/verificar as tabelas"
            )

        # Verificar se arquivo já foi processado
        if self.check_file_processed(file_hash):
            logger.warning(f"Arquivo já processado anteriormente: {file_name}")
            return LoadResult(
                success=True,
                records_skipped=len(df),
                error_message="Arquivo já processado anteriormente",
            )

        # Iniciar sessão e transação
        with self.Session() as session:
            try:
                # Criar log de execução
                log = self.create_log_entry(session, file_name, file_hash)

                # Inserir dados
                batch_size = self.settings.etl_batch_size
                if use_bulk:
                    stats = self.insert_batch_bulk(session, df, batch_size)
                else:
                    stats = self.insert_batch(session, df, batch_size)

                # Calcular tempo de execução
                execution_time = (datetime.now() - start_time).total_seconds()

                # Determinar status final
                if stats["failed"] > 0:
                    status = "PARCIAL" if stats["inserted"] > 0 else "ERRO"
                else:
                    status = "SUCESSO"

                # Atualizar log
                self.update_log_entry(
                    session=session,
                    log=log,
                    status=status,
                    records_read=len(df),
                    records_inserted=stats["inserted"],
                    records_rejected=stats["failed"],
                    duplicates_ignored=stats["skipped"],
                    execution_time=execution_time,
                    details={
                        "batch_size": batch_size,
                        "use_bulk": use_bulk,
                    },
                )

                # Registrar arquivo como processado
                self.register_file_processed(
                    session=session,
                    file_name=file_name,
                    file_path=file_path,
                    file_hash=file_hash,
                    file_size=file_size,
                    log_id=log.id_log,
                )

                # Commit da transação
                session.commit()

                logger.success(
                    f"Carga concluída: {stats['inserted']} inseridos, "
                    f"{stats['skipped']} ignorados, {stats['failed']} falhas "
                    f"({execution_time:.2f}s)"
                )

                return LoadResult(
                    success=True,
                    records_inserted=stats["inserted"],
                    records_skipped=stats["skipped"],
                    records_failed=stats["failed"],
                    log_id=log.id_log,
                    execution_time=execution_time,
                )

            except Exception as e:
                session.rollback()
                execution_time = (datetime.now() - start_time).total_seconds()

                logger.error(f"Erro na carga: {str(e)}")

                return LoadResult(
                    success=False,
                    error_message=str(e),
                    execution_time=execution_time,
                )


def load(
    df: pd.DataFrame, file_name: str, file_path: str, file_hash: str, file_size: int = 0
) -> LoadResult:
    """
    Função principal de carga.

    Args:
        df: DataFrame com os dados transformados.
        file_name: Nome do arquivo de origem.
        file_path: Caminho completo do arquivo.
        file_hash: Hash MD5 do arquivo.
        file_size: Tamanho do arquivo em bytes.

    Returns:
        LoadResult com o resultado da carga.
    """
    loader = DataLoader()
    return loader.load(df, file_name, file_path, file_hash, file_size)


if __name__ == "__main__":
    # Teste do módulo de carga
    logger.info("Iniciando teste do módulo de carga...")

    # Criar dados de teste
    test_data = pd.DataFrame(
        {
            "id_transacao": ["TEST001", "TEST002", "TEST003"],
            "data_transacao": pd.to_datetime(["2024-01-01", "2024-01-15", "2024-01-20"]),
            "cliente": ["João Silva", "Maria Santos", "Pedro Costa"],
            "produto": ["Notebook", "Mouse", "Teclado"],
            "categoria": ["Eletrônicos", "Periféricos", "Periféricos"],
            "valor": [2500.00, 50.00, 150.00],
            "status_pagamento": ["PAGO", "PENDENTE", "CANCELADO"],
            "data_pagamento": pd.to_datetime(["2024-01-05", None, None]),
            "ano_transacao": [2024, 2024, 2024],
            "mes_transacao": [1, 1, 1],
            "dia_semana": [0, 0, 5],
            "trimestre": [1, 1, 1],
            "arquivo_origem": ["test_load.csv"] * 3,
        }
    )

    result = load(
        df=test_data,
        file_name="test_load.csv",
        file_path="/tmp/test_load.csv",
        file_hash="test_hash_123",
    )

    if result.success:
        print("\n✓ Carga bem-sucedida!")
        print(f"  Inseridos: {result.records_inserted}")
        print(f"  Ignorados: {result.records_skipped}")
        print(f"  Falhas: {result.records_failed}")
        print(f"  Tempo: {result.execution_time:.2f}s")
    else:
        print(f"\n✗ Erro na carga: {result.error_message}")
