"""
ETL Pipeline - Módulo de Carga
==============================

Responsável pela carga de dados no PostgreSQL.
Implementa inserção em lote, controle de duplicatas e logging.

Autor: Kaio Ambrosio
"""

import csv
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
    Categoria,
    LogETL,
    Produto,
    Transacao,
    TransacaoItem,
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
                stmt = stmt.on_conflict_do_nothing(index_elements=["id_transacao"])

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


    def _copy_from_csv(self, table: str, columns: list[str], file_path: Path) -> None:
        """
        Executa COPY FROM STDIN para carregar CSV em tabela.

        Args:
            table: Nome da tabela de destino.
            columns: Lista de colunas na ordem do arquivo.
            file_path: Caminho do CSV.
        """
        copy_sql = (
            f"COPY {table} ({', '.join(columns)}) FROM STDIN "
            "WITH (FORMAT csv, DELIMITER ',', NULL '')"
        )

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                with file_path.open("r", encoding="utf-8") as handle:
                    cursor.copy_expert(copy_sql, handle)
            raw_conn.commit()
        finally:
            raw_conn.close()

    def load_via_copy(
        self,
        df: pd.DataFrame,
        file_name: str,
        file_path: str,
        file_hash: str,
        file_size: int = 0,
    ) -> LoadResult:
        """
        Carrega dados usando COPY nativo do PostgreSQL.
        """
        start_time = datetime.now()

        if not self._connected:
            if not self.connect():
                return LoadResult(
                    success=False,
                    error_message="Nao foi possivel conectar ao banco de dados",
                )

        if not self.ensure_tables_exist():
            return LoadResult(
                success=False,
                error_message="Nao foi possivel criar/verificar as tabelas",
            )

        if self.check_file_processed(file_hash):
            logger.warning(f"Arquivo ja processado anteriormente: {file_name}")
            return LoadResult(
                success=True,
                records_skipped=len(df),
                error_message="Arquivo ja processado anteriormente",
            )

        base_columns = [
            "id_transacao",
            "data_transacao",
            "cliente",
            "produto",
            "categoria",
            "valor",
            "status_pagamento",
            "data_pagamento",
            "ano_transacao",
            "mes_transacao",
            "dia_semana",
            "trimestre",
            "arquivo_origem",
        ]
        copy_columns = base_columns + ["data_processamento"]
        missing = [col for col in base_columns if col not in df.columns]
        if missing:
            return LoadResult(
                success=False,
                error_message=f"Colunas ausentes para COPY: {missing}",
            )

        df_copy = df[base_columns].copy()
        df_copy["data_processamento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_copy["data_processamento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for col in ["data_transacao", "data_pagamento"]:
            if col in df_copy.columns:
                df_copy[col] = pd.to_datetime(df_copy[col], errors="coerce")
                df_copy[col] = df_copy[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        df_copy = df_copy.where(pd.notna(df_copy), "")

        temp_dir = self.settings.processed_data_path
        temp_dir.mkdir(parents=True, exist_ok=True)
        temp_path = temp_dir / f"transacoes_copy_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        df_copy.to_csv(
            temp_path,
            index=False,
            header=False,
            sep=",",
            encoding="utf-8",
            quoting=csv.QUOTE_MINIMAL,
        )

        with self.Session() as session:
            log = self.create_log_entry(session, file_name, file_hash)
            session.commit()
            log_id = log.id_log

        try:
            self._copy_from_csv("transacoes", copy_columns, temp_path)
            inserted = len(df_copy)
            execution_time = (datetime.now() - start_time).total_seconds()

            with self.Session() as session:
                log_db = session.get(LogETL, log_id)
                if not log_db:
                    raise RuntimeError("Log ETL nao encontrado para atualizacao")
                self.update_log_entry(
                    session=session,
                    log=log_db,
                    status="SUCESSO",
                    records_read=len(df_copy),
                    records_inserted=inserted,
                    records_rejected=0,
                    duplicates_ignored=0,
                    execution_time=execution_time,
                    details={
                        "batch_size": self.settings.etl_batch_size,
                        "use_copy": True,
                    },
                )
                self.register_file_processed(
                    session=session,
                    file_name=file_name,
                    file_path=file_path,
                    file_hash=file_hash,
                    file_size=file_size,
                    log_id=log_id,
                )
                session.commit()

            logger.success(
                f"Carga (COPY) concluida: {inserted} inseridos ({execution_time:.2f}s)"
            )

            return LoadResult(
                success=True,
                records_inserted=inserted,
                records_skipped=0,
                records_failed=0,
                log_id=log.id_log,
                execution_time=execution_time,
            )
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Erro na carga COPY: {str(e)}")

            with self.Session() as session:
                log_db = session.get(LogETL, log_id)
                if not log_db:
                    raise RuntimeError("Log ETL nao encontrado para atualizacao")
                self.update_log_entry(
                    session=session,
                    log=log_db,
                    status="ERRO",
                    records_read=len(df_copy),
                    records_inserted=0,
                    records_rejected=len(df_copy),
                    duplicates_ignored=0,
                    execution_time=execution_time,
                    error_message=str(e),
                )
                session.commit()

            return LoadResult(
                success=False,
                error_message=str(e),
                execution_time=execution_time,
            )
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except Exception:
                pass

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
        use_copy = self.settings.etl_use_copy and len(df) >= self.settings.etl_copy_threshold
        if use_copy:
            logger.info("COPY habilitado para carga de transacoes")
            return self.load_via_copy(
                df=df,
                file_name=file_name,
                file_path=file_path,
                file_hash=file_hash,
                file_size=file_size,
            )

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

    def load_catalog(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Carrega catálogo de categorias e produtos.

        Espera colunas:
        - categoria
        - categoria_descricao (opcional)
        - produto
        - descricao
        - preco_base
        - preco_min
        - preco_max
        - ativo (opcional)
        """
        if not self._connected:
            if not self.connect():
                return {"categorias": 0, "produtos": 0, "error": 1}

        if not self.ensure_tables_exist():
            return {"categorias": 0, "produtos": 0, "error": 1}

        df = df.copy()
        df.columns = df.columns.str.lower().str.strip()

        required = {"categoria", "produto", "descricao", "preco_base", "preco_min", "preco_max"}
        if not required.issubset(set(df.columns)):
            missing = sorted(required - set(df.columns))
            logger.error(f"Catálogo inválido: colunas ausentes {missing}")
            return {"categorias": 0, "produtos": 0, "error": 1}

        df["categoria_descricao"] = df.get("categoria_descricao", "").fillna("")
        df["ativo"] = df.get("ativo", True)

        categorias_rows = (
            df[["categoria", "categoria_descricao"]]
            .drop_duplicates()
            .rename(columns={"categoria": "nome", "categoria_descricao": "descricao"})
            .to_dict("records")
        )

        produtos_rows = (
            df[
                [
                    "categoria",
                    "produto",
                    "descricao",
                    "preco_base",
                    "preco_min",
                    "preco_max",
                    "ativo",
                ]
            ]
            .rename(columns={"produto": "nome"})
            .to_dict("records")
        )

        with self.Session() as session:
            try:
                if categorias_rows:
                    stmt = insert(Categoria).values(categorias_rows)
                    stmt = stmt.on_conflict_do_nothing(index_elements=["nome"])
                    session.execute(stmt)

                categorias_db = session.query(Categoria.id, Categoria.nome).all()
                categoria_map = {c.nome: c.id for c in categorias_db}

                produtos_payload = []
                for row in produtos_rows:
                    categoria_id = categoria_map.get(row["categoria"])
                    if not categoria_id:
                        continue
                    produtos_payload.append(
                        {
                            "categoria_id": categoria_id,
                            "nome": row["nome"],
                            "descricao": row.get("descricao"),
                            "preco_base": Decimal(str(row.get("preco_base", 0))),
                            "preco_min": Decimal(str(row.get("preco_min", 0))),
                            "preco_max": Decimal(str(row.get("preco_max", 0))),
                            "ativo": bool(row.get("ativo", True)),
                        }
                    )

                if produtos_payload:
                    stmt = insert(Produto).values(produtos_payload)
                    stmt = stmt.on_conflict_do_nothing(index_elements=["nome"])
                    session.execute(stmt)

                session.commit()
                return {
                    "categorias": len(categorias_rows),
                    "produtos": len(produtos_payload),
                    "error": 0,
                }

            except Exception as e:
                session.rollback()
                logger.error(f"Erro ao carregar catálogo: {str(e)}")
                return {"categorias": 0, "produtos": 0, "error": 1}


    def load_items_from_file_copy(self, file_path: Path) -> Dict[str, int]:
        """
        Carrega itens de transacoes usando COPY para tabela temporaria.
        """
        if not self._connected:
            if not self.connect():
                return {"inserted": 0, "failed": 0, "skipped": 0}

        if not self.ensure_tables_exist():
            return {"inserted": 0, "failed": 0, "skipped": 0}

        total = 0
        inserted = 0

        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                cursor.execute("DROP TABLE IF EXISTS stg_transacao_itens")
                cursor.execute(
                    """
                    CREATE TEMP TABLE stg_transacao_itens (
                        id_transacao TEXT,
                        produto TEXT,
                        quantidade INTEGER,
                        valor_unitario NUMERIC(15, 2),
                        valor_total NUMERIC(15, 2)
                    )
                    """
                )

                copy_sql = (
                    "COPY stg_transacao_itens (id_transacao, produto, quantidade, "
                    "valor_unitario, valor_total) FROM STDIN "
                    "WITH (FORMAT csv, HEADER true, DELIMITER ',')"
                )
                with file_path.open("r", encoding="utf-8") as handle:
                    cursor.copy_expert(copy_sql, handle)

                cursor.execute("SELECT COUNT(*) FROM stg_transacao_itens")
                total = cursor.fetchone()[0]

                cursor.execute(
                    """
                    INSERT INTO transacao_itens (
                        id_transacao, produto_id, quantidade, valor_unitario, valor_total
                    )
                    SELECT
                        s.id_transacao,
                        p.id,
                        s.quantidade,
                        s.valor_unitario,
                        s.valor_total
                    FROM stg_transacao_itens s
                    JOIN produtos p ON p.nome = s.produto
                    """
                )
                inserted = cursor.rowcount

            raw_conn.commit()
            skipped = max(0, total - inserted)
            return {"inserted": inserted, "failed": 0, "skipped": skipped}

        except Exception as e:
            raw_conn.rollback()
            logger.error(f"Erro ao inserir itens via COPY: {str(e)}")
            return {"inserted": 0, "failed": total if total else 1, "skipped": 0}
        finally:
            raw_conn.close()

    def load_items_from_file(
        self, file_path: Path, batch_size: int = 100000, use_copy: Optional[bool] = None
    ) -> Dict[str, int]:
        """
        Carrega itens de transações em lote (streaming).

        Espera colunas:
        - id_transacao
        - produto
        - quantidade
        - valor_unitario
        - valor_total
        """
        if use_copy is None:
            use_copy = self.settings.etl_use_copy
        if use_copy:
            logger.info("COPY habilitado para carga de itens")
            return self.load_items_from_file_copy(file_path)
        if not self._connected:
            if not self.connect():
                return {"inserted": 0, "failed": 0, "skipped": 0}

        if not self.ensure_tables_exist():
            return {"inserted": 0, "failed": 0, "skipped": 0}

        inserted = 0
        failed = 0
        skipped = 0

        with self.Session() as session:
            produtos_db = session.query(Produto.id, Produto.nome).all()
            produto_map = {p.nome: p.id for p in produtos_db}

        for chunk in pd.read_csv(file_path, chunksize=batch_size):
            chunk.columns = chunk.columns.str.lower().str.strip()
            required = {"id_transacao", "produto", "quantidade", "valor_unitario", "valor_total"}
            if not required.issubset(set(chunk.columns)):
                missing = sorted(required - set(chunk.columns))
                logger.error(f"Itens inválido: colunas ausentes {missing}")
                failed += len(chunk)
                continue

            values = []
            for row in chunk.to_dict("records"):
                produto_id = produto_map.get(str(row.get("produto")).strip())
                if not produto_id:
                    skipped += 1
                    continue
                values.append(
                    {
                        "id_transacao": str(row.get("id_transacao")),
                        "produto_id": produto_id,
                        "quantidade": int(row.get("quantidade", 1)),
                        "valor_unitario": Decimal(str(row.get("valor_unitario", 0))),
                        "valor_total": Decimal(str(row.get("valor_total", 0))),
                    }
                )

            if not values:
                continue

            with self.Session() as session:
                try:
                    session.execute(insert(TransacaoItem).values(values))
                    session.commit()
                    inserted += len(values)
                except Exception as e:
                    session.rollback()
                    failed += len(values)
                    logger.error(f"Erro ao inserir itens: {str(e)}")

        return {"inserted": inserted, "failed": failed, "skipped": skipped}


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
