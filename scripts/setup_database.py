"""
ETL Pipeline - Setup do Banco de Dados
======================================

Script para criar o banco de dados e as tabelas necessárias.

Autor: Kaio Ambrosio
"""

import sys
from pathlib import Path

# Adicionar diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from loguru import logger
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config.settings import get_settings


def create_database():
    """Cria o banco de dados se não existir."""
    settings = get_settings()

    logger.info("Conectando ao PostgreSQL...")

    # Conectar ao banco postgres (padrão) para criar o novo banco
    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            user=settings.db_user,
            password=settings.db_password,
            database="postgres",  # Conecta ao banco padrão
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cursor = conn.cursor()

        # Verificar se o banco já existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (settings.db_name,))

        if cursor.fetchone():
            logger.info(f"Banco de dados '{settings.db_name}' já existe.")
        else:
            # Criar o banco de dados
            cursor.execute(f'CREATE DATABASE "{settings.db_name}"')
            logger.success(f"Banco de dados '{settings.db_name}' criado com sucesso!")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        return False


def create_tables():
    """Cria as tabelas usando SQLAlchemy."""
    from scripts.models import Base, get_engine

    logger.info("Criando tabelas...")

    try:
        engine = get_engine()
        Base.metadata.create_all(engine)
        logger.success("Tabelas criadas com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")
        return False


def main():
    """Função principal de setup."""
    logger.info("=" * 50)
    logger.info("ETL Pipeline - Setup do Banco de Dados")
    logger.info("=" * 50)

    settings = get_settings()
    logger.info(f"Host: {settings.db_host}")
    logger.info(f"Port: {settings.db_port}")
    logger.info(f"Database: {settings.db_name}")
    logger.info(f"User: {settings.db_user}")
    logger.info("=" * 50)

    # Criar banco de dados
    if not create_database():
        logger.error("Falha ao criar banco de dados. Abortando.")
        sys.exit(1)

    # Criar tabelas
    if not create_tables():
        logger.error("Falha ao criar tabelas. Abortando.")
        sys.exit(1)

    logger.info("=" * 50)
    logger.success("Setup concluído com sucesso!")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
