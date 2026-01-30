"""
ETL Pipeline - Configurações
============================

Módulo de configuração centralizado usando Pydantic Settings.
Carrega variáveis de ambiente do arquivo .env e valida os valores.

Autor: Kaio Ambrosio
"""

from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Diretório raiz do projeto
ROOT_DIR = Path(__file__).parent.parent


class Settings(BaseSettings):
    """
    Configurações do Pipeline ETL.

    Carrega automaticamente variáveis do arquivo .env
    e valida os valores usando Pydantic.
    """

    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )

    # -------------------------------------------------------------------------
    # Configurações do Banco de Dados PostgreSQL
    # -------------------------------------------------------------------------
    db_host: str = Field(default="localhost", description="Host do PostgreSQL")
    db_port: int = Field(default=5432, description="Porta do PostgreSQL")
    db_name: str = Field(default="etl_portfolio", description="Nome do banco de dados")
    db_user: str = Field(default="postgres", description="Usuário do banco")
    db_password: str = Field(default="", description="Senha do banco")

    # -------------------------------------------------------------------------
    # Configurações do Pipeline ETL
    # -------------------------------------------------------------------------
    etl_log_level: str = Field(default="INFO", description="Nível de log")
    etl_batch_size: int = Field(default=1000, description="Tamanho do lote para inserção")
    etl_use_copy: bool = Field(
        default=False, description="Usar COPY nativo do PostgreSQL para cargas grandes"
    )
    etl_copy_threshold: int = Field(
        default=200000, description="Quantidade minima de registros para ativar COPY"
    )
    etl_data_raw_path: str = Field(default="data/raw", description="Caminho dos dados brutos")
    etl_data_processed_path: str = Field(
        default="data/processed", description="Caminho dos dados processados"
    )

    # -------------------------------------------------------------------------
    # Configurações da API
    # -------------------------------------------------------------------------
    api_cors_origins: str = Field(
        default="http://localhost:5173",
        description="Origens permitidas para CORS (separadas por vírgula)",
    )
    api_snapshot_limit: int = Field(
        default=2000,
        description="Quantidade máxima de registros para snapshot do dashboard",
    )

    @field_validator("etl_log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Valida o nível de log."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Nível de log inválido: {v}. Use: {valid_levels}")
        return v_upper

    @property
    def database_url(self) -> str:
        """Retorna a URL de conexão completa do PostgreSQL."""
        # URL encode da senha para tratar caracteres especiais como @, #, etc.
        encoded_password = quote_plus(self.db_password)
        return (
            f"postgresql://{self.db_user}:{encoded_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def database_url_masked(self) -> str:
        """Retorna a URL de conexão com senha mascarada (para logs)."""
        return f"postgresql://{self.db_user}:****" f"@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def raw_data_path(self) -> Path:
        """Retorna o caminho absoluto para dados brutos."""
        return ROOT_DIR / self.etl_data_raw_path

    @property
    def processed_data_path(self) -> Path:
        """Retorna o caminho absoluto para dados processados."""
        return ROOT_DIR / self.etl_data_processed_path

    @property
    def logs_path(self) -> Path:
        """Retorna o caminho absoluto para logs."""
        return ROOT_DIR / "logs"


@lru_cache
def get_settings() -> Settings:
    """
    Retorna uma instância cacheada das configurações.

    Usar esta função garante que as configurações são carregadas
    apenas uma vez durante a execução do programa.

    Returns:
        Settings: Instância das configurações do pipeline.

    Example:
        >>> from config import get_settings
        >>> settings = get_settings()
        >>> print(settings.db_host)
        'localhost'
    """
    return Settings()


if __name__ == "__main__":
    # Teste rápido das configurações
    settings = get_settings()
    print("=" * 60)
    print("ETL Pipeline - Configurações Carregadas")
    print("=" * 60)
    print(f"Database URL: {settings.database_url_masked}")
    print(f"Log Level: {settings.etl_log_level}")
    print(f"Batch Size: {settings.etl_batch_size}")
    print(f"Raw Data Path: {settings.raw_data_path}")
    print(f"Processed Data Path: {settings.processed_data_path}")
    print("=" * 60)
