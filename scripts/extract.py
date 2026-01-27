"""
ETL Pipeline - Módulo de Extração
=================================

Responsável pela extração de dados de arquivos CSV e Excel.
Implementa validação de estrutura e tratamento de erros.

Autor: Kaio Ambrosio
"""

import hashlib
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings

# Colunas obrigatórias esperadas nos arquivos de entrada
REQUIRED_COLUMNS = {
    "id_transacao",
    "data_transacao",
    "cliente",
    "produto",
    "categoria",
    "valor",
    "status_pagamento",
}

# Colunas opcionais
OPTIONAL_COLUMNS = {"data_pagamento"}

# Extensões de arquivo suportadas
SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


@dataclass
class ExtractionResult:
    """
    Resultado da extração de um arquivo.

    Attributes:
        success: Se a extração foi bem-sucedida.
        dataframe: DataFrame com os dados extraídos.
        file_path: Caminho do arquivo processado.
        file_hash: Hash MD5 do arquivo para controle de duplicatas.
        records_count: Quantidade de registros extraídos.
        error_message: Mensagem de erro, se houver.
        warnings: Lista de avisos durante a extração.
    """

    success: bool
    dataframe: Optional[pd.DataFrame] = None
    file_path: str = ""
    file_hash: str = ""
    records_count: int = 0
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


class DataExtractor:
    """
    Classe responsável pela extração de dados de arquivos.

    Suporta arquivos CSV e Excel (.xlsx, .xls).
    Realiza validação de estrutura e cálculo de hash para controle.
    """

    def __init__(self, raw_data_path: Optional[Path] = None):
        """
        Inicializa o extrator de dados.

        Args:
            raw_data_path: Caminho para a pasta de dados brutos.
                          Se não informado, usa o padrão das configurações.
        """
        self.settings = get_settings()
        self.raw_data_path = raw_data_path or self.settings.raw_data_path

        logger.info(f"DataExtractor inicializado. Pasta de dados: {self.raw_data_path}")

    def calculate_file_hash(self, file_path: Path) -> str:
        """
        Calcula o hash MD5 de um arquivo.

        Args:
            file_path: Caminho do arquivo.

        Returns:
            Hash MD5 em hexadecimal.
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def list_files(self) -> List[Path]:
        """
        Lista todos os arquivos suportados na pasta de dados brutos.

        Returns:
            Lista de caminhos dos arquivos encontrados.
        """
        if not self.raw_data_path.exists():
            logger.warning(f"Pasta não encontrada: {self.raw_data_path}")
            return []

        files = []
        for ext in SUPPORTED_EXTENSIONS:
            files.extend(self.raw_data_path.glob(f"*{ext}"))

        logger.info(f"Encontrados {len(files)} arquivo(s) para processar")
        return sorted(files)

    def validate_columns(self, df: pd.DataFrame, file_path: Path) -> Tuple[bool, List[str]]:
        """
        Valida se o DataFrame possui as colunas obrigatórias.

        Args:
            df: DataFrame a ser validado.
            file_path: Caminho do arquivo (para mensagens de log).

        Returns:
            Tupla (é_válido, lista_de_avisos).
        """
        warnings = []

        # Normalizar nomes das colunas para comparação
        df_columns = {col.lower().strip() for col in df.columns}

        # Verificar colunas obrigatórias
        missing_required = REQUIRED_COLUMNS - df_columns
        if missing_required:
            return False, [f"Colunas obrigatórias ausentes: {missing_required}"]

        # Verificar colunas opcionais
        missing_optional = OPTIONAL_COLUMNS - df_columns
        if missing_optional:
            warnings.append(f"Colunas opcionais ausentes: {missing_optional}")

        # Verificar colunas extras (informativo)
        expected_columns = REQUIRED_COLUMNS | OPTIONAL_COLUMNS
        extra_columns = df_columns - expected_columns
        if extra_columns:
            warnings.append(f"Colunas extras encontradas (serão ignoradas): {extra_columns}")

        return True, warnings

    def read_csv(self, file_path: Path) -> pd.DataFrame:
        """
        Lê um arquivo CSV.

        Args:
            file_path: Caminho do arquivo CSV.

        Returns:
            DataFrame com os dados do arquivo.
        """
        # Tenta diferentes encodings comuns (utf-8-sig trata BOM automaticamente)
        encodings = ["utf-8-sig", "utf-8", "latin-1", "cp1252", "iso-8859-1"]

        for encoding in encodings:
            try:
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    sep=None,  # Detecta separador automaticamente
                    engine="python",
                    on_bad_lines="warn",
                )
                logger.debug(f"Arquivo lido com encoding: {encoding}")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.debug(f"Erro com encoding {encoding}: {e}")
                continue

        # Se nenhum encoding funcionar, tenta com errors='replace'
        return pd.read_csv(
            file_path,
            encoding="utf-8-sig",
            errors="replace",
            sep=None,
            engine="python",
        )

    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """
        Lê um arquivo Excel (.xlsx ou .xls).

        Args:
            file_path: Caminho do arquivo Excel.

        Returns:
            DataFrame com os dados do arquivo.
        """
        engine = "openpyxl" if file_path.suffix == ".xlsx" else "xlrd"
        return pd.read_excel(file_path, engine=engine)

    def extract_file(self, file_path: Path) -> ExtractionResult:
        """
        Extrai dados de um arquivo individual.

        Args:
            file_path: Caminho do arquivo a ser extraído.

        Returns:
            ExtractionResult com os dados extraídos ou informações de erro.
        """
        logger.info(f"Extraindo arquivo: {file_path.name}")

        # Verificar se o arquivo existe
        if not file_path.exists():
            return ExtractionResult(
                success=False,
                file_path=str(file_path),
                error_message=f"Arquivo não encontrado: {file_path}",
            )

        # Verificar extensão suportada
        if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
            return ExtractionResult(
                success=False,
                file_path=str(file_path),
                error_message=f"Extensão não suportada: {file_path.suffix}",
            )

        try:
            # Calcular hash do arquivo
            file_hash = self.calculate_file_hash(file_path)

            # Ler arquivo de acordo com a extensão
            if file_path.suffix.lower() == ".csv":
                df = self.read_csv(file_path)
            else:
                df = self.read_excel(file_path)

            # Normalizar nomes das colunas
            df.columns = df.columns.str.lower().str.strip()

            # Validar estrutura
            is_valid, warnings = self.validate_columns(df, file_path)
            if not is_valid:
                return ExtractionResult(
                    success=False,
                    file_path=str(file_path),
                    file_hash=file_hash,
                    error_message=warnings[0] if warnings else "Estrutura inválida",
                )

            # Adicionar metadados
            df["_arquivo_origem"] = file_path.name
            df["_arquivo_hash"] = file_hash

            logger.success(f"Extraído com sucesso: {len(df)} registros de {file_path.name}")

            return ExtractionResult(
                success=True,
                dataframe=df,
                file_path=str(file_path),
                file_hash=file_hash,
                records_count=len(df),
                warnings=warnings,
            )

        except Exception as e:
            logger.error(f"Erro ao extrair {file_path.name}: {str(e)}")
            return ExtractionResult(
                success=False,
                file_path=str(file_path),
                error_message=str(e),
            )

    def extract_all(self) -> List[ExtractionResult]:
        """
        Extrai dados de todos os arquivos na pasta de dados brutos.

        Returns:
            Lista de ExtractionResult para cada arquivo processado.
        """
        files = self.list_files()
        results = []

        for file_path in files:
            result = self.extract_file(file_path)
            results.append(result)

        # Resumo
        success_count = sum(1 for r in results if r.success)
        total_records = sum(r.records_count for r in results if r.success)

        logger.info(
            f"Extração concluída: {success_count}/{len(results)} arquivos, "
            f"{total_records} registros no total"
        )

        return results


def extract(file_path: Optional[str] = None) -> List[ExtractionResult]:
    """
    Função principal de extração.

    Args:
        file_path: Caminho específico de um arquivo para extrair.
                  Se não informado, extrai todos os arquivos da pasta padrão.

    Returns:
        Lista de resultados da extração.
    """
    extractor = DataExtractor()

    if file_path:
        return [extractor.extract_file(Path(file_path))]
    else:
        return extractor.extract_all()


if __name__ == "__main__":
    # Teste do módulo de extração
    logger.info("Iniciando teste do módulo de extração...")

    results = extract()

    for result in results:
        if result.success:
            print(f"✓ {result.file_path}: {result.records_count} registros")
            if result.warnings:
                for warning in result.warnings:
                    print(f"  ⚠ {warning}")
        else:
            print(f"✗ {result.file_path}: {result.error_message}")
