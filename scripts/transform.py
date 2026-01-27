"""
ETL Pipeline - Módulo de Transformação
======================================

Responsável pela transformação, limpeza e padronização dos dados.
Aplica regras de negócio e cria campos derivados.

Autor: Kaio Ambrosio
"""

import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings

# Mapeamento de normalização de status de pagamento
STATUS_MAPPING = {
    # Variações de PAGO
    "pago": "PAGO",
    "paid": "PAGO",
    "pg": "PAGO",
    "quitado": "PAGO",
    "concluido": "PAGO",
    "concluído": "PAGO",
    "completed": "PAGO",
    "aprovado": "PAGO",
    # Variações de PENDENTE
    "pendente": "PENDENTE",
    "pending": "PENDENTE",
    "aguardando": "PENDENTE",
    "em aberto": "PENDENTE",
    "aberto": "PENDENTE",
    "waiting": "PENDENTE",
    # Variações de CANCELADO
    "cancelado": "CANCELADO",
    "cancelled": "CANCELADO",
    "canceled": "CANCELADO",
    "estornado": "CANCELADO",
    "refunded": "CANCELADO",
    "devolvido": "CANCELADO",
    # Variações de ATRASADO
    "atrasado": "ATRASADO",
    "vencido": "ATRASADO",
    "overdue": "ATRASADO",
    "late": "ATRASADO",
    "em atraso": "ATRASADO",
}

# Status válidos após normalização
VALID_STATUS = {"PAGO", "PENDENTE", "CANCELADO", "ATRASADO"}


@dataclass
class TransformationResult:
    """
    Resultado da transformação de dados.

    Attributes:
        success: Se a transformação foi bem-sucedida.
        dataframe: DataFrame transformado.
        records_input: Quantidade de registros de entrada.
        records_output: Quantidade de registros de saída.
        records_removed: Quantidade de registros removidos.
        duplicates_removed: Quantidade de duplicatas removidas.
        error_message: Mensagem de erro, se houver.
        transformation_stats: Estatísticas detalhadas da transformação.
    """

    success: bool
    dataframe: Optional[pd.DataFrame] = None
    records_input: int = 0
    records_output: int = 0
    records_removed: int = 0
    duplicates_removed: int = 0
    error_message: Optional[str] = None
    transformation_stats: Dict = field(default_factory=dict)


class DataTransformer:
    """
    Classe responsável pela transformação de dados.

    Aplica uma série de transformações para limpar, padronizar
    e enriquecer os dados para carga no banco.
    """

    def __init__(self):
        """Inicializa o transformador de dados."""
        self.settings = get_settings()
        self.transformation_stats = {}
        logger.info("DataTransformer inicializado")

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Padroniza os nomes das colunas para snake_case.

        Args:
            df: DataFrame com colunas a serem padronizadas.

        Returns:
            DataFrame com colunas padronizadas.
        """
        df = df.copy()

        # Mapear colunas para o padrão esperado
        column_mapping = {
            col: col.lower().strip().replace(" ", "_").replace("-", "_") for col in df.columns
        }

        df.rename(columns=column_mapping, inplace=True)

        logger.debug(f"Colunas padronizadas: {list(df.columns)}")
        return df

    def convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte colunas para os tipos de dados corretos.

        Args:
            df: DataFrame a ser convertido.

        Returns:
            DataFrame com tipos convertidos.
        """
        df = df.copy()

        # Converter data_transacao para datetime
        if "data_transacao" in df.columns:
            df["data_transacao"] = pd.to_datetime(
                df["data_transacao"],
                errors="coerce",
                dayfirst=True,  # Formato brasileiro DD/MM/YYYY
                format="mixed",
            )

        # Converter data_pagamento para datetime (pode ser nulo)
        if "data_pagamento" in df.columns:
            df["data_pagamento"] = pd.to_datetime(
                df["data_pagamento"],
                errors="coerce",
                dayfirst=True,
                format="mixed",
            )

        # Converter valor para numérico
        if "valor" in df.columns:
            # Tratar valores com formato brasileiro (1.234,56)
            df["valor"] = (
                df["valor"]
                .astype(str)
                .str.replace("R$", "", regex=False)
                .str.replace(" ", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

        # Converter id_transacao para string
        if "id_transacao" in df.columns:
            df["id_transacao"] = df["id_transacao"].astype(str).str.strip()

        # Converter campos de texto
        text_columns = ["cliente", "produto", "categoria", "status_pagamento"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        logger.debug("Tipos de dados convertidos")
        return df

    def handle_null_values(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Trata valores nulos de acordo com regras de negócio.

        Args:
            df: DataFrame com possíveis valores nulos.

        Returns:
            Tupla (DataFrame tratado, estatísticas de nulos).
        """
        df = df.copy()
        null_stats = {}

        # Contar nulos antes do tratamento
        for col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                null_stats[col] = {
                    "antes": int(null_count),
                    "percentual": round(null_count / len(df) * 100, 2),
                }

        # Regras de tratamento por coluna

        # cliente: substituir por "DESCONHECIDO"
        if "cliente" in df.columns:
            df["cliente"] = df["cliente"].fillna("DESCONHECIDO")
            df.loc[df["cliente"].isin(["nan", "None", ""]), "cliente"] = "DESCONHECIDO"

        # categoria: substituir por "OUTROS"
        if "categoria" in df.columns:
            df["categoria"] = df["categoria"].fillna("OUTROS")
            df.loc[df["categoria"].isin(["nan", "None", ""]), "categoria"] = "OUTROS"

        # data_pagamento: pode ser nulo (não pago ainda)
        # Não precisa tratar

        # Remover registros com campos críticos nulos
        critical_columns = ["id_transacao", "data_transacao", "valor"]
        initial_count = len(df)

        for col in critical_columns:
            if col in df.columns:
                df = df.dropna(subset=[col])

        removed_count = initial_count - len(df)
        if removed_count > 0:
            null_stats["registros_removidos_por_nulos_criticos"] = removed_count
            logger.warning(f"Removidos {removed_count} registros com valores críticos nulos")

        # Atualizar estatísticas pós-tratamento
        for col in null_stats:
            if col != "registros_removidos_por_nulos_criticos":
                null_stats[col]["depois"] = int(df[col].isna().sum())

        logger.debug(f"Tratamento de nulos concluído: {null_stats}")
        return df, null_stats

    def normalize_payment_status(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza o status de pagamento para valores padronizados.

        Args:
            df: DataFrame com coluna status_pagamento.

        Returns:
            DataFrame com status normalizado.
        """
        df = df.copy()

        if "status_pagamento" not in df.columns:
            return df

        # Criar cópia normalizada para mapeamento
        df["_status_lower"] = df["status_pagamento"].str.lower().str.strip()

        # Aplicar mapeamento
        df["status_pagamento"] = df["_status_lower"].map(STATUS_MAPPING)

        # Para valores não mapeados, usar "PENDENTE" como padrão
        unknown_status = df["status_pagamento"].isna()
        if unknown_status.any():
            unknown_values = df.loc[unknown_status, "_status_lower"].unique()
            logger.warning(
                f"Status não reconhecidos (convertidos para PENDENTE): {list(unknown_values)}"
            )
            df.loc[unknown_status, "status_pagamento"] = "PENDENTE"

        # Remover coluna auxiliar
        df.drop(columns=["_status_lower"], inplace=True)

        logger.debug("Status de pagamento normalizado")
        return df

    def create_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Cria colunas derivadas a partir dos dados existentes.

        Colunas criadas:
        - ano_transacao: Ano da transação
        - mes_transacao: Mês da transação (1-12)
        - dia_semana: Dia da semana (0=Segunda, 6=Domingo)
        - trimestre: Trimestre do ano (1-4)

        Args:
            df: DataFrame com coluna data_transacao.

        Returns:
            DataFrame com colunas derivadas.
        """
        df = df.copy()

        if "data_transacao" not in df.columns:
            logger.warning("Coluna data_transacao não encontrada")
            return df

        # Garantir que é datetime
        if not pd.api.types.is_datetime64_any_dtype(df["data_transacao"]):
            df["data_transacao"] = pd.to_datetime(df["data_transacao"], errors="coerce")

        # Criar colunas derivadas
        df["ano_transacao"] = df["data_transacao"].dt.year
        df["mes_transacao"] = df["data_transacao"].dt.month
        df["dia_semana"] = df["data_transacao"].dt.dayofweek
        df["trimestre"] = df["data_transacao"].dt.quarter

        logger.debug("Colunas derivadas criadas: ano, mês, dia_semana, trimestre")
        return df

    def remove_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """
        Remove registros duplicados.

        Considera duplicata quando id_transacao é igual.

        Args:
            df: DataFrame com possíveis duplicatas.

        Returns:
            Tupla (DataFrame sem duplicatas, quantidade removida).
        """
        df = df.copy()
        initial_count = len(df)

        # Remover duplicatas baseado em id_transacao
        if "id_transacao" in df.columns:
            df = df.drop_duplicates(subset=["id_transacao"], keep="first")

        duplicates_removed = initial_count - len(df)

        if duplicates_removed > 0:
            logger.info(f"Removidas {duplicates_removed} duplicatas")

        return df, duplicates_removed

    def validate_data_quality(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Valida qualidade dos dados e remove registros inválidos.

        Validações:
        - Valor deve ser positivo
        - Data da transação não pode ser futura
        - Data de pagamento não pode ser anterior à transação

        Args:
            df: DataFrame a ser validado.

        Returns:
            Tupla (DataFrame válido, estatísticas de validação).
        """
        df = df.copy()
        initial_count = len(df)
        validation_stats = {}

        # Validar valores positivos
        if "valor" in df.columns:
            invalid_values = df["valor"] < 0
            if invalid_values.any():
                validation_stats["valores_negativos"] = int(invalid_values.sum())
                df = df[~invalid_values]

        # Validar datas não futuras
        if "data_transacao" in df.columns:
            future_dates = df["data_transacao"] > datetime.now()
            if future_dates.any():
                validation_stats["datas_futuras"] = int(future_dates.sum())
                df = df[~future_dates]

        # Validar consistência de datas (pagamento >= transação)
        if "data_pagamento" in df.columns and "data_transacao" in df.columns:
            # Apenas para registros com data de pagamento
            has_payment = df["data_pagamento"].notna()
            invalid_payment = has_payment & (df["data_pagamento"] < df["data_transacao"])
            if invalid_payment.any():
                validation_stats["pagamento_antes_transacao"] = int(invalid_payment.sum())
                # Corrigir em vez de remover: setar data_pagamento = data_transacao
                df.loc[invalid_payment, "data_pagamento"] = df.loc[
                    invalid_payment, "data_transacao"
                ]

        validation_stats["total_removidos"] = initial_count - len(df)

        if validation_stats.get("total_removidos", 0) > 0:
            logger.warning(f"Validação removeu {validation_stats['total_removidos']} registros")

        return df, validation_stats

    def prepare_final_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara as colunas finais para carga no banco.

        Seleciona e ordena as colunas conforme o schema do banco.

        Args:
            df: DataFrame transformado.

        Returns:
            DataFrame com colunas finais ordenadas.
        """
        df = df.copy()

        # Colunas finais na ordem do schema
        final_columns = [
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
            "_arquivo_origem",
        ]

        # Selecionar apenas colunas existentes
        available_columns = [col for col in final_columns if col in df.columns]

        # Adicionar data_pagamento se não existir
        if "data_pagamento" not in df.columns:
            df["data_pagamento"] = pd.NaT

        df = df[available_columns]

        # Renomear coluna de metadados
        df = df.rename(columns={"_arquivo_origem": "arquivo_origem"})

        logger.debug(f"Colunas finais preparadas: {list(df.columns)}")
        return df

    def transform(self, df: pd.DataFrame) -> TransformationResult:
        """
        Executa todas as transformações no DataFrame.

        Pipeline de transformação:
        1. Padronização de nomes de colunas
        2. Conversão de tipos de dados
        3. Tratamento de valores nulos
        4. Normalização de status de pagamento
        5. Criação de colunas derivadas
        6. Remoção de duplicatas
        7. Validação de qualidade
        8. Preparação de colunas finais

        Args:
            df: DataFrame de entrada (resultado da extração).

        Returns:
            TransformationResult com os dados transformados.
        """
        logger.info(f"Iniciando transformação de {len(df)} registros")

        records_input = len(df)
        stats = {}

        try:
            # 1. Padronização de colunas
            df = self.standardize_column_names(df)

            # 2. Conversão de tipos
            df = self.convert_data_types(df)

            # 3. Tratamento de nulos
            df, null_stats = self.handle_null_values(df)
            stats["null_handling"] = null_stats

            # 4. Normalização de status
            df = self.normalize_payment_status(df)

            # 5. Colunas derivadas
            df = self.create_derived_columns(df)

            # 6. Remoção de duplicatas
            df, duplicates_removed = self.remove_duplicates(df)
            stats["duplicates_removed"] = duplicates_removed

            # 7. Validação de qualidade
            df, validation_stats = self.validate_data_quality(df)
            stats["validation"] = validation_stats

            # 8. Colunas finais
            df = self.prepare_final_columns(df)

            records_output = len(df)
            records_removed = records_input - records_output

            logger.success(
                f"Transformação concluída: {records_input} → {records_output} registros "
                f"({records_removed} removidos)"
            )

            return TransformationResult(
                success=True,
                dataframe=df,
                records_input=records_input,
                records_output=records_output,
                records_removed=records_removed,
                duplicates_removed=duplicates_removed,
                transformation_stats=stats,
            )

        except Exception as e:
            logger.error(f"Erro na transformação: {str(e)}")
            return TransformationResult(
                success=False,
                records_input=records_input,
                error_message=str(e),
            )


def transform(df: pd.DataFrame) -> TransformationResult:
    """
    Função principal de transformação.

    Args:
        df: DataFrame a ser transformado.

    Returns:
        TransformationResult com os dados transformados.
    """
    transformer = DataTransformer()
    return transformer.transform(df)


if __name__ == "__main__":
    # Teste do módulo de transformação
    logger.info("Iniciando teste do módulo de transformação...")

    # Criar dados de teste
    test_data = pd.DataFrame(
        {
            "id_transacao": ["T001", "T002", "T003", "T002"],  # T002 duplicado
            "data_transacao": ["01/01/2024", "15/01/2024", "20/01/2024", "15/01/2024"],
            "cliente": ["João Silva", None, "Maria Santos", "Pedro Costa"],
            "produto": ["Notebook", "Mouse", "Teclado", "Mouse"],
            "categoria": ["Eletrônicos", "Periféricos", None, "Periféricos"],
            "valor": ["R$ 2.500,00", "50.00", "150", "50.00"],
            "status_pagamento": ["pago", "PENDENTE", "cancelled", "Pending"],
            "data_pagamento": ["05/01/2024", None, None, None],
            "_arquivo_origem": ["test.csv"] * 4,
        }
    )

    result = transform(test_data)

    if result.success:
        print("\n✓ Transformação bem-sucedida!")
        print(f"  Entrada: {result.records_input} registros")
        print(f"  Saída: {result.records_output} registros")
        print(f"  Removidos: {result.records_removed}")
        print(f"  Duplicatas: {result.duplicates_removed}")
        print("\nDados transformados:")
        print(result.dataframe.to_string())
    else:
        print(f"\n✗ Erro na transformação: {result.error_message}")
