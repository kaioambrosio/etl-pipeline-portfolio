"""
ETL Pipeline - Script Principal
===============================

Orquestrador do pipeline ETL completo.
Coordena extração, transformação e carga de dados.

Autor: Kaio Ambrosio
GitHub: https://github.com/KaioAmbrosio

Uso:
    python main.py                    # Processa todos os arquivos
    python main.py arquivo.csv        # Processa arquivo específico
    python main.py --generate-sample  # Gera dados de exemplo
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from loguru import logger

# Adicionar diretório raiz ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings
from scripts.extract import DataExtractor
from scripts.load import DataLoader
from scripts.transform import DataTransformer


class ETLPipeline:
    """
    Orquestrador do Pipeline ETL.

    Coordena a execução sequencial de:
    1. Extração de dados de arquivos
    2. Transformação e limpeza
    3. Carga no banco de dados
    """

    def __init__(self):
        """Inicializa o pipeline ETL."""
        self.settings = get_settings()
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()

        # Configurar logging
        self._setup_logging()

        logger.info("=" * 60)
        logger.info("ETL Pipeline inicializado")
        logger.info(f"Raw Data Path: {self.settings.raw_data_path}")
        logger.info(f"Database: {self.settings.database_url_masked}")
        logger.info("=" * 60)

    def _setup_logging(self) -> None:
        """Configura o sistema de logging."""
        # Remover handler padrão
        logger.remove()

        # Formato do log
        log_format = (
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

        # Console handler
        logger.add(
            sys.stderr,
            format=log_format,
            level=self.settings.etl_log_level,
            colorize=True,
        )

        # File handler
        log_file = self.settings.logs_path / "etl.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)

        logger.add(
            str(log_file),
            format=log_format,
            level="DEBUG",
            rotation="10 MB",
            retention="30 days",
            compression="zip",
        )

    def process_file(self, file_path: Path) -> dict:
        """
        Processa um arquivo individual através do pipeline completo.

        Args:
            file_path: Caminho do arquivo a processar.

        Returns:
            Dicionário com estatísticas do processamento.
        """
        stats = {
            "file": file_path.name,
            "success": False,
            "extract": None,
            "transform": None,
            "load": None,
            "error": None,
        }

        start_time = datetime.now()

        logger.info(f"Processando arquivo: {file_path.name}")
        logger.info("-" * 40)

        # =====================================================================
        # ETAPA 1: EXTRAÇÃO
        # =====================================================================
        logger.info("📥 ETAPA 1: Extração")

        extract_result = self.extractor.extract_file(file_path)

        if not extract_result.success:
            logger.error(f"Falha na extração: {extract_result.error_message}")
            stats["error"] = f"Extração: {extract_result.error_message}"
            return stats

        stats["extract"] = {
            "records": extract_result.records_count,
            "hash": extract_result.file_hash,
        }

        logger.success(f"Extraídos {extract_result.records_count} registros")

        # =====================================================================
        # ETAPA 2: TRANSFORMAÇÃO
        # =====================================================================
        logger.info("🔄 ETAPA 2: Transformação")

        transform_result = self.transformer.transform(extract_result.dataframe)

        if not transform_result.success:
            logger.error(f"Falha na transformação: {transform_result.error_message}")
            stats["error"] = f"Transformação: {transform_result.error_message}"
            return stats

        stats["transform"] = {
            "input": transform_result.records_input,
            "output": transform_result.records_output,
            "removed": transform_result.records_removed,
            "duplicates": transform_result.duplicates_removed,
        }

        logger.success(
            f"Transformados: {transform_result.records_input} → "
            f"{transform_result.records_output} registros"
        )

        # =====================================================================
        # ETAPA 3: CARGA
        # =====================================================================
        logger.info("📤 ETAPA 3: Carga")

        file_size = file_path.stat().st_size if file_path.exists() else 0

        load_result = self.loader.load(
            df=transform_result.dataframe,
            file_name=file_path.name,
            file_path=str(file_path),
            file_hash=extract_result.file_hash,
            file_size=file_size,
        )

        if not load_result.success:
            logger.error(f"Falha na carga: {load_result.error_message}")
            stats["error"] = f"Carga: {load_result.error_message}"
            return stats

        stats["load"] = {
            "inserted": load_result.records_inserted,
            "skipped": load_result.records_skipped,
            "failed": load_result.records_failed,
            "time": load_result.execution_time,
        }

        logger.success(
            f"Carregados: {load_result.records_inserted} inseridos, "
            f"{load_result.records_skipped} ignorados"
        )

        # =====================================================================
        # CONCLUSÃO
        # =====================================================================
        total_time = (datetime.now() - start_time).total_seconds()

        stats["success"] = True
        stats["total_time"] = total_time

        logger.info("-" * 40)
        logger.success(f"✓ Arquivo processado com sucesso em {total_time:.2f}s")

        return stats

    def run(self, file_path: Optional[str] = None) -> List[dict]:
        """
        Executa o pipeline ETL.

        Args:
            file_path: Caminho específico de um arquivo (opcional).
                      Se não informado, processa todos os arquivos.

        Returns:
            Lista de estatísticas de cada arquivo processado.
        """
        pipeline_start = datetime.now()

        logger.info("=" * 60)
        logger.info("🚀 INICIANDO PIPELINE ETL")
        logger.info(f"Data/Hora: {pipeline_start.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        results = []

        # Obter lista de arquivos
        if file_path:
            files = [Path(file_path)]
        else:
            files = self.extractor.list_files()

        if not files:
            logger.warning("Nenhum arquivo encontrado para processar")
            return results

        logger.info(f"Arquivos a processar: {len(files)}")

        # Processar cada arquivo
        for i, file in enumerate(files, 1):
            logger.info("")
            logger.info(f"📁 Arquivo {i}/{len(files)}: {file.name}")

            stats = self.process_file(file)
            results.append(stats)

            if not stats["success"]:
                logger.error(f"Arquivo {file.name} falhou: {stats['error']}")

        # =====================================================================
        # RESUMO FINAL
        # =====================================================================
        pipeline_end = datetime.now()
        total_time = (pipeline_end - pipeline_start).total_seconds()

        success_count = sum(1 for r in results if r["success"])
        failed_count = len(results) - success_count

        total_extracted = sum(r["extract"]["records"] for r in results if r["extract"])
        total_loaded = sum(r["load"]["inserted"] for r in results if r["load"])

        logger.info("")
        logger.info("=" * 60)
        logger.info("📊 RESUMO DO PIPELINE")
        logger.info("=" * 60)
        logger.info(f"Arquivos processados: {success_count}/{len(results)}")
        logger.info(f"Arquivos com falha: {failed_count}")
        logger.info(f"Total de registros extraídos: {total_extracted}")
        logger.info(f"Total de registros carregados: {total_loaded}")
        logger.info(f"Tempo total: {total_time:.2f}s")
        logger.info("=" * 60)

        if failed_count == 0:
            logger.success("✅ Pipeline concluído com sucesso!")
        else:
            logger.warning(f"⚠️ Pipeline concluído com {failed_count} falha(s)")

        return results


def generate_sample_data(num_records: int = 1000) -> None:
    """
    Gera dados de exemplo para teste do pipeline.

    Args:
        num_records: Quantidade de registros a gerar.
    """
    import random

    import pandas as pd
    from faker import Faker

    fake = Faker("pt_BR")
    settings = get_settings()

    logger.info(f"Gerando {num_records} registros de exemplo...")

    # Listas de valores possíveis
    produtos = [
        "Notebook",
        "Mouse",
        "Teclado",
        "Monitor",
        "Headset",
        "Webcam",
        "SSD",
        "Memória RAM",
        "Placa de Vídeo",
        "Processador",
        "Smartphone",
        "Tablet",
        "Smartwatch",
        "Fone Bluetooth",
        "Carregador",
    ]

    categorias = ["Eletrônicos", "Periféricos", "Componentes", "Mobile", "Acessórios"]

    status_list = ["pago", "pendente", "cancelado", "atrasado"]

    # Gerar dados
    data = []
    for i in range(num_records):
        data_transacao = fake.date_between(start_date="-1y", end_date="today")
        status = random.choice(status_list)

        # Data de pagamento apenas para status "pago"
        data_pagamento = None
        if status == "pago":
            data_pagamento = fake.date_between(
                start_date=data_transacao,
                end_date=min(data_transacao + pd.Timedelta(days=30), datetime.now().date()),
            )

        data.append(
            {
                "id_transacao": f"TRX{str(i+1).zfill(6)}",
                "data_transacao": data_transacao.strftime("%d/%m/%Y"),
                "cliente": fake.name(),
                "produto": random.choice(produtos),
                "categoria": random.choice(categorias),
                "valor": f"R$ {random.uniform(50, 5000):,.2f}".replace(",", "X")
                .replace(".", ",")
                .replace("X", "."),
                "status_pagamento": status,
                "data_pagamento": data_pagamento.strftime("%d/%m/%Y") if data_pagamento else "",
            }
        )

    # Criar DataFrame
    df = pd.DataFrame(data)

    # Salvar arquivo
    output_path = (
        settings.raw_data_path
        / f"transacoes_exemplo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig", sep=";")

    logger.success(f"Arquivo gerado: {output_path}")
    logger.info(f"Total de registros: {len(df)}")


def main():
    """Função principal do pipeline ETL."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline - Processamento de dados financeiros",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py                      # Processa todos os arquivos em data/raw
  python main.py arquivo.csv          # Processa arquivo específico
  python main.py --generate-sample    # Gera 1000 registros de exemplo
  python main.py -g -n 5000           # Gera 5000 registros de exemplo
        """,
    )

    parser.add_argument("file", nargs="?", help="Arquivo específico para processar (opcional)")

    parser.add_argument(
        "-g", "--generate-sample", action="store_true", help="Gera dados de exemplo para teste"
    )

    parser.add_argument(
        "-n",
        "--num-records",
        type=int,
        default=1000,
        help="Quantidade de registros a gerar (padrão: 1000)",
    )

    args = parser.parse_args()

    # Modo de geração de dados de exemplo
    if args.generate_sample:
        generate_sample_data(args.num_records)
        return

    # Modo de execução do pipeline
    pipeline = ETLPipeline()
    results = pipeline.run(args.file)

    # Exit code baseado no resultado
    failed = sum(1 for r in results if not r["success"])
    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
