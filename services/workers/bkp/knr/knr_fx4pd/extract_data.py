from backend.database import db_manager_knr
from typing import Optional, Dict
from dotenv import load_dotenv
from threading import Event
from backend.log import log_files
from pathlib import Path
import polars as pl, re, sys, time, os


logger = log_files.get_logger("knr | infos_knr_fx4pd | extract_data", "knr")

class FX4PDProcessor:
    load_dotenv()

    def __init__(self, master_stop_event: Optional[Event]):
        self.file_path = Path(os.getenv("DEMANDA_FX4PD_PATH"))
        self.db_manager = db_manager_knr
        self.max_retries = os.getenv("MAX_RETRIES")
        self.min_retries = os.getenv("MIN_RETRIES")
        self.time_between_retries = os.getenv("BETWEEN_RETRIES")
        self.stop_event = master_stop_event
        self.processed_count = 0
        self.expected_columns = [
            "PON_Kennnummer",
            "PartCode_Sachnummer",
            "Quantity_Menge",
            "QuantityUnit_Mengeneinheit",
        ]

        logger.info("FX4PDProcessor inicializado")

    def pipeline(self) -> None:
        try:
            logger.info("Iniciando pipeline FX4PD")
            file_path = self._wait_for_file()

            logger.info(f"Processando arquivo: {file_path.name}")
            df = self._read_and_clean_excel(file_path)

            if df is None or df.is_empty():
                logger.warning(f"Nenhum dado válido encontrado no arquivo: {file_path.name}")
                return

            self._save_to_database(df)
            logger.info(f"Pipeline finalizado com sucesso - {self.processed_count} registros processados")

        except Exception as e:
            logger.critical(f"Erro fatal no pipeline: {e}", exc_info=True)
            raise

    def _wait_for_file(self) -> Optional[Path]:
        file_path = self.file_path

        if file_path.exists():
            logger.info(f"Arquivo Excel localizado: {file_path.name}")
            return file_path

        for attempt in range(self.min_retries, self.max_retries + 1):
            logger.warning(
                f"Arquivo '{self.file_path}' não encontrado. "
                f"Tentativa {attempt}/{self.max_retries} - aguardando {self.time_between_retries // 60} minutos..."
            )
            time.sleep(self.time_between_retries)
            if file_path.exists():
                logger.info(
                    f"Arquivo '{self.file_path}' encontrado após {attempt} tentativas "
                    f"({(self.time_between_retries // 60) * attempt} minutos)."
                )
                return file_path

        return None

    def _read_and_clean_excel(self, file_path: Path) -> Optional[pl.DataFrame]:
        try:
            logger.info(f"Lendo arquivo Excel: {file_path.name}")
            df = pl.read_excel(source=file_path, engine="calamine")

            if df.is_empty():
                logger.warning(f"Arquivo Excel vazio: {file_path.name}")
                return None

            logger.info(f"{len(df)} linhas lidas (brutas)")

            def normalize_header(h: str) -> str:
                h = str(h)
                h = re.sub(r"\s*/\s*[\r\n]+", "_", h)
                h = re.sub(r"\s*/\s*", "_", h)
                h = re.sub(r"[\r\n]+", "", h)
                h = h.strip().replace(" ", "")
                return h

            headers = [normalize_header(h) for h in df.row(0)]
            df = df.slice(1)
            df.columns = headers
            logger.debug(f"Colunas após limpeza: {df.columns}")

            if "PON_Kennnummer" not in df.columns:
                logger.error(f"Coluna obrigatória ausente: 'PON_Kennnummer' em {file_path.name}")
                return None

            columns = {
                "PON_Kennnummer": pl.col("PON_Kennnummer"),
                "PartCode_Sachnummer": (
                    pl.col("PartCode_Sachnummer")
                    if "PartCode_Sachnummer" in df.columns
                    else pl.lit(None).alias("PartCode_Sachnummer")
                ),
                "Quantity_Menge": (
                    pl.col("Quantity_Menge").cast(pl.Float64)
                    if "Quantity_Menge" in df.columns
                    else pl.lit(None).cast(pl.Float64).alias("Quantity_Menge")
                ),
                "QuantityUnit_Mengeneinheit": (
                    pl.col("QuantityUnit_Mengeneinheit")
                    if "QuantityUnit_Mengeneinheit" in df.columns
                    else pl.lit(None).alias("QuantityUnit_Mengeneinheit")
                ),
            }

            df_clean = df.select(list(columns.values()))

            df_clean = (
                df_clean
                .filter(pl.col("PON_Kennnummer").is_not_null())
                .filter(pl.col("PON_Kennnummer").str.len_chars() > 0)
                .with_columns(
                    pl.when(pl.col("PartCode_Sachnummer").is_not_null())
                    .then(
                        pl.col("PartCode_Sachnummer")
                        .str.strip_chars()
                        .str.replace_all(r"\s+", "")
                        .str.replace_all(r"\.", "")
                        .str.replace_all(r"[^\w-]", "")
                        .str.to_uppercase()
                    )
                    .otherwise(pl.col("PartCode_Sachnummer"))
                    .alias("PartCode_Sachnummer")
                )
            )

            logger.info(f"Arquivo Excel limpo: {len(df_clean)} linhas válidas")

            if df_clean.is_empty():
                logger.warning("Nenhum registro válido após a limpeza.")
                return None

            return df_clean

        except Exception as e:
            logger.error(f"Falha ao ler/limpar arquivo Excel {file_path.name}: {e}", exc_info=True)
            return None

    def _save_to_database(self, df: pl.DataFrame) -> None:
        try:
            total_rows = len(df)
            logger.info(f"Gravando {total_rows} linhas no banco de dados...")
            self.db_manager.insert_knrs_fx4pd(df)
            self.processed_count = total_rows
            logger.info(f"Gravação concluída com sucesso ({self.processed_count} registros).")
        except Exception as e:
            logger.error(f"Erro ao salvar FX4PD no banco: {e}", exc_info=True)
            raise

    def get_statistics(self) -> Dict[str, Optional[str]]:
        return {
            "processed_count": self.processed_count,
        }


def main(master_stop_event: Optional[Event] = None) -> int:
    processor = FX4PDProcessor(master_stop_event=master_stop_event)
    try:
        logger.info("Executando processador FX4PD (modo único).")
        processor.pipeline()
        stats = processor.get_statistics()
        logger.info(f"Execução finalizada — estatísticas: {stats}")
        return 0
    except Exception as e:
        logger.critical(f"Erro fatal no processador FX4PD: {e}", exc_info=True)
        return 1
    finally:
        logger.info("Encerramento completo do processador FX4PD.")


if __name__ == "__main__":
    sys.exit(main())