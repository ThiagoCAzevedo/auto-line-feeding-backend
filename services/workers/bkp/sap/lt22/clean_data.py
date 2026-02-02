import polars as pl, os
from pathlib import Path
from datetime import datetime
from backend.log import log_files
from backend.database import db_manager_knr
from dotenv import load_dotenv

logger = log_files.get_logger("sap | lt22 | clean_data", "sap")

class LT22Processor:
    load_dotenv()

    def __init__(self):
        self.file_path = Path(os.getenv("LT22_PATH")).resolve()
        self.complete_path = Path(os.getenv("LT22_COMPLETE_PATH")).resolve()
        self.previous_file = self.file_path / "previous_lt22.parquet"
        self.db_manager = db_manager_knr
        self.df_processed = pl.DataFrame()

        # Carrega o DataFrame anterior, se existir
        if self.previous_file.exists():
            try:
                self.previous_df = pl.read_parquet(self.previous_file)
                logger.info(f"Previous LT22 DataFrame carregado de {self.previous_file}")
            except Exception as e:
                logger.warning(f"Falha ao carregar previous_lt22.parquet: {e}")
                self.previous_df = pl.DataFrame()
        else:
            logger.info("Nenhum previous LT22 encontrado (primeira execução)")
            self.previous_df = pl.DataFrame()

        self.column_mapping = {
            "Nº OT": "num_ot",
            "Material": "material",
            "Tp.": "tp_destino",
            "PosiçDest": "posicao_destino",
            "QtdTeór DE": "quantidade",
            "Unid.dep.origem": "unidade_deposito",
            "Usuário": "usuario",
            "Data conf.": "data_confirmacao",
            "HoraCnf": "hora_confirmacao",
        }

    def find_header_line(self) -> int:
        with self.complete_path.open(encoding="latin1") as f:
            for i, line in enumerate(f):
                if "Material" in line:
                    logger.debug(f"Cabeçalho localizado na linha {i}")
                    return i
        logger.error(f"Cabeçalho não encontrado no arquivo {self.complete_path.name}")
        raise ValueError("Cabeçalho não encontrado no arquivo")

    def load_and_clean(self) -> pl.DataFrame:
        skip_rows = self.find_header_line()
        logger.info(f"Carregando arquivo LT22 a partir da linha {skip_rows}")

        df = pl.read_csv(
            self.complete_path,
            encoding="latin1",
            truncate_ragged_lines=True,
            skip_rows=skip_rows,
            has_header=True,
            quote_char=None,
            separator="|",
        )

        initial_rows = len(df)
        logger.info(f"{initial_rows} linhas brutas carregadas")

        df = df.rename({col: col.strip() for col in df.columns})
        df = df.filter(pl.col("Material").is_not_null())
        df = df.select([col for col in df.columns if df[col].null_count() < df.height])
        df = df.rename(self.column_mapping)

        cleaned_rows = len(df)
        logger.info(f"Dataset limpo: {cleaned_rows}/{initial_rows} linhas mantidas após limpeza")

        return df

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        logger.info(f"Aplicando transformações em {len(df)} registros")

        df = df.filter(
            pl.col("tp_destino").str.strip_chars() == "B01",
            pl.col("hora_confirmacao").str.strip_chars() != "",
            pl.col("quantidade")
            .str.strip_chars()
            .str.replace_all(",", "")
            .str.contains(r"^\d+$"),
        )

        df = df.with_columns([
            pl.col("hora_confirmacao").str.to_time("%H:%M:%S"),
            pl.col("data_confirmacao")
                .str.strptime(pl.Date, "%d.%m.%Y", strict=False)
                .dt.strftime("%Y-%m-%d"),
            pl.col("quantidade")
                .str.replace_all(",", ".")
                .str.strip_chars()
                .cast(pl.Float64),
            pl.col("posicao_destino")
                .str.extract(r"(P\d+[A-Z]?)", 0)
                .alias("prateleira"),
            pl.col("num_ot").str.strip_chars(),
            pl.col("material")
                .str.strip_chars()
                .str.replace_all(r"\s+", "")
                .str.replace_all(r"\.", "")
                .str.replace_all(r"[^\w-]", "")
                .str.to_uppercase()
                .alias("partnumber"),
        ])

        before_filter = len(df)
        final_count = len(df)
        logger.info(f"Transformação concluída: {final_count}/{before_filter} registros válidos")

        return df.select([
            "num_ot", "partnumber", "tp_destino", "posicao_destino",
            "quantidade", "unidade_deposito", "usuario", "prateleira",
            "data_confirmacao", "hora_confirmacao",
        ])

    def process(self, save_to_db: bool = True) -> None:
        if not self.complete_path.exists():
            logger.warning("Arquivo LT22 não encontrado. Nenhum processamento será realizado.")
            return

        start_time = datetime.now()
        logger.info(f"Processamento do LT22 iniciado em {start_time:%Y-%m-%d %H:%M:%S}")

        df = self.load_and_clean()
        initial_count = len(df)
        df = self.transform(df)
        final_count = len(df)
        self.df_processed = df

        if len(self.previous_df) > 0:
            join_keys = ["num_ot", "partnumber", "posicao_destino"]
            disappeared_df = self.previous_df.join(df, on=join_keys, how="anti")
            logger.info(f"{len(disappeared_df)} registros desapareceram desde a última execução")
            df_final = disappeared_df.drop_nulls(subset=["prateleira"])
            
            if save_to_db and len(df_final) > 0:
                logger.info("Salvando registros que desapareceram no banco de dados...")
                self.db_manager.insert_lt22(df_final)
                logger.info(f"{len(df_final)} registros salvos no banco de dados")
            else:
                logger.info("Nenhum registro desaparecido para inserir no banco.")
        else:
            logger.info("Primeira execução — sem histórico anterior.")
            df_final = pl.DataFrame()

        self.previous_df = df
        try:
            self.previous_df.write_parquet(self.previous_file)
            logger.info(f"Previous LT22 atualizado em {self.previous_file}")
        except Exception as e:
            logger.error(f"Falha ao salvar previous_lt22.parquet: {e}")

        elapsed = (datetime.now() - start_time).total_seconds()
        removal_rate = (initial_count - final_count) / initial_count * 100 if initial_count else 0
        logger.info(
            f"Processamento finalizado em {elapsed:.2f}s | "
            f"mantidas {final_count}/{initial_count} linhas | "
            f"taxa de remoção: {removal_rate:.1f}%"
        )


def main(save_to_db: bool = True):
    logger.info("Execução principal (main) do LT22 iniciada")
    try:
        processor = LT22Processor()
        processor.process(save_to_db)
        logger.info("process_lt22 concluído com sucesso")
    except Exception as e:
        logger.error(f"Falha no processo principal: {e}", exc_info=True)
    finally:
        logger.info("Execução principal (main) do LT22 finalizada")

if __name__ == "__main__":
    main()