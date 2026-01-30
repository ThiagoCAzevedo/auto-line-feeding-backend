from backend.database import db_manager_general, db_manager_knr
from .infos_knr_email import main_get_unique_values
from datetime import datetime, date
from dotenv import load_dotenv
from typing import Dict, Any
import polars as pl, time, os
from backend.log import log_files
from pathlib import Path


logger = log_files.get_logger("knr | verify_same_values", "knr")

class VerifySameValues:
    load_dotenv()
    
    def __init__(self):
        self.excel_path = Path(os.getenv("VALORES_UNICOS_EMAIL_PATH"))        
        self.db_general = db_manager_general
        self.db_knr = db_manager_knr
        self.df_email = pl.DataFrame()
        self.df_fx4pd = pl.DataFrame()
        self.df_email_unique = pl.DataFrame()
        self.df_final = pl.DataFrame()
        self.actual_date = date.today()
        logger.info(f"Processador 'VerifySameValues' inicializado com o arquivo Excel: {self.excel_path.name}")

    def load_email_data(self) -> pl.DataFrame:
        try:
            logger.info("Buscando registros KNR da tabela 'knrs_vivos'")
            query = """
                SELECT knr, knr_fx4pd, cor, tmamg, cod_pais, pais, modelo
                FROM knr.knrs_vivos
            """
            result = self.db_general.execute_query(query)
            
            if not result:
                logger.warning("Nenhum registro KNR encontrado na tabela 'knrs_vivos' para hoje")
                return pl.DataFrame()
            
            df = pl.DataFrame(result).with_columns([
                pl.col("knr").cast(pl.Utf8).str.strip_chars(),
                pl.col("knr_fx4pd").cast(pl.Utf8).str.strip_chars(),
                pl.col("cor").cast(pl.Utf8),
                pl.col("tmamg").cast(pl.Utf8),
                pl.col("cod_pais").cast(pl.Utf8),
                pl.col("pais").cast(pl.Utf8),
                pl.col("modelo").cast(pl.Utf8),
                pl.lit(None).alias("partnumber"),
                pl.lit(None).alias("quantidade"),
                pl.lit(None).alias("quantidade_unidade"),
            ])
            
            self.df_email = df
            logger.info(f"{len(df)} registros KNR carregados da tabela 'knrs_vivos'")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar dados da tabela 'knrs_vivos': {e}", exc_info=True)
            raise

    def load_fx4pd_data(self) -> pl.DataFrame:
        try:
            logger.info("Buscando registros KNR FX4PD")
            query = f"""
                SELECT knr_fx4pd, partnumber, quantidade, quantidade_unidade
                FROM knr.knrs_fx4pd
            """
                # WHERE criado_em = DATE '{self.actual_date}'
            result = self.db_general.execute_query(query)
            
            if not result:
                logger.warning("Nenhum registro encontrado na tabela 'knrs_fx4pd' para hoje")
                return pl.DataFrame()
            
            df = pl.DataFrame(result).with_columns(
                pl.col("knr_fx4pd").cast(pl.Utf8).str.strip_chars()
            )
            self.df_fx4pd = df
            logger.info(f"{len(df)} registros KNR FX4PD carregados")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar dados da tabela 'knrs_fx4pd': {e}", exc_info=True)
            raise

    def load_unique_values(self) -> pl.DataFrame:
        try:
            if not self.excel_path.exists():
                logger.error(f"Arquivo Excel esperado não encontrado: {self.excel_path}")
                raise FileNotFoundError(f"Arquivo Excel não encontrado: {self.excel_path}")
            
            logger.info(f"Carregando valores únicos do arquivo {self.excel_path.name}")
            df = pl.read_excel(self.excel_path).with_columns([
                pl.col("knr").cast(pl.Utf8).str.strip_chars(),
                pl.col("knr_fx4pd").cast(pl.Utf8).str.strip_chars(),
            ])
            self.df_email_unique = df
            logger.info(f"{len(df)} linhas carregadas do arquivo 'valores_unicos.xlsx'")
            return df
        except Exception as e:
            logger.error(f"Erro ao carregar o arquivo Excel de valores únicos: {e}", exc_info=True)
            raise

    def process_join_and_fill(self) -> pl.DataFrame:
        try:
            logger.info("Realizando junção dos dados e preenchimento de campos ausentes")

            df_joined = self.df_email_unique.join(
                self.df_fx4pd,
                on="knr_fx4pd",
                how="left"
            ).select(
                "knr", "knr_fx4pd", "cor", "tmamg", "cod_pais",
                "pais", "modelo", "partnumber", "quantidade", "quantidade_unidade"
            )

            logger.info(f"Junção concluída: {len(df_joined)} registros")

            knrs_in_join = set(df_joined["knr_fx4pd"].to_list())
            df_missing = self.df_email.filter(~pl.col("knr_fx4pd").is_in(knrs_in_join))
            if len(df_missing) > 0:
                logger.warning(f"{len(df_missing)} registros KNR vivos não encontrados na junção com FX4PD")

            df_combined = pl.concat([df_joined, df_missing], how="vertical_relaxed")

            df_nulls = df_combined.filter(pl.col("partnumber").is_null() | (pl.col("partnumber") == ""))
            if len(df_nulls) > 0:
                logger.info(f"{len(df_nulls)} registros sem 'partnumber' — tentando preenchimento baseado em atributos")
                df_filled = df_nulls.drop(["partnumber", "quantidade", "quantidade_unidade"]).join(
                    df_joined.filter(pl.col("partnumber").is_not_null() & (pl.col("partnumber") != ""))
                    .select("cor", "tmamg", "cod_pais", "pais", "modelo", "partnumber", "quantidade", "quantidade_unidade"),
                    on=["cor", "tmamg", "cod_pais", "pais", "modelo"],
                    how="inner"
                )
                logger.info(f"Preenchimento baseado em atributos adicionou {len(df_filled)} registros")
                df_final = pl.concat([df_joined, df_filled], how="vertical_relaxed")
            else:
                df_final = df_combined

            df_final = df_final.with_columns(
                pl.when(pl.col("partnumber").is_not_null())
                .then(
                    pl.col("partnumber")
                    .str.strip_chars()
                    .str.replace_all(r"\s+", "")
                    .str.replace_all(r"\.", "")
                    .str.replace_all(r"[^\w-]", "")
                    .str.to_uppercase()
                )
                .otherwise(pl.col("partnumber"))
            )

            self.df_final = df_final
            logger.info(f"Dataset final pronto: {len(df_final)} registros")
            return df_final
        except Exception as e:
            logger.error(f"Erro durante junção e preenchimento: {e}", exc_info=True)
            raise


    def process(self, save_to_db: bool = True) -> Dict[str, Any]:
        try:
            start_time = datetime.now()
            logger.info(f"Verificação de KNR iniciada em {start_time:%Y-%m-%d %H:%M:%S}")

            self.load_email_data()
            self.load_fx4pd_data()
            self.load_unique_values()
            self.process_join_and_fill()

            final_count = len(self.df_final)
            null_parts = self.df_final.filter(
                pl.col("partnumber").is_null() | (pl.col("partnumber") == "")
            ).height

            if null_parts > 0:
                main_get_unique_values(has_null_values=True)
                time.sleep(2)
                self.process()

            if save_to_db:
                try:
                    logger.info(f"Salvando {final_count} valores na tabela 'knr.knrs_comum'")
                    self.db_knr.insert_knrs_comuns(self.df_final)
                    logger.info("Valores salvos com sucesso em 'knr.knrs_comum'")
                except Exception as e:
                    logger.error(f"Erro ao salvar valores na tabela 'knr.knrs_comum': {e}", exc_info=True)
                    return {
                        "status": "error",
                        "message": f"DB save failed: {e}",
                        "final_records": final_count,
                    }

            elapsed = (datetime.now() - start_time).total_seconds()
            completeness = round((1 - null_parts / final_count) * 100, 2) if final_count > 0 else 0

            logger.info(f"Verificação completa em {elapsed:.2f}s - {final_count} valores")
            logger.info(f"Porcentagem de sucesso na inserção de valores: {completeness}%")

            return {
                "status": "success",
                "final_records": final_count,
                "null_partnumbers": null_parts,
                "completeness_rate": completeness,
                "processing_time": round(elapsed, 2),
                "saved_to_db": save_to_db
            }

        except Exception as e:
            logger.error(f"Erro no processo de verificação: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}
                

def verify_knr_values(save_to_db: bool = True) -> Dict[str, Any]:
    try:
        logger.info("Inicializando verify_knr_values()...")
        processor = VerifySameValues()
        return processor.process(save_to_db)
    except Exception as e:
        logger.critical(f"Erro ao inicializar verify_knr_values(): {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def main():
    logger.info("main() VerifySameKnrValues inicializado")
    result = verify_knr_values()

    status = result.get("status", "unknown")
    message = result.get("message", None)

    if status == "success":
        logger.info(
            f"Main finalizado: {result['final_records']} Tempo de processamento "
            f"{result['processing_time']}s"
        )
        logger.info(f"Porcentagem de sucesso: {result['completeness_rate']}%")
    elif status == "partial":
        logger.warning(
            f"Main completado de forma parcial — {result['null_partnumbers']} KNR´s com Partnumber faltando "
            f"({result['completeness_rate']}% complete)"
        )
    else:
        logger.error(f"Main {status} — {message or 'Nenhuma mensagem informada'}")

    logger.info("VerifySameKnrValues main() finalizada")


if __name__ == "__main__":
    main()