from pathlib import Path
from datetime import datetime, date
from typing import Optional, Dict, Any
from backend.log import log_files
from backend.database import db_manager_general
from dotenv import load_dotenv
import polars as pl, os


logger = log_files.get_logger("knr | infos_knr_email | get_unique_values", "knr")

class UniqueValuesExporter:
    load_dotenv()

    def __init__(self):
        self.db_manager = db_manager_general
        self.output_dir = Path(os.getenv("VALORES_UNICOS_EMAIL_PATH"))
        logger.debug(f"UniqueValuesExporter inicializado, diretório de saída={self.output_dir}")

    def get_unique_knr_values(self, target_date: Optional[date] = None) -> pl.DataFrame:
        target_date = target_date or datetime.now().date()
        formatted_date = target_date.strftime("%Y-%m-%d")
        logger.info(f"Buscando valores únicos de KNR para {formatted_date}")

        query = """
            SELECT 
                cor, 
                tmamg,
                cod_pais, 
                pais, 
                modelo,
                ANY_VALUE(knr) as knr,
                ANY_VALUE(knr_fx4pd) as knr_fx4pd
            FROM knr.knrs_vivos
            WHERE DATE(criado_em) = DATE(?)
            GROUP BY cor, tmamg, cod_pais, pais, modelo
        """

        try:
            results = self.db_manager.execute_query(query, (formatted_date,))
            if not results:
                logger.warning(f"Nenhum dado de KNR encontrado para {formatted_date}")
                raise ValueError(f"Nenhum dado para a data {formatted_date}")

            df = pl.DataFrame(results)

            logger.info(f"{len(df)} combinações únicas de KNR recuperadas para {formatted_date}")
            return df

        except Exception as e:
            logger.error(f"Falha ao buscar valores no banco de dados: {e}", exc_info=True)
            raise

    def export_to_excel(
        self,
        df: pl.DataFrame,
        sheet_name: str = "valores_unicos",
        must_update_excel: Optional[bool] = False
    ) -> Path:
        
        if df.is_empty():
            logger.warning("Tentativa de exportar DataFrame vazio")
            raise ValueError("Não é possível exportar um DataFrame vazio")
        
        if self.output_dir.exists() and must_update_excel is False:
            logger.info(f"Arquivo já existe, não será sobrescrito: {self.output_dir}")
            return self.output_dir

        logger.info(f"Exportando {len(df)} registros para Excel: {self.output_dir}")

        try:
            df.write_excel(
                workbook=self.output_dir,
                worksheet=sheet_name,
                autofit=True,
                freeze_panes=(1, 0)
            )

            logger.info(f"Exportação para Excel concluída: {self.output_dir}")
            return self.output_dir

        except Exception as e:
            logger.error(f"Erro durante exportação para Excel: {e}", exc_info=True)
            raise IOError(f"Falha ao escrever o arquivo Excel: {e}")

    def generate_unique_values_report(
        self,
        target_date: Optional[date] = None,
        must_update_excel: Optional[bool] = False,
    ) -> Dict[str, Any]:
        try:
            logger.info("Iniciando geração do relatório de valores únicos")
            df = self.get_unique_knr_values(target_date)

            self.output_dir = self.export_to_excel(df, sheet_name="Valores_Unicos", must_update_excel=must_update_excel)

            stats = {
                "total_unique_combinations": len(df),
                "unique_colors": df["cor"].n_unique(),
                "unique_models": df["modelo"].n_unique(),
                "unique_countries": df["pais"].n_unique(),
                "self.output_dir": str(self.output_dir),
                "export_date": datetime.now().isoformat(),
                "data_date": (target_date or datetime.now().date()).isoformat(),
            }

            logger.info(f"Relatório gerado com sucesso: {stats}")
            return stats

        except Exception as e:
            logger.error(f"Falha na geração do relatório: {e}", exc_info=True)
            return {
                "error": str(e),
                "export_date": datetime.now().isoformat(),
                "status": "falhou",
            }
        
    def _has_null_values(self, must_update_excel):
        df_knrs_fx4pd = pl.DataFrame(
            db_manager_general.execute_query("SELECT DISTINCT(knr_fx4pd) FROM knr.knrs_fx4pd")
        )
        df_knrs_vivos = pl.DataFrame(
            db_manager_general.execute_query(
                "SELECT cor, tmamg, cod_pais, pais, modelo, knr, knr_fx4pd FROM knr.knrs_vivos"
            )
        )

        df_join = (
            df_knrs_fx4pd.join(df_knrs_vivos, on="knr_fx4pd", how="inner")
            .select("cor", "tmamg", "cod_pais", "pais", "modelo", "knr", "knr_fx4pd")
        )

        self.export_to_excel(df_join, must_update_excel=must_update_excel)
    
    
def main(has_null_values: Optional[bool] = False, must_update_excel: Optional[bool] = False, target_date: Optional[date] = None) -> Dict[str, Any]:
    exporter = UniqueValuesExporter()
    if has_null_values:
        return exporter._has_null_values(must_update_excel=must_update_excel)
    else: 
        return exporter.generate_unique_values_report(target_date, must_update_excel)


if __name__ == "__main__":
    result = main()