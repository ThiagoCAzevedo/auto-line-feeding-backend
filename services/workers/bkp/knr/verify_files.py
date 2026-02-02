from .infos_knr_email import main_get_unique_values
from backend.database import db_manager_general
from dotenv import load_dotenv
from backend.log import log_files
from pathlib import Path
import polars as pl, os


logger = log_files.get_logger('knr | verify_files', 'knr')

class VerifyFiles():
    load_dotenv()

    def __init__(self):
        logger.info("Inicializando verificação de arquivos KNR")
        self.db_manager = db_manager_general
        self.df_knrs_fx4pd = pl.DataFrame()
        self.df_knrs_mortos = pl.DataFrame()
        self.path_demanda_fx4pd = Path(os.getenv("DEMANDA_FX4PD_PATH"))
        logger.debug(f"Caminho do arquivo de demanda: {self.path_demanda_fx4pd}")

    def _return_db_values(self):
        logger.info("Buscando valores no banco de dados para comparação")
        try:
            self.df_knrs_fx4pd = pl.DataFrame(self.db_manager.execute_query("SELECT knr_fx4pd FROM knr.knrs_fx4pd"))
            self.df_knrs_mortos = pl.DataFrame(self.db_manager.execute_query("SELECT knr_fx4pd FROM knr.knrs_mortos"))
            logger.debug(f"Valores KNR FX4PD: {self.df_knrs_fx4pd.shape}")
            logger.debug(f"Valores KNR Mortos: {self.df_knrs_mortos.shape}")
        except Exception as e:
            logger.error(f"Erro ao buscar dados do banco: {e}", exc_info=True)

    def verify_values_in_knrs_mortos(self):
        logger.info("Iniciando verificação de valores entre KNR FX4PD e KNR Mortos")
        self._return_db_values()

        if (self.df_knrs_fx4pd.height != 0 and self.df_knrs_mortos.height != 0) and self.df_knrs_fx4pd.equals(self.df_knrs_mortos):
            logger.info("Valores são iguais. Iniciando criação de novo arquivo de valores únicos")
            main_get_unique_values(True)

            try:
                os.unlink(self.path_demanda_fx4pd)
                logger.warning("Arquivo antigo 'demanda_fx4pd.xlsx' removido com sucesso")
            except Exception as e:
                logger.error(f"Erro ao remover arquivo antigo: {e}", exc_info=True)

            logger.info("Novo 'valores_unicos.xlsx' criado. Adicione novo 'demanda_fx4pd.xlsx'")
            return True
        else:
            logger.info("Os valores não são iguais. Nenhuma ação necessária")
            return False

def main():
    logger.info("Iniciando rotina principal de verificação de arquivos KNR")
    try:
        verify_files_to_continue = VerifyFiles()
        resultado = verify_files_to_continue.verify_values_in_knrs_mortos()
        logger.info(f"Resultado da verificação: {resultado}")
    except Exception as e:
        logger.critical(f"Erro crítico na execução da verificação de arquivos: {e}", exc_info=True)

if __name__ == '__main__':
    main()