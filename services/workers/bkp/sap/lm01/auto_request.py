from dotenv import load_dotenv
from backend.database import db_manager_general
from ..main_tasks_sap import main_tasks_sap
from backend.log import log_files
import polars as pl, os


logger = log_files.get_logger("sap | lm01 | auto_request", "sap")

class AutoRequestSAP:
    load_dotenv()

    def __init__(self):
        logger.info("Inicializando AutoRequestSAP")
        self.db_manager_general = db_manager_general
        self.df_values_pkmc_pk05 = pl.DataFrame()
        self._return_db_values()

    def _return_db_values(self):
        logger.info("Buscando dados do banco para PKMC_PK05")
        try:
            values_pkmc_pk05 = self.db_manager_general.execute_query(
                """
                SELECT partnumber, num_circ_regul_pkmc, tacto, prateleira, saldo_lb, qtd_total_teorica, qtd_para_reabastecimento, qtd_por_caixa, qtd_max_caixas
                FROM knr.pkmc_pk05
                WHERE saldo_lb <= qtd_para_reabastecimento
                """
            )
            logger.info(f"Registros retornados: {len(values_pkmc_pk05)}")
            self.df_values_pkmc_pk05 = pl.DataFrame(values_pkmc_pk05)
        except Exception as e:
            logger.error("Erro ao buscar dados do banco", exc_info=True)

    def verify_necessity_and_quantity(self):
        logger.info("Verificando necessidade de solicitação de OT")
        try:
            if "partnumber" in self.df_values_pkmc_pk05.columns:
                existe_partnumber = self.df_values_pkmc_pk05.filter(
                    pl.col("partnumber") == "2G5827550B",
                    pl.col("num_circ_regul_pkmc") == "215229"   
                ).height > 0

            logger.info(f"Existe partnumber '2G5827550B'? {existe_partnumber}")
            logger.info(f"Total de registros: {len(self.df_values_pkmc_pk05)}")

            if len(self.df_values_pkmc_pk05) > 0 and existe_partnumber:
                self.df_values_pkmc_pk05 = self.df_values_pkmc_pk05.filter(
                    pl.col("partnumber") == "2G5827550B",
                    pl.col("num_circ_regul_pkmc") == "215229"
                )

                self.df_values_pkmc_pk05 = self.df_values_pkmc_pk05.with_columns(
                    (pl.col('qtd_total_teorica') - pl.col('saldo_lb')).alias('qtd_para_solicitar'),
                    (pl.col('saldo_lb') / pl.col('qtd_por_caixa')).alias('qtd_caixas_em_uso_atual').ceil()
                )

                self.df_values_pkmc_pk05 = self.df_values_pkmc_pk05.with_columns(
                    (pl.col('qtd_para_solicitar') / pl.col('qtd_por_caixa')).alias('qtd_caixas_para_solicitar').floor()
                )

                self.request_lm01()
            else:
                logger.info("Nenhuma necessidade de solicitação encontrada.")
        except Exception as e:
            logger.error("Erro na verificação de necessidade", exc_info=True)

    def request_lm01(self):
        logger.info("Iniciando transação SAP /nlm01")
        try:
            session, already_opened_sap_session = main_tasks_sap.call_specific_transaction_sap("/nlm01")

            try:
                # Tenta focar no campo GV_OT
                session.findById("wnd[0]/usr/txtGV_OT").setFocus()
                session.findById("wnd[0]/usr/btnTEXT1").press()
            except Exception as e:
                logger.warning("Campo GV_OT não encontrado, tentando caminho alternativo")
                try:
                    session.findById("wnd[0]/usr/btnTEXT2").press()
                    session.findById("wnd[0]/usr/btnTEXT1").press()
                except Exception as e2:
                    logger.error("Não foi possível pressionar TEXT2 ou TEXT1", exc_info=True)
                    return  # Encerra a função se não conseguir prosseguir

            logger.info(f"Total de linhas para processar: {self.df_values_pkmc_pk05.height}")
            for row in self.df_values_pkmc_pk05.iter_rows(named=True):
                qtd_caixas_para_solicitar = row['qtd_caixas_para_solicitar']
                num_circ_regul_pkmc = row['num_circ_regul_pkmc']
                logger.info(f"Processando partnumber: {row['partnumber']} | Circ: {num_circ_regul_pkmc} | Caixas: {qtd_caixas_para_solicitar}")

                for i in range(int(qtd_caixas_para_solicitar)):
                    logger.info(f"Solicitando OT {i+1}/{int(qtd_caixas_para_solicitar)} para {num_circ_regul_pkmc}")
                    # session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = str(num_circ_regul_pkmc)
                    # session.findById("wnd[0]").sendVKey(0)
                    # session.findById("wnd[0]").sendVKey(8)
                    # session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                    # session.findById("wnd[0]/usr/btnBTOK").press()
                    logger.info("OT solicitada com sucesso")

            logger.info("Processamento SAP finalizado")
        except Exception as e:
            logger.error("Erro ao executar transação LM01", exc_info=True)



def main():
    logger.info("Iniciando execução principal do AutoRequestSAP")
    try:
        auto_request_sap = AutoRequestSAP()
        auto_request_sap.verify_necessity_and_quantity()
    except Exception as e:
        logger.error("Erro na execução principal", exc_info=True)
    finally:
        logger.info("Execução principal finalizada")


if __name__ == "__main__":
    main()