from database.queries import SelectInfos
import polars as pl


class DefineDataFrame(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def _return_partnumbers_to_request(self):
        return self.select_bd_infos(
                """
                    SELECT pkmc.partnumber, pkmc.num_circ_regul_pkmc, pk05.tacto, pkmc.prateleira,
                        pkmc.saldo_lb, pkmc.qtd_total_teorica, pkmc.qtd_para_reabastecimento, pkmc.qtd_por_caixa,
                        pkmc.qtd_max_caixas
                    FROM pkmc
                    JOIN pk05 ON pk05.area_abastecimento = pkmc.area_abastecimento
                    WHERE pkmc.saldo_lb <= pkmc.qtd_para_reabastecimento;
                """
            )


class QuantityToRequest:
    def _define_diference_to_request(self):
        df = DefineDataFrame()._return_partnumbers_to_request()
        df = df.with_columns([
            (pl.col("qtd_total_teorica") - pl.col("saldo_lb"))
                .alias("qtd_para_solicitar"),

            ((pl.col("qtd_total_teorica") - pl.col("saldo_lb")) / pl.col("qtd_por_caixa"))
                .floor()
                .alias("qtd_caixas_para_solicitar")
        ])
        return df


class Requester:
    def _request_lm01(self):
        pass

#         try:
#             session, already_opened_sap_session = main_tasks_sap.call_specific_transaction_sap("/nlm01")

#             try:
#                 # Tenta focar no campo GV_OT
#                 session.findById("wnd[0]/usr/txtGV_OT").setFocus()
#                 session.findById("wnd[0]/usr/btnTEXT1").press()
#             except Exception as e:
#                 try:
#                     session.findById("wnd[0]/usr/btnTEXT2").press()
#                     session.findById("wnd[0]/usr/btnTEXT1").press()
#                 except Exception as e2:
#                     return  # Encerra a função se não conseguir prosseguir

#             for row in self.df_values_pkmc_pk05.iter_rows(named=True):
#                 qtd_caixas_para_solicitar = row['qtd_caixas_para_solicitar']
#                 num_circ_regul_pkmc = row['num_circ_regul_pkmc']

#                 # solicita OT
#                 # for i in range(int(qtd_caixas_para_solicitar)):
#                     # session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = str(num_circ_regul_pkmc)
#                     # session.findById("wnd[0]").sendVKey(0)
#                     # session.findById("wnd[0]").sendVKey(8)
#                     # session.findById("wnd[0]/usr/btnRLMOB-POK").press()
#                     # session.findById("wnd[0]/usr/btnBTOK").press()

#         except Exception:
#             raise

