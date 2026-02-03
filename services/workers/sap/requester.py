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


class LM01_Requester:
    def __init__(self, sap, df):
        self.sap = sap
        self.df = df.collect()

    def _request_lm01(self):
        session, _ = self.sap.run_transaction("/nLM01")

        session.findById("wnd[0]/usr/txtGV_OT").setFocus()
        session.findById("wnd[0]/usr/btnTEXT1").press()

        for row in self.df.iter_rows(named=True):
            qtd_caixas = int(row["qtd_caixas_para_solicitar"])
            num_circ = str(row["num_circ_regul_pkmc"])

            for _ in range(qtd_caixas):
                session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                session.findById("wnd[0]").sendVKey(0)
                session.findById("wnd[0]").sendVKey(8)
                session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                session.findById("wnd[0]/usr/btnBTOK").press()