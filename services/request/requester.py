from database.queries import SelectInfos
import polars as pl


class QuantityToRequest(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def _define_diference_to_request(self):
        df = self.select_bd_infos(
                """
                    SELECT pkmc.partnumber, pkmc.num_reg_circ, pk05.takt, pkmc.rack,
                        pkmc.lb_balance, pkmc.total_theoretical_qty, pkmc.qty_for_restock, pkmc.qty_per_box,
                        pkmc.qty_max_box
                    FROM pkmc
                    JOIN pk05 ON pk05.supply_area = pkmc.supply_area
                    WHERE pkmc.lb_balance <= pkmc.qty_for_restock;
                """
            )
        df = df.with_columns([
            (pl.col("total_theoretical_qty") - pl.col("lb_balance"))
                .alias("qty_to_request"),

            ((pl.col("total_theoretical_qty") - pl.col("lb_balance")) / pl.col("qty_per_box"))
                .floor()
                .alias("qty_boxes_to_request")
        ])

        return df.select(["partnumber", "num_reg_circ", "qty_to_request", "qty_boxes_to_request", "takt", "rack"])
    

class LM01_Requester:
    def __init__(self, sap, df):
        self.sap = sap
        self.df = df.collect()

    def _request_lm01(self):
        session, _ = self.sap.run_transaction("/nLM01")

        session.findById("wnd[0]/usr/txtGV_OT").setFocus()
        session.findById("wnd[0]/usr/btnTEXT1").press()
        rows_requested = 0

        for row in self.df.iter_rows(named=True):
            qtd_caixas = int(row["qty_boxes_to_request"])
            num_circ = str(row["num_reg_circ"])

            for _ in range(qtd_caixas):
                rows_requested+=1
                session.findById("wnd[0]/usr/ctxtVG_PKNUM").Text = num_circ
                session.findById("wnd[0]").sendVKey(0)
                session.findById("wnd[0]").sendVKey(8)
                session.findById("wnd[0]/usr/btnRLMOB-POK").press()
                session.findById("wnd[0]/usr/btnBTOK").press()

        return rows_requested