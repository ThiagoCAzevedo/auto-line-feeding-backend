from database.queries import SelectInfos
from helpers.data.cleaner import CleanerBase
import polars as pl


class DefineForecastValues(SelectInfos):
    def join_fx4pd_pkmc_pk05(self):
        return self.select_bd_infos(
            """
            SELECT
                fx4pd.knr_fx4pd,
                fx4pd.partnumber,
                fx4pd.qty_usage,
                fx4pd.qty_unit,
                pkmc.num_reg_circ,
                pk05.takt,
                pkmc.rack,
                pkmc.lb_balance,
                pkmc.total_theoretical_qty,
                pkmc.qty_for_restock,
                pkmc.qty_per_box,
                pkmc.qty_max_box
            FROM fx4pd
            INNER JOIN pkmc
                ON pkmc.partnumber = fx4pd.partnumber
            INNER JOIN pk05
                ON pk05.supply_area = pkmc.supply_area
            """
        )