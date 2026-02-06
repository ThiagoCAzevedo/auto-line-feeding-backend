from database.queries import SelectInfos
from helpers.data.cleaner import CleanerBase
import polars as pl


class ReturnReceptionValues(SelectInfos):
    def __init__(self):
        SelectInfos.__init__(self)

    def return_values_from_db(self):
        return self.select_bd_infos("SELECT knr, model, lfdnr_sequence FROM auto_line_feeding.assembly_line WHERE lane = 'reception'").lazy()


class ReturnValuesFX4PD(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def create_fx4pd_df(self):
        return self._load_file("FX4PD_PATH").lazy()
    
    def rename_select_columns(self, df):
        rename_map = {
            df.columns[0]: "knr_fx4pd",
            df.columns[1]: "partnumber",
            df.columns[5]: "qty_usage",
            df.columns[6]: "qty_unit",
        }
        return self._rename(df, rename_map)
    
    def clean_column(self, df: pl.LazyFrame | pl.DataFrame):
        df = df.with_columns(
            pl.col(pl.Utf8).str.replace_all(" ", "")
        )

        df = df.filter(
            pl.col("qty_usage")
            .cast(pl.Float64, strict=False)
            .is_not_null()
        )

        df = df.with_columns(
            qty_usage = pl.col("qty_usage").cast(pl.Float64, strict=False).fill_null(0.0),
            qty_unit = pl.col("qty_unit").cast(pl.Int32,  strict=False).fill_null(0),
        )

        return df


class MainAggregations(SelectInfos):
    def join_pkmc_pk05(self):
        return self.select_bd_infos(
                """
                    SELECT pkmc.partnumber, pkmc.num_reg_circ, pk05.takt, pkmc.rack,
                        pkmc.lb_balance, pkmc.total_theoretical_qty, pkmc.qty_for_restock, pkmc.qty_per_box,
                        pkmc.qty_max_box
                    FROM pkmc
                    JOIN pk05 ON pk05.supply_area = pkmc.supply_area
                """
            )
    
    def join_fx4pd_pkmc_pk05(self, df_pkmc_pk05):
        df_fx4pd = self.select_bd_infos("SELECT fx4pd.knr_fx4pd, fx4pd.partnumber, fx4pd.qty_usage, fx4pd.qty_unit FROM fx4pd")

        return df_fx4pd.join(
            df_pkmc_pk05,
            on="partnumber",
            how="inner"
        )