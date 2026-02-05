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
            df.columns[6]: "qty_unity",
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
            qty_unity = pl.col("qty_unity").cast(pl.Int32,  strict=False).fill_null(0),
        )

        return df


class MainAggregations(SelectInfos):
    def join_all(self):
        return self.select_bd_infos(
            """
            SELECT 
                pkmc.partnumber,
                pkmc.num_reg_circ,
                pk05.takt,
                pkmc.rack,
                pkmc.lb_balance,
                pkmc.total_theoretical_qty,
                pkmc.qty_for_restock,
                pkmc.qty_per_box,
                pkmc.qty_max_box,
                fx4pd.knr_fx4pd,
                fx4pd.qty_usage,
                fx4pd.qty_unity
            FROM pkmc
            JOIN pk05 ON pk05.supply_area = pkmc.supply_area
            LEFT JOIN fx4pd ON fx4pd.partnumber = pkmc.partnumber
            """
        )