from helpers.data.cleaner import CleanerBase
import polars as pl


class ReturnFX4PDValues(CleanerBase):
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