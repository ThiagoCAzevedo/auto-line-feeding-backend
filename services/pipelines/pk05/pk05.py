from helpers.data.cleaner import CleanerBase
import polars as pl


class PK05_DefineDataframe(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def create_df(self):
        return self._load_file("PK05_PATH").lazy()


class PK05_Cleaner(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def filter_columns(self, df):
        df = df.filter(
            pl.col("deposit") == "LB01",
            (pl.col("takt").is_not_null() &
            pl.col("takt").str.starts_with("T")))
        return df
    
    def create_columns(self, df):
        df = df.with_columns(
            pl.col("description").str.extract(r"(T\d+)", 1).alias("takt")
        )
        df = df.with_row_index(name="id")
        return df

    def rename_columns(self, df):
        rename_map = {
            "Área abastec.prod.": "supply_area",
            "Depósito": "deposit",
            "Responsável": "responsible",
            "Ponto de descarga": "discharge_point",
            "Denominação SupM": "description",
        }
        return self._rename(df, rename_map)