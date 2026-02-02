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
            pl.col("deposito_pk05") == "LB01",
            (pl.col("tacto").is_not_null() &
            pl.col("tacto").str.starts_with("T")))
        return df
    
    def create_columns(self, df):
        df = df.with_columns(
            pl.col("denominacao_pk05").str.extract(r"(T\d+)", 1).alias("tacto")
        )
        df = df.with_row_index(name="id")
        return df

    def rename_columns(self, df):
        rename_map = {
            "Área abastec.prod.": "area_abastecimento",
            "Depósito": "deposito_pk05",
            "Responsável": "responsavel_pk05",
            "Ponto de descarga": "ponto_descarga_pk05",
            "Denominação SupM": "denominacao_pk05",
        }
        return self._rename(df, rename_map)