from helpers.cleaner import CleanerBase
import polars as pl


class DefineDataframe(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def create_df(self):
        return self._load_file("PK05_PATH").lazy()


class PK05_Cleaner(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def filter_columns(self, df):
        return df.filter(pl.col("Depósito") == "LB01")

    def rename_columns(self, df):
        rename_map = {
            "Área abastec.prod.": "area_abastecimento",
            "Depósito": "deposito_pk05",
            "Responsável": "responsavel_pk05",
            "Ponto de descarga": "ponto_descarga_pk05",
            "Denominação SupM": "denominacao_pk05",
        }
        return self._rename(df, rename_map)