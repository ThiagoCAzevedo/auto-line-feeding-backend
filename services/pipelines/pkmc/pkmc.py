from helpers.data.cleaner import CleanerBase
import polars as pl


class PKMC_DefineDataframe(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def create_df(self):
        return self._load_file("PKMC_PATH").lazy()
    

class PKMC_Cleaner(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)
        
    def filter_columns(self, df):
        df = df.filter(
            pl.col("deposit_type") == "B01"
        )
        return df
    
    def clean_columns(self, df):
        return df.with_columns(
            pl.col("qty_max_box")
                .cast(pl.Utf8)
                .str.replace_all(r"(?i)max", "")
                .str.replace_all(r"[ :]", "")
                .str.replace_all(r"\D+", "")
                .cast(pl.Int64, strict=False)
                .fill_null(0),
            pl.col("partnumber")
                .cast(pl.Utf8)
                .str.strip_chars()
                .str.replace_all(r"\s+", "")
                .str.replace_all(r"\.", "")
                .str.replace_all(r"[^\w-]", "")
                .str.to_uppercase()
        )

    def create_columns(self, df):
        df = df.with_columns([
            (pl.col("qty_per_box") * pl.col("qty_max_box")).alias("total_theoretical_qty"),
            (pl.col("qty_per_box") * (pl.col("qty_max_box") - 1)).alias("qty_for_restock"),
            pl.col("supply_area").str.extract(r"(P\d+[A-Z]?)", 1).alias("rack")
        ])
        df = df.with_row_index(name="id")
        return df

    def rename_columns(self, df):
        rename_map = {
            "Material": "partnumber",
            "Área abastec.prod.": "supply_area",
            "Nº circ.regul.": "num_reg_circ",
            "Tipo de depósito": "deposit_type",
            "Posição no depósito": "deposit_position",
            "Container": "container",
            "Texto breve de material": "description",
            "Norma de embalagem": "pack_standard",
            "Quantidade Kanban": "qty_per_box", 
            "Posição de armazenamento": "qty_max_box",
        }
        return self._rename(df, rename_map)
    