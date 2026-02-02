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
            pl.col("tipo_deposito_pkmc") == "B01"
        )
        return df
    
    def clean_columns(self, df):
        return df.with_columns(
            pl.col("qtd_max_caixas")
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
            (pl.col("qtd_por_caixa") * pl.col("qtd_max_caixas")).alias("qtd_total_teorica"),
            (pl.col("qtd_por_caixa") * (pl.col("qtd_max_caixas") - 1)).alias("qtd_para_reabastecimento"),
            pl.col("area_abastecimento").str.extract(r"(P\d+[A-Z]?)", 1).alias("prateleira")
        ])
        df = df.with_row_index(name="id")
        return df

    def rename_columns(self, df):
        rename_map = {
            "Material": "partnumber",
            "Área abastec.prod.": "area_abastecimento",
            "Nº circ.regul.": "num_circ_regul_pkmc",
            "Tipo de depósito": "tipo_deposito_pkmc",
            "Posição no depósito": "posicao_deposito_pkmc",
            "Container": "container_pkmc",
            "Texto breve de material": "descricao_partnumber",
            "Norma de embalagem": "norma_embalagem_pkmc",
            "Quantidade Kanban": "qtd_por_caixa", 
            "Posição de armazenamento": "qtd_max_caixas",
        }
        return self._rename(df, rename_map)
    