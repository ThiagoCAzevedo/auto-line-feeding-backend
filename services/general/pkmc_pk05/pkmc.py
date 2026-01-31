from helpers.cleaner import CleanerBase
import polars as pl


class DefineDataframe(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)

    def create_df(self):
        return self._load_file("PKMC_PATH").lazy()
    

class PKMC_Cleaner(CleanerBase):
    def __init__(self):
        CleanerBase.__init__(self)
        
    def filter_columns(self, df):
        return df.filter(pl.col("Tipo de depósito") == "B01")
    
    def clean_columns(self, df):
        return df.with_columns(
            pl.col("Material").str.replace_all(" ","").str.replace_all(r"\.", ""),
            pl.col("Posição de armazenamento").str.replace_all("MAX","")
        )

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
            "Posição de armazenamento": "qtd_max_caixas"
        }
        return self._rename(df, rename_map)