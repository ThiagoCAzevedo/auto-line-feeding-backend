import polars as pl


class DataFrameJoinCleaner:
    def __init__(self, df_pkmc, df_pk05):
        self.df_pkmc = df_pkmc
        self.df_pk05 = df_pk05

    def cleaner_joiner(self):
        df = self.df_pkmc.join(self.df_pk05, on="area_abastecimento", how="inner")

        df = df.with_columns([
            pl.col("denominacao_pk05").str.extract(r"(T\d+)", 1).alias("tacto"),
            pl.col("area_abastecimento").str.extract(r"(P\d+[A-Z]?)", 1).alias("prateleira")
        ])

        df = df.with_columns([
            (pl.col("qtd_por_caixa") * pl.col("qtd_max_caixas")).alias("qtd_total_teorica"),
            (pl.col("qtd_por_caixa") * (pl.col("qtd_max_caixas") - 1)).alias("qtd_para_reabastecimento"),
        ])

        df = df.filter(
            pl.col("tacto").is_not_null() &
            pl.col("tacto").str.starts_with("T")
        )
        return df