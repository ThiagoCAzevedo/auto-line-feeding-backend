from .pkmc import PKMC_Cleaner, PKMC_DefineDataframe
from database.upserter import UpsertInfos
import polars as pl


def pkmc_cleaner() -> pl.DataFrame:
    df_pkmc = PKMC_DefineDataframe().create_df()
    cleaner = PKMC_Cleaner()
    return (
        df_pkmc
        .pipe(cleaner.rename_columns)
        .pipe(cleaner.filter_columns)
        .pipe(cleaner.clean_columns)
        .pipe(cleaner.create_columns)
    )

def pkmc_upserter(df_pkmc):
    UpsertInfos().upsert_df("pkmc", df_pkmc, 1000)

def pkmc_pipeline() -> pl.DataFrame:
    df_pkmc = pkmc_cleaner()
    pkmc_upserter(df_pkmc)