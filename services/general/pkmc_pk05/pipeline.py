from .integrator import DataFrameJoinCleaner
from .pk05 import PK05_Cleaner, PK05_DefineDataframe
from .pkmc import PKMC_Cleaner, PKMC_DefineDataframe
import polars as pl


def pkmc_cleaner() -> pl.DataFrame:
    df_pkmc = PKMC_DefineDataframe().create_df()
    cleaner = PKMC_Cleaner()
    return (
        df_pkmc
        .pipe(cleaner.filter_columns)
        .pipe(cleaner.clean_columns)
        .pipe(cleaner.rename_columns)
    )

def pk05_cleaner() -> pl.DataFrame:
    df_pk05 = PK05_DefineDataframe().create_df()
    cleaner = PK05_Cleaner()
    return (
        df_pk05
        .pipe(cleaner.filter_columns)
        .pipe(cleaner.rename_columns)
    )

def run_pipeline() -> pl.DataFrame:
    df_pkmc = pkmc_cleaner()
    df_pk05 = pk05_cleaner()
    return DataFrameJoinCleaner(df_pkmc, df_pk05).cleaner_joiner().collect()