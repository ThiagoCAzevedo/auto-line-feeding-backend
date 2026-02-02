from .pk05 import PK05_Cleaner, PK05_DefineDataframe
import polars as pl


def pk05_cleaner() -> pl.DataFrame:
    df_pk05 = PK05_DefineDataframe().create_df()
    cleaner = PK05_Cleaner()
    return (
        df_pk05
        .pipe(cleaner.rename_columns)
        .pipe(cleaner.create_columns)
        .pipe(cleaner.filter_columns)
    )


def pk05_pipeline() -> pl.DataFrame:
    df_pk05 = pk05_cleaner()