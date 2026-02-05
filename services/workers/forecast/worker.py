from .forecaster import ReturnReceptionValues, ReturnValuesFX4PD, MainAggregations
from database.queries import UpsertInfos
import polars as pl

def get_reception_values():
    return ReturnReceptionValues().return_values_from_db()
    # com esse valor, fazer a pesquisa no sistema do fx4pd

def transform_fx4pd_values():
    rv = ReturnValuesFX4PD()
    df = rv.create_fx4pd_df()        
    df = rv.rename_select_columns(df)  
    df = rv.clean_column(df)   
    return df.collect()

def return_aggregation():
    df_pkmc_pk05 = MainAggregations().join_pkmc_pk05()
    df_consumption = MainAggregations().join_fx4pd_pkmc_pk05(df_pkmc_pk05)
    return df_consumption

def upserter(table, df):
    UpsertInfos().upsert_df(table, df, 1000)

def forecast_worker():
    df = get_reception_values()
    # com esse valor acima, fazer a pesquisa no sistema do fx4pd

    df_fx4pd = transform_fx4pd_values()
    upserter("fx4pd", df_fx4pd)
    df_consumption = return_aggregation()
    upserter("consumption", df_consumption)