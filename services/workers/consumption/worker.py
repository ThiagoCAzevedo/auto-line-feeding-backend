from .forecast import ReturnReceptionValues, ReturnValuesFX4PD, MainAggregations
from database.queries import UpsertInfos


def get_reception_values():
    return ReturnReceptionValues().return_values_from_db()

def transform_fx4pd_values():
    rv = ReturnValuesFX4PD()
    df = rv.create_fx4pd_df()        
    df = rv.rename_select_columns(df)  
    df = rv.clean_column(df)   
    return df.collect()

def return_aggregation():
    return MainAggregations().join_all()

def upserter(table, df):
    UpsertInfos().upsert_df(table, df, 1000)

def consumption_worker():
    df = get_reception_values()
    df_fx4pd = transform_fx4pd_values()
    upserter("fx4pd", df_fx4pd)
    df_consumption = return_aggregation()
    upserter("consumption", df_consumption)

consumption_worker()