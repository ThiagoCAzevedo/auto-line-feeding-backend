from .api import AssemblyLineApi
from .processor import DefineDataFrame, TransformDataFrame
from database.queries import UpsertInfos


def return_response():
    return AssemblyLineApi()._return_response()

def process_response(response):
    cleaner = DefineDataFrame(response)
    df = cleaner._extract_car_records(response)

    transformer = TransformDataFrame(df)
    df = transformer.transform()
    return df

def upserter(df):
    UpsertInfos().upsert_df("assembly_line", df, 1000)

def al_worker():    
    response = return_response()
    df = process_response(response)
    upserter(df)