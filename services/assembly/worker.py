from .assembly_api import AssemblyLineApi
from .processor import DefineDataFrame, TransformDataFrame
from database.queries import UpsertInfos


def return_response():
    return AssemblyLineApi()._return_response()

def process_response(response):
    cleaner = DefineDataFrame(response)
    df = cleaner._extract_car_records(response)

    transformer = TransformDataFrame(df)
    df = transformer.transform()
    df = transformer.create_fx4pd_column()
    return df

def upserter(df):
    UpsertInfos().upsert_df("assembly_line", df, 1000)

def al_worker():    
    response = return_response()
    
    # import json
    # with open(r"C:\Users\thiago.azevedo\OneDrive - Sese\thiago_sese\auto_line_feeding\backend\outputfile.json", "r", encoding="utf-8") as f:
    #     response = json.load(f)

    df = process_response(response)
    upserter(df)

