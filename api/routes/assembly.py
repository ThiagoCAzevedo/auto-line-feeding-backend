# app/api/assembly.py
from fastapi import APIRouter, HTTPException, Query
from services.assembly.assembly_api import AssemblyLineApi
from services.assembly.processor import DefineDataFrame, TransformDataFrame
from database.queries import UpsertInfos

router = APIRouter()


@router.get("/response")
def get_response():
    try:
        return AssemblyLineApi().get_raw_json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erro ao buscar origem: {e}")


@router.get("/processor")
def get_response_transformed():
    try:
        raw = AssemblyLineApi().get_raw_json()
        df_raw = DefineDataFrame(raw).extract_car_records()
        df_t1 = TransformDataFrame(df_raw).transform()
        df_final = TransformDataFrame(df_t1).attach_fx4pd()
        return df_final.to_dicts()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao transformar registros: {e}")


@router.post("/upsert"  )
def upsert_infos():
    try:
        raw = AssemblyLineApi().get_raw_json()
        df_raw = DefineDataFrame(raw).extract_car_records()
        df_t1 = TransformDataFrame(df_raw).transform()
        df_final = TransformDataFrame(df_t1).attach_fx4pd()

        rows = UpsertInfos().upsert_df(df_final, 10000)
        return {
            "message": "Upsert concluído com sucesso.",
            "rows": rows,
            "batch_size": 10000,
            "table": "assembly_line",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no upsert: {e}")