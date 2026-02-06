from fastapi import APIRouter, HTTPException, Query, Depends
from services.assembly.assembly_api import AccessAssemblyLineApi
from database.queries import UpsertInfos
from helpers.services.assembly import BuildPipeline, DependeciesInjection
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.get("/response/raw")
def get_raw_response(api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api)):
    try:
        return api.get_raw_response()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar origem: ", e)


@router.get("/response/processed")
def get_processed_response(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    limit: int = Query(5000, ge=1, le=100000)
):
    try:
        df = BuildPipeline().build_assembly(api)
        return df.head(limit).to_dicts()
    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao processar registros:", e)


@router.post("/upsert")
def upsert_assembly(
    api: AccessAssemblyLineApi = Depends(DependeciesInjection.get_api),
    upsert: UpsertInfos = Depends(DependeciesInjection.get_upsert),
    batch_size: int = Query(10000, ge=1, le=100000)
):
    try:
        df = BuildPipeline().build_assembly(api)
        rows = upsert.upsert_df("assembly_line", df, batch_size)

        return {
            "message": "Upsert concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "assembly_line",
        }
    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no upsert:", e)