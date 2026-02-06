from fastapi import APIRouter, Query, Depends

from database.queries import UpsertInfos
from services.pk05.pk05 import PK05_Cleaner, PK05_DefineDataframe
from helpers.services.pk05 import BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.get("/response/raw", summary="Get Raw PK05 Values")
def get_raw_pk05(
    svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    limit: int = Query(50, ge=1, le=1000),
):
    try:
        df = svc.create_df().collect()
        return df.head(limit).to_dicts()

    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar PK05 bruto: ", e)


@router.get("/response/processed", summary="Get Cleaned PK05 Values")
def get_clean_pk05(
    raw_svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    cleaner_svc: PK05_Cleaner = Depends(DependenciesInjection.get_pk05_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    try:
        df = (BuildPipeline.build_pk05(raw_svc, cleaner_svc).head(limit).collect())
        return df.to_dicts()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao processar PK05 (clean): ", e)


@router.post("/upsert", summary="Upsert PK05 Values In The DataBase")
def upsert_pk05(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: PK05_DefineDataframe = Depends(DependenciesInjection.get_pk05),
    cleaner_svc: PK05_Cleaner = Depends(DependenciesInjection.get_pk05_cleaner),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    try:
        df = BuildPipeline.build_pk05(raw_svc, cleaner_svc)
        rows = upsert_svc.upsert_df("pk05", df, batch_size)

        return {
            "message": "Upsert PK05 concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pk05",
        }

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no upsert (pk05)", e)