from fastapi import APIRouter, Query, Depends

from database.queries import UpsertInfos
from services.pkmc.pkmc import PKMC_DefineDataframe, PKMC_Cleaner
from helpers.services.pkmc import BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.get("/response/raw", summary="Get Raw PKMC Values")
def get_raw_pkmc(
    svc: PKMC_DefineDataframe = Depends(DependenciesInjection.get_pkmc),
    limit: int = Query(50, ge=1, le=1000),
):
    try:
        df = svc.create_df().collect()
        return df.head(limit).to_dicts()

    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar PKMC bruto: ", e)


@router.get("/response/processed", summary="Get Cleaned PKMC Values")
def get_clean_pkmc(
    raw_svc: PKMC_DefineDataframe = Depends(DependenciesInjection.get_pkmc),
    cleaner_svc: PKMC_Cleaner = Depends(DependenciesInjection.get_pkmc_cleaner),
    limit: int = Query(50, ge=1, le=1000),
):
    try:
        df = (BuildPipeline.build_pkmc(raw_svc, cleaner_svc).head(limit).collect())
        return df.to_dicts()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao processar PKMC (clean): ", e)


@router.post("/upsert", summary="Upsert PKMC Values In The DataBase")
def upsert_pkmc(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    raw_svc: PKMC_DefineDataframe = Depends(DependenciesInjection.get_pkmc),
    cleaner_svc: PKMC_Cleaner = Depends(DependenciesInjection.get_pkmc_cleaner),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    try:
        df = BuildPipeline.build_pkmc(raw_svc, cleaner_svc)
        rows = upsert_svc.upsert_df("pkmc", df, batch_size)

        return {
            "message": "Upsert PKMC concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "pkmc",
        }

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no upsert (pkmc)", e)