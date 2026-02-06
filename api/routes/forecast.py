from fastapi import APIRouter, Query, Depends

from database.queries import UpsertInfos

from services.forecast.buff_al import ReturnBuffAssemblyLineValues
from services.forecast.fx4pd import ReturnFX4PDValues
from services.forecast.forecaster import DefineForecastValues

from helpers.services.forecast import BuildPipeline, DependenciesInjection
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.get("/response/buffer-al", summary="Get Only Buffer Values From Assembly Line DataBase")
def get_buffer_al_response(
    svc: ReturnBuffAssemblyLineValues = Depends(DependenciesInjection.get_buff_al_service),
    limit: int = Query(5000, ge=1, le=100000),
):
    try:
        df = svc.return_values_from_db().collect()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar origem (buff_al): ", e)


@router.get("/response/fx4pd", summary="Get Values From FX4PD")
def get_fx4pd_response(
    svc: ReturnFX4PDValues = Depends(DependenciesInjection.get_fx4pd_service),
    limit: int = Query(5000, ge=1, le=100000),
):
    try:
        df = BuildPipeline().build_forecast(svc).collect()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar origem (fx4pd)", e)


@router.get("/result", summary="Get Values Forecasted")
def get_forecast_result(
    svc: DefineForecastValues = Depends(DependenciesInjection.get_forecast_service),
    limit: int = Query(5000, ge=1, le=100000),
):
    try:
        df = svc.join_fx4pd_pkmc_pk05().collect()
        return df.head(limit).to_dicts()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar origem (forecast)", e)


@router.post("/upsert/fx4pd", summary="Upsert FX4PD Values In The DataBase")
def upsert_fx4pd(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    fx4pd_svc: ReturnFX4PDValues = Depends(DependenciesInjection.get_fx4pd_service),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    try:
        df = BuildPipeline().build_forecast(fx4pd_svc)
        rows = upsert_svc.upsert_df("fx4pd", df, batch_size)
        return {
            "message": "Upsert concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "fx4pd",
        }
    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no upsert (fx4pd)", e)


@router.post("/upsert", summary="Upsert Forecasted Values In The DataBase")
def upsert_forecast_pipeline(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    fx4pd_svc: ReturnFX4PDValues = Depends(DependenciesInjection.get_fx4pd_service),
    forecast_svc: DefineForecastValues = Depends(DependenciesInjection.get_forecast_service),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    try:
        df_fx4pd = BuildPipeline().build_forecast(fx4pd_svc)
        upsert_svc.upsert_df("fx4pd", df_fx4pd, batch_size)

        df_forecast = forecast_svc.join_fx4pd_pkmc_pk05()
        rows_forecast = upsert_svc.upsert_df("forecast", df_forecast, batch_size)

        return {
            "message": "Upsert concluído com sucesso.",
            "rows": {
                "fx4pd": len(df_fx4pd),
                "forecast": rows_forecast,
            },
            "batch_size": batch_size,
            "tables": ["fx4pd", "forecast"],
        }
    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no upsert (pipeline forecast)", e)