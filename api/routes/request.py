from fastapi import APIRouter, Query, Depends

from database.queries import UpsertInfos
from services.request.requester import QuantityToRequest, LM01_Requester
from services.sap.session_manager import SAPSessionManager
from helpers.services.request import DependenciesInjection, BuildPipeline

from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.get("/response/to-request", summary="Get Values To Request")
def get_to_request(
    svc: QuantityToRequest = Depends(DependenciesInjection.get_to_request),
    limit: int = Query(50, ge=1, le=1000),
):
    try:
        df = svc._define_diference_to_request().collect()
        return df.head(limit).to_dicts()

    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar valores para solicitar: ", e)


@router.post("/upsert/to-request", summary="Upsert To Request Values In The DataBase")
def upsert_to_request(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: QuantityToRequest = Depends(DependenciesInjection.get_to_request),
    upsert_svc: UpsertInfos = Depends(DependenciesInjection.get_upsert_service),
):
    try:
        df = BuildPipeline.build_to_request(svc)
        rows = upsert_svc.upsert_df("requests_made", df, batch_size)

        return {
            "message": "Upsert To Request concluído com sucesso.",
            "rows": rows,
            "batch_size": batch_size,
            "table": "requests_made",
        }

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no upsert (to request)", e)
    

@router.post("/requester", summary="Requester To Request Values In SAP")
def requester(
    svc: LM01_Requester = Depends(DependenciesInjection.get_lm01_requester),
    svc_sap_session: SAPSessionManager = Depends(DependenciesInjection.get_sap_session)
):
    try:
        session = svc_sap_session.get_session()
        if not session:
            raise Exception("Nenhuma sessão SAP ativa. Chame /sap/session primeiro.")

        rows = svc._request_lm01(session)

        return {
            "message": "Requester concluído com sucesso.",
            "rows": rows,
        }

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro no request (to request)", e)