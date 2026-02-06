from fastapi import APIRouter, Query, Depends

from services.consumption.consumer import ConsumeValues

from helpers.services.consumption import DependeciesInjection
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.get("/response/to-consume", summary="Get Values To Consume")
def get_to_consume_response(svc: ConsumeValues = Depends(DependeciesInjection.get_consume)):
    try:
        return svc.get_raw_response()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar origem: ", e)


@router.put("/update/to-consume", summary="Update Values To Consume")
def update_to_consume(
    batch_size: int = Query(10_000, ge=1, le=100_000),
    svc: ConsumeValues = Depends(DependeciesInjection.get_consume),
):
    try:
        df = svc.values_to_consume()
        updated_rows = svc._update_infos(df=df, batch_size=batch_size)

        return {
            "message": "Update executado com sucesso.",
            "rows_updated": updated_rows,
            "batch_size": batch_size,
        }

    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao atualizar valores (to-consume): ", e)