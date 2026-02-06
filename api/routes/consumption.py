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
def update_to_consume(svc: ConsumeValues = Depends(DependeciesInjection.get_consume)):
    try:
        return svc._update_infos()
    except Exception as e:
        raise HTTP_Exceptions().http_502("Erro ao buscar origem: ", e)