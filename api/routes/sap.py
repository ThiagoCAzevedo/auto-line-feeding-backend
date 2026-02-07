from fastapi import APIRouter, Depends
from helpers.services.sap import DependenciesInjection 
from services.sap.client import SAP_Launcher, SAP_Authenticator, SAP_SessionProvider, SAP_Client
from services.sap.session_manager import SAPSessionManager
from helpers.services.http_exception import HTTP_Exceptions


router = APIRouter()


@router.post("/session", summary="Creates a SAP Session And Stores It")
def create_sap_session(
    client: SAP_Client = Depends(DependenciesInjection.get_sap_client),
    session_manager: SAPSessionManager = Depends(DependenciesInjection.get_sap_session)
):
    try:
        session = client.connect()
        session_manager.set_session(session)

        return {"message": "SAP session created successfully!"}

    except Exception as e:
        raise HTTP_Exceptions().http_500("Erro ao criar sessão SAP", e)