from services.request.requester import QuantityToRequest, LM01_Requester
from services.sap.session_manager import SAPSessionManager
from database.queries import UpsertInfos


class BuildPipeline:
    @staticmethod
    def build_to_request(svc: QuantityToRequest):
        return svc._define_diference_to_request()
    
    
class DependenciesInjection:
    @staticmethod
    def get_to_request() -> QuantityToRequest:
        return QuantityToRequest()
    
    @staticmethod
    def get_lm01_requester() -> LM01_Requester:
        return LM01_Requester()
    
    @staticmethod
    def get_sap_session() -> SAPSessionManager:
        return SAPSessionManager()

    @staticmethod
    def get_upsert_service() -> UpsertInfos:
        return UpsertInfos()