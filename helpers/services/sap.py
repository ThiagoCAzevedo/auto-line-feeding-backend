from services.sap.session_manager import SAPSessionManager
from services.sap.client import SAP_Launcher, SAP_Authenticator, SAP_SessionProvider, SAP_Client


class DependenciesInjection:
    @staticmethod
    def get_sap_client():
        launcher = SAP_Launcher()
        provider = SAP_SessionProvider(launcher)
        auth = SAP_Authenticator()
        return SAP_Client(provider, auth, launcher)

    @staticmethod
    def get_sap_session():
        return SAPSessionManager