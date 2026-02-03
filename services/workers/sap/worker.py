from client import SapLauncher, SapSessionProvider, SapAuthenticator, SapClient
from requester import DefineDataFrame, QuantityToRequest, Requester


def initialize_sap():
    launcher = SapLauncher()
    provider = SapSessionProvider(launcher)
    auth = SapAuthenticator()
    sap = SapClient(provider, auth, launcher)
    return sap


def requester(sap):
    df = QuantityToRequest()._define_diference_to_request()
    Requester()._request_lm01(sap, df)