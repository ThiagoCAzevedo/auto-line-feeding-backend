from client import SapLauncher, SapSessionProvider, SapAuthenticator, SapClient
from requester import DefineDataFrame, QuantityToRequest, LM01_Requester
from reqdone import LT22_Session, LT22_Selectors, LT22_Parameters, LT22_Submit
from listreq import SP02_Session, SP02_Rows, SP02_Actions


def initialize_sap():
    launcher = SapLauncher()
    provider = SapSessionProvider(launcher)
    auth = SapAuthenticator()
    sap = SapClient(provider, auth, launcher)
    return sap


def requester(sap):
    df = QuantityToRequest()._define_diference_to_request()
    LM01_Requester()._request_lm01(sap, df)


def sap_worker():
    sap = initialize_sap()
    requester(sap)