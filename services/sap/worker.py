from .client import SAP_Launcher, SAP_SessionProvider, SAP_Authenticator, SAP_Client
from .requester import QuantityToRequest, LM01_Requester
from .reqdone import LT22_Session, LT22_Selectors, LT22_Parameters, LT22_Submit
from .listreq import SP02_Session, SP02_Rows, SP02_Actions


def initialize_sap():
    launcher = SAP_Launcher()
    provider = SAP_SessionProvider(launcher)
    auth = SAP_Authenticator()
    sap = SAP_Client(provider, auth, launcher)
    return sap


def lt22_verify_requests(sap):
    session = LT22_Session(sap).open()

    selectors = LT22_Selectors(session)
    params = LT22_Parameters(session)
    submit = LT22_Submit(session)

    params.set_deposit()

    selectors.take()

    params.set_b01()
    params.set_pending_only()
    params.set_dates_today()
    params.set_layout()

    submit.send()


def sp02_download_latest_lt22(sap):
    session = SP02_Session(sap).open()
    rows = SP02_Rows(session)
    job = rows.find_lt22_job()

    if job:
        actions = SP02_Actions(sap)
        actions.download(session, job["index"])
        actions.clean(job["index"])


def lm01_request(sap):
    df = QuantityToRequest()._define_diference_to_request()
    LM01_Requester(sap, df)._request_lm01()


def sap_worker():
    sap = initialize_sap()

    lm01_request(sap)
    lt22_verify_requests(sap)
    sp02_download_latest_lt22(sap)