from client import SapLauncher, SapSessionProvider, SapAuthenticator, SapClient
from requester import QuantityToRequest, LM01_Requester
from reqdone import LT22_Session, LT22_Selectors, LT22_Parameters, LT22_Submit
from listreq import SP02_Session, SP02_Rows, SP02_Actions


def initialize_sap():
    launcher = SapLauncher()
    provider = SapSessionProvider(launcher)
    auth = SapAuthenticator()
    sap = SapClient(provider, auth, launcher)
    return sap


def lt22_verify_requests(sap):
    session = LT22_Session(sap).open()

    selectors = LT22_Selectors(session)
    params = LT22_Parameters(session)
    submit = LT22_Submit(session)

    params.set_deposit()

    selectors.expand("         68")
    selectors.select("        108")
    selectors.select("        123")
    selectors.top("        123")
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
    LM01_Requester()._request_lm01(sap, df)


def sap_worker():
    sap = initialize_sap()

    lm01_request(sap)
    lt22_verify_requests(sap)
    sp02_download_latest_lt22(sap)