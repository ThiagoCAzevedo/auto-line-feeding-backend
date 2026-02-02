from access_api import AssemblyLineApi

def access_al_api():
    return AssemblyLineApi()._return_response()

def al_worker():
    response = access_al_api()