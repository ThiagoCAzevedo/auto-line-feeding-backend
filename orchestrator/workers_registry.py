
# from services.workers.lines.assembly.worker import al_worker
from services.workers.sap.worker import sap_worker



WORKERS = {
    # "assembly_line": al_worker,
    "sap": sap_worker
}
