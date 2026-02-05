from services.workers.sap.worker import sap_worker
from services.workers.consumption.worker import consumption_worker


WORKERS = {
    "consumption": consumption_worker,
    "sap": sap_worker
}
