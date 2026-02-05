from services.workers.sap.worker import sap_worker
from services.workers.forecast.worker import forecast_worker


WORKERS = {
    "forecast": forecast_worker,
    "sap": sap_worker
}
