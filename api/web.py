from fastapi import FastAPI
from orchestrator.orchestrator import PipelinesOrchestrator, WorkersOrchestrator
from starlette.middleware.timeout import TimeoutMiddleware
from fastapi.middleware.gzip import GZipMiddleware


app = FastAPI(
    title="Auto Line Feeding", 
    docs_url="/alf-doc",
    description="Backend API's for Auto Line Feeding System"
)

app.add_middleware(TimeoutMiddleware, timeout=20)
app.add_middleware(GZipMiddleware, minimum_size=1000)

@app.post("/pipeline/{pipeline_service}")
def run_pipeline(pipeline_service):
    return PipelinesOrchestrator().run_pipeline(pipeline_service)

@app.post("/worker/start/{worker_service}")
def start_worker(worker_service):
    return WorkersOrchestrator().start_worker(worker_service)

@app.post("/worker/stop/{worker_service}")
def stop_worker(worker_service):
    return WorkersOrchestrator().start_worker(worker_service)

