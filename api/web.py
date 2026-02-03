from fastapi import FastAPI, APIRouter, HTTPException, Query, Path
from fastapi.responses import HTMLResponse
from orchestrator.orchestrator import PipelinesOrchestrator, WorkersOrchestrator


app = FastAPI(title="Auto Line Feeding", docs_url="/alf-doc")

@app.post("/pipeline/{pipeline_service}")
def run_pipeline(pipeline_service):
    return PipelinesOrchestrator().run_pipeline(pipeline_service)

@app.post("/worker/start/{worker_service}")
def start_worker(worker_service):
    return WorkersOrchestrator().start_worker(worker_service)

@app.post("/worker/stop/{worker_service}")
def stop_worker(worker_service):
    return WorkersOrchestrator().start_worker(worker_service)

