from fastapi import FastAPI, File, UploadFile
# from orchestrator.orchestrator import PipelinesOrchestrator, WorkersOrchestrator
# from services.storage import ListExcelFiles, UploadFiles, DeleteFiles
from fastapi.middleware.gzip import GZipMiddleware

from .routes.assembly import router as assembly_router
from .routes.forecast import router as forecast_router

app = FastAPI(
    title="Auto Line Feeding", 
    docs_url="/alf-doc",
    description="Backend API's for Auto Line Feeding System"
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(assembly_router, prefix="/assembly", tags=["assembly"])
app.include_router(forecast_router, prefix="/forecast", tags=["forecast"])



# -- PIPELINE --
# @app.post("/pipeline/{pipeline_service}", tags=["pipelines"])
# def run_pipeline(pipeline_service):
#     return PipelinesOrchestrator().run_pipeline(pipeline_service)


# # -- WORKERS --
# @app.post("/worker/start/{worker_service}", tags=["workers"])
# def start_worker(worker_service):
#     return WorkersOrchestrator().start_worker(worker_service)

# @app.post("/worker/stop/{worker_service}", tags=["workers"])
# def stop_worker(worker_service):
#     return WorkersOrchestrator().stop_workers(worker_service)


# # -- FILES -- 
# @app.get("/files/list/", tags=["files"])
# def list_files():
#     return ListExcelFiles()._list_files()

# @app.post("/files/upload/", tags=["files"])
# def upload_files(file: UploadFile = File(...)):
#     return UploadFiles()._upload_files(file)

# @app.delete("/delete/{filename}", tags=["files"])
# def delete_files(filename):
#     return DeleteFiles()._delete_files(filename)
