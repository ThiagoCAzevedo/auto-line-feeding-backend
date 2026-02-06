from fastapi import FastAPI, File, UploadFile
# from orchestrator.orchestrator import PipelinesOrchestrator, WorkersOrchestrator
# from services.storage import ListExcelFiles, UploadFiles, DeleteFiles
from fastapi.middleware.gzip import GZipMiddleware

from .routes.assembly import router as assembly_router
from .routes.forecast import router as forecast_router
from .routes.consumption import router as consumption_router


app = FastAPI(
    title="Auto Line Feeding", 
    docs_url="/alf-doc",
    description="Backend API's for Auto Line Feeding System"
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(assembly_router, prefix="/assembly", tags=["assembly"])
app.include_router(forecast_router, prefix="/forecast", tags=["forecast"])
app.include_router(consumption_router, prefix="/consumption", tags=["consumption"])


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
