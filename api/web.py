from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware

from .routes.assembly import router as assembly_router
from .routes.forecast import router as forecast_router
from .routes.consumption import router as consumption_router
from .routes.files import router as static_files_router
from .routes.pkmc import router as pkmc_router
from .routes.pk05 import router as pk05_router



app = FastAPI(
    title="Auto Line Feeding", 
    docs_url="/alf-doc",
    description="Backend API's for Auto Line Feeding System"
)

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.include_router(assembly_router, prefix="/assembly", tags=["assembly"])
app.include_router(forecast_router, prefix="/forecast", tags=["forecast"])
app.include_router(consumption_router, prefix="/consumption", tags=["consumption"])
app.include_router(pkmc_router, prefix="/pkmc", tags=["pkmc"])
app.include_router(pk05_router, prefix="/pk05", tags=["pk05"])
app.include_router(static_files_router, prefix="/files", tags=["files"])

