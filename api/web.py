from fastapi import FastAPI, APIRouter, HTTPException, Query, Path
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn


from orchestrator.orchestrator import PipelinesOrchestrator, WorkersOrchestrator

app = FastAPI(title="API Sistema KNR", docs_url="/api-doc")
