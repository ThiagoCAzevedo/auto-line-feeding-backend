from fastapi import FastAPI, APIRouter, HTTPException, Query, Path
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional
from datetime import datetime
from pathlib import Path
from backend.orchestrators import main_orchestrator
from backend.database import db_manager_api
import uvicorn


app = FastAPI(title="API Sistema KNR", docs_url="/api-doc")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

routes_start  = APIRouter(prefix="/iniciar/sistema", tags=["Iniciar"])
routes_stop   = APIRouter(prefix="/parar/sistema", tags=["Parar"])
routes_pages  = APIRouter(prefix="/sistema", tags=["Páginas"])
routes_data   = APIRouter(prefix="/data", tags=["Dados"])
routes_api    = APIRouter(prefix="/api", tags=["API"])
routes_logs   = APIRouter(prefix="/logs", tags=["Logs"])
routes_status = APIRouter(prefix="/status", tags=["Status"])

template_path = Path(__file__).resolve().parents[2] / "frontend" / "templates"
templates     = Jinja2Templates(directory=template_path)


# --- ROTAS DE LOG ---
@routes_logs.get("/knr")
async def get_knr_logs():
    logs = db_manager_api.read_log_file("knr")
    return {
        "logs": logs,
        "count": len(logs),
        "file": "knr.log"
    }


@routes_logs.get("/linhas")
async def get_linha_logs():
    logs = db_manager_api.read_log_file("linhas")
    return {
        "logs": logs,
        "count": len(logs),
        "file": "linhas.log"
    }


# --- API ROUTES (PRATELEIRAS E TACTOS) ---
@routes_api.get("/tacto")
async def get_by_tacto(
    tacto: Optional[str] = Query(None, description="Código do tacto (opcional)")
):
    result = db_manager_api.get_by_tacto(tacto)
    if "error" in result and not result.get("shelves"):
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@routes_api.get("/prateleira")
async def get_by_prateleira(
    prateleira: Optional[str] = Query(None, description="Código da prateleira (opcional)")
):
    result = db_manager_api.get_by_prateleira(prateleira)
    if "error" in result and not result.get("shelves"):
        raise HTTPException(status_code=500, detail=result["error"])
    return result


@routes_api.get("/ots/sem-conclusao")
async def get_open_ots():
    result = db_manager_api.get_lt22_stil_opened()
    if result is None or "error" in result:
        raise HTTPException(status_code=500, detail="Erro ao buscar OTs abertas")
    return result


# --- ROTAS DE INICIO ---
@routes_start.get("/knr-completo")
def start_all_systems():
    results = {
        "linhas": main_orchestrator.lines_start(),
        "knr": main_orchestrator.knr_start(),
        "sap": main_orchestrator.sap_start()
    }
    return results


@routes_start.get("/linhas")
def start_linhas():
    return main_orchestrator.lines_start()

@routes_start.get("/knr")
def knr_start():
    return main_orchestrator.knr_start()

@routes_start.get("/sap")
def knr_start():
    return main_orchestrator.sap_start()


# --- ROTAS DE PARADA ---
@routes_stop.get("/knr-completo")
def stop_all_services():
    results = {
        "linhas": main_orchestrator.lines_stop(),
        "knr": main_orchestrator.knr_stop(),
        "sap": main_orchestrator.sap_stop()
    }
    return results


@routes_stop.get("/linhas")
def lines_stop():
    return main_orchestrator.lines_stop()


@routes_stop.get("/knr")
def knr_stop():
    return main_orchestrator.knr_stop()

@routes_stop.get("/sap")
def knr_stop():
    return main_orchestrator.sap_stop()


# --- ROTAS DE STATUS ---
@routes_status.get("/")
def get_status():
    return main_orchestrator.get_status()


@routes_status.get("/detailed")
def get_detailed_status():
    status = main_orchestrator.get_status()
    return {
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "systems": {
            "linhas": status.get("linhas", "unknown"),
            "knr": status.get("knr", "unknown"),
            "sap": status.get("sap", "unknown"),
        }
    }


# --- ROTAS DE INFO (apenas para visualizar com FastAPI mesmo) ---
@routes_data.get("/dashboard")
async def get_dashboard_data():
    return db_manager_api.get_dashboard_data()


# --- ROTAS DE PÁGINAS (apenas para visualizar com FastAPI mesmo) ---
@routes_pages.get("/controle", response_class=HTMLResponse)
async def get_control_page():
    templates_dir = Path(__file__).resolve().parents[2] / "frontend" / "templates"
    control_file = templates_dir / "controle.html"

    if control_file.exists():
        with open(control_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(
            content="<h1>Arquivo controle.html não encontrado</h1>", status_code=404
        )


@routes_pages.get("/dashboard", response_class=HTMLResponse)
async def get_dashboard_page():
    templates_dir = Path(__file__).resolve().parents[2] / "frontend" / "templates"
    dashboard_file = templates_dir / "dashboard.html"

    if dashboard_file.exists():
        with open(dashboard_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    else:
        return HTMLResponse(
            content="<h1>Arquivo dashboard.html não encontrado</h1>", status_code=404
        )


# --- INCLUIR ROTAS ---
app.include_router(routes_start)
app.include_router(routes_stop)
app.include_router(routes_pages)
app.include_router(routes_data)
app.include_router(routes_api)
app.include_router(routes_logs)
app.include_router(routes_status)


# --- ROTAS ROOT ---
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "Sistema Sesé API",
        "timestamp": datetime.now().isoformat()
    }


@app.get("/")
async def root():
    return HTMLResponse(
        content="""
    <html>
        <head>
            <title>Sistema Sesé</title>
            <meta http-equiv="refresh" content="0; url=/sistema/dashboard">
        </head>
        <body>
            <p>Redirecionando para o dashboard...</p>
        </body>
    </html>
    """
    )


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)