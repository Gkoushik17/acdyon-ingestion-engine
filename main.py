"""
FastAPI Application Entrypoint.
Exposes REST API endpoints, Server-Sent Events (SSE) stream, and serves the interactive UI dashboard.
"""

import os
from fastapi import FastAPI, Request, Query, Body, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any

from database import get_all_jobs, get_stats
from services.ingestion import IngestionOrchestrator
from services.resilience import circuit_breakers, global_chaos
from services.sources import SOURCES_REGISTRY

app = FastAPI(
    title="Acdyon Resilient Job Ingestion Service",
    description="Fault-tolerant job data ingestion engine with anti-detection pacing, circuit breakers, and schema validation.",
    version="1.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static files and Templates
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    """Serves the interactive dark-mode diagnostics dashboard."""
    sources = list(SOURCES_REGISTRY.keys())
    stats = get_stats()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "sources": sources,
            "stats": stats
        }
    )


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    from database import DB_BACKEND
    return {
        "status": "healthy",
        "service": "acdyon-ingestion-engine",
        "database_backend": DB_BACKEND,
        "registered_sources": list(SOURCES_REGISTRY.keys())
    }


@app.get("/api/stats")
async def fetch_statistics():
    """Returns database summary statistics, run history, and drift alerts."""
    stats = get_stats()
    return {
        "metrics": stats
    }


@app.get("/api/jobs")
async def list_jobs(
    limit: int = Query(50, ge=1, le=200),
    source: Optional[str] = Query(None),
    search: Optional[str] = Query(None)
):
    """Retrieve normalized jobs from SQLite."""
    jobs = get_all_jobs(limit=limit, source=source, search=search)
    return {
        "count": len(jobs),
        "jobs": jobs
    }


@app.get("/api/ingest")
async def trigger_ingest_stream(
    primary: str = Query("SandboxSource"),
    fallback: str = Query("WeWorkRemotely"),
    limit: int = Query(8, ge=1, le=25)
):
    """
    Executes the ingestion pipeline and streams real-time SSE progress events.
    """
    return StreamingResponse(
        IngestionOrchestrator.run_pipeline_stream(
            primary_source_name=primary,
            fallback_source_name=fallback,
            item_limit=limit
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.get("/api/circuit-status")
async def get_circuit_breakers_status():
    """Returns status of all active circuit breakers."""
    return {name: cb.get_status().model_dump() for name, cb in circuit_breakers.items()}


@app.post("/api/circuit-reset")
async def reset_circuit_breaker(payload: Dict[str, str] = Body(...)):
    """Manually resets a tripped circuit breaker."""
    source_name = payload.get("source")
    if source_name and source_name in circuit_breakers:
        circuit_breakers[source_name].reset()
        return {"status": "reset", "source": source_name, "breaker": circuit_breakers[source_name].get_status().model_dump()}
    raise HTTPException(status_code=404, detail=f"Source {source_name} not found.")


@app.get("/api/chaos")
async def get_chaos_config():
    """Returns active chaos simulation toggles."""
    return global_chaos.get_status()


@app.post("/api/chaos")
async def set_chaos_config(payload: Dict[str, Any] = Body(...)):
    """Configures simulated 429, 403, and schema drift triggers for demonstration."""
    global_chaos.configure(
        rate_limit=bool(payload.get("rate_limit", False)),
        bot_block=bool(payload.get("bot_block", False)),
        schema_drift=bool(payload.get("schema_drift", False)),
        latency=float(payload.get("latency", 0.0))
    )
    return {"status": "configured", "chaos": global_chaos.get_status()}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
