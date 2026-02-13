from fastapi import FastAPI, Query
import logging

from app.core.logging import setup_logging
from app.core.config import settings
from app.db.schema import init_db
from app.jobs.runner import run_once

setup_logging()
log = logging.getLogger("orion-brain")

app = FastAPI(title=settings.APP_NAME)

@app.on_event("startup")
def _startup():
    init_db()
    log.info("db ready")

@app.get("/healthz")
def healthz():
    return {"ok": True, "service": settings.APP_NAME, "env": settings.ENV}

@app.get("/readyz")
def readyz():
    return {
        "ready": True,
        "pairs": settings.PAIRS.split(","),
        "strategies_url": settings.STRATEGIES_BASE_URL,
    }

@app.post("/v1/run")
def run(pairs: str = Query(default="")):
    universe = settings.PAIRS.split(",") if not pairs else [p.strip() for p in pairs.split(",")]
    # normalize to polygon ticker format
    universe = [f"C:{p}" if not p.startswith("C:") else p for p in universe]
    log.info(f"run_once pairs={universe}")
    res = run_once(universe)
    return res
