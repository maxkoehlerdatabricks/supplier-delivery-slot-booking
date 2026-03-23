import logging
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from server.routes import slots, bookings, pos
from server.db import db_pool

logger = logging.getLogger(__name__)
FRONTEND_DIR = Path(__file__).parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await db_pool.initialize()
    except Exception as e:
        logger.warning(
            f"Could not connect to Lakebase: {e}. "
            "API routes will fail but frontend will be served."
        )
    yield
    try:
        await db_pool.close()
    except Exception:
        pass


app = FastAPI(title="Supplier Delivery Slot Booking API", lifespan=lifespan)

app.include_router(slots.router, prefix="/api")
app.include_router(bookings.router, prefix="/api")
app.include_router(pos.router, prefix="/api")


@app.exception_handler(RuntimeError)
async def runtime_error_handler(request: Request, exc: RuntimeError):
    """Return 503 when Lakebase is unavailable."""
    return JSONResponse(
        status_code=503,
        content={"detail": str(exc)},
    )


if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")


@app.get("/api/health")
async def health():
    return {"status": "ok", "db_connected": db_pool.is_connected}


@app.get("/{full_path:path}")
async def serve_spa(request: Request, full_path: str):
    if full_path.startswith("api/"):
        return JSONResponse(status_code=404, content={"error": "not found"})
    file_path = FRONTEND_DIR / full_path
    if file_path.is_file():
        return FileResponse(file_path)
    return FileResponse(FRONTEND_DIR / "index.html")
