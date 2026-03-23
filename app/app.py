import os
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from server.routes import slots, bookings, pos
from server.db import db_pool

FRONTEND_DIR = Path(__file__).parent / "frontend" / "dist"

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db_pool.initialize()
    yield
    await db_pool.close()

app = FastAPI(title="Supplier Delivery Slot Booking API", lifespan=lifespan)

app.include_router(slots.router, prefix="/api")
app.include_router(bookings.router, prefix="/api")
app.include_router(pos.router, prefix="/api")

if FRONTEND_DIR.exists():
    app.mount("/assets", StaticFiles(directory=FRONTEND_DIR / "assets"), name="assets")

@app.get("/api/health")
async def health():
    return {"status": "ok"}

@app.get("/{full_path:path}")
async def serve_spa(request: Request, full_path: str):
    if full_path.startswith("api/"):
        return {"error": "not found"}
    file_path = FRONTEND_DIR / full_path
    if file_path.is_file():
        return FileResponse(file_path)
    return FileResponse(FRONTEND_DIR / "index.html")
