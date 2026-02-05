from fastapi import FastAPI
from app.routes.health import router as health_router
from app.routes.ingest import router as ingestions_router

app = FastAPI(title="Letterboxd Data Pipeline API", version="0.1.0")

app.include_router(health_router)
app.include_router(ingestions_router)
