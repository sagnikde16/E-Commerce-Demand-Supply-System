"""
FastAPI application entry point.
Provides REST API access to demand-supply analytics.
"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.routes import demand, inventory, supply, products, health

app = FastAPI(
    title="E-Commerce Demand-Supply API",
    description=(
        "REST API for the distributed e-commerce demand-supply analytics system "
        "powered by Apache Spark."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(health.router,    tags=["Health"])
app.include_router(demand.router,    prefix="/api/v1/demand",    tags=["Demand"])
app.include_router(inventory.router, prefix="/api/v1/inventory", tags=["Inventory"])
app.include_router(supply.router,    prefix="/api/v1/supply",    tags=["Supply"])
app.include_router(products.router,  prefix="/api/v1/products",  tags=["Products"])


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "detail": str(exc)},
    )


def start():
    """Entry point for uvicorn."""
    uvicorn.run("src.api.main:app", host="0.0.0.0", port=8000, reload=True)


if __name__ == "__main__":
    start()
