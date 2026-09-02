"""FastAPI app.

Endpoints built:
- GET  /stations
- GET  /stations/{id}/history   (cursor pagination)

Endpoints to build:
- GET  /stations/risk           (cached, rate-limited)
- POST /rebalance-tasks         (idempotency-key aware, outbox pattern)
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from dockwatch.api.db import pool
from dockwatch.api.routers import stations


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await pool.open()
    try:
        yield
    finally:
        await pool.close()


app = FastAPI(title="Dockwatch API", version="0.1.0", lifespan=lifespan)
app.include_router(stations.router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
