"""FastAPI app. Phase 3 stub.

Endpoints to build:
- GET  /stations
- GET  /stations/{id}/history   (cursor pagination)
- GET  /stations/risk           (cached, rate-limited)
- POST /rebalance-tasks         (idempotency-key aware, outbox pattern)
"""

from fastapi import FastAPI

app = FastAPI(title="Dockwatch API", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
