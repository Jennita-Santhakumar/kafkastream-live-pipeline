import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import alerts, events, metrics, stats
from app.api.metrics import render_prometheus_metrics
from app.core.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("realtime-streaming-api")

app = FastAPI(
    title="Realtime Event Streaming Pipeline API",
    description=(
        "Read-only API over the Delta Lake tables written by the Kafka + "
        "PySpark Structured Streaming pipeline: events, windowed stats, "
        "alerts, and pipeline health metrics."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origin_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(events.router)
app.include_router(stats.router)
app.include_router(alerts.router)
app.include_router(metrics.router)


@app.get("/metrics")
def prometheus_metrics():
    return render_prometheus_metrics()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
