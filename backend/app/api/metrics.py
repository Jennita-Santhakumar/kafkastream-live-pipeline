import pandas as pd
from fastapi import APIRouter, Query
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest

from app.core.delta_reader import read_alerts, read_pipeline_metrics, read_table
from app.schemas.responses import MetricPointOut, MetricsSummaryOut

router = APIRouter(prefix="/api/metrics", tags=["metrics"])

_events_per_sec_gauge = Gauge("pipeline_events_per_sec", "Latest measured throughput")
_p95_lag_gauge = Gauge("pipeline_p95_lag_ms", "Latest measured p95 processing lag (ms)")
_total_events_gauge = Gauge("pipeline_total_events", "Total events written to Delta so far")
_alerts_last_hour_gauge = Gauge("pipeline_alerts_last_hour", "Alerts raised in the last hour")


def _summary() -> dict:
    metrics_df = read_pipeline_metrics()
    events_df = read_table("events")
    alerts_df = read_alerts()

    if metrics_df.empty:
        return {
            "total_events": 0 if events_df.empty else len(events_df),
            "current_events_per_sec": 0.0,
            "current_p95_lag_ms": 0.0,
            "alerts_last_hour": 0,
            "latest_batch_id": None,
        }

    latest = metrics_df.tail(5)
    alerts_last_hour = 0
    if not alerts_df.empty:
        cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(hours=1)
        alerts_last_hour = int((alerts_df["created_at"] >= cutoff).sum())

    return {
        "total_events": 0 if events_df.empty else len(events_df),
        "current_events_per_sec": round(float(latest["events_per_sec"].mean()), 2),
        "current_p95_lag_ms": round(float(latest["p95_lag_ms"].mean()), 2),
        "alerts_last_hour": alerts_last_hour,
        "latest_batch_id": int(metrics_df.iloc[-1]["batch_id"]),
    }


@router.get("/timeseries", response_model=list[MetricPointOut])
def metrics_timeseries(minutes: int = Query(15, ge=1, le=1440)) -> list[dict]:
    df = read_pipeline_metrics()
    if df.empty:
        return []
    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(minutes=minutes)
    df = df[df["processed_at"] >= cutoff]
    return df.to_dict(orient="records")


@router.get("/summary", response_model=MetricsSummaryOut)
def metrics_summary() -> dict:
    return _summary()


def render_prometheus_metrics() -> PlainTextResponse:
    """Refreshes the business-metric gauges from the latest Delta state and
    renders them in Prometheus text exposition format. Mounted at the
    conventional root `/metrics` scrape path in app.main, not under /api."""
    summary = _summary()
    _events_per_sec_gauge.set(summary["current_events_per_sec"])
    _p95_lag_gauge.set(summary["current_p95_lag_ms"])
    _total_events_gauge.set(summary["total_events"])
    _alerts_last_hour_gauge.set(summary["alerts_last_hour"])
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
