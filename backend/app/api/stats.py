import pandas as pd
from fastapi import APIRouter, Query

from app.core.delta_reader import read_table
from app.schemas.responses import WindowedStatOut

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/windowed", response_model=list[WindowedStatOut])
def windowed_stats(
    minutes: int = Query(60, ge=1, le=1440),
    event_type: str | None = None,
) -> list[dict]:
    """Roll up per-micro-batch partial window aggregates into final per-window
    totals. The streaming job appends one partial row per micro-batch for
    each (window, event_type) it saw data for, so summing event_count (a
    true additive count) and re-deriving avg_value from the underlying
    sum/count (rather than averaging the partial averages) gives the correct
    final aggregate."""
    df = read_table("event_stats")
    if df.empty:
        return []

    if event_type:
        df = df[df["event_type"] == event_type]

    # Real Spark-written rows come back tz-aware (UTC) from Delta's TimestampType;
    # `utc=True` also normalizes any naive timestamps (e.g. from a test fixture)
    # to UTC instead of leaving a naive/aware mismatch that pandas can't compare.
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(minutes=minutes)
    df = df[df["window_start"] >= cutoff]
    if df.empty:
        return []

    df["value_sum"] = df["avg_value"] * df["event_count"]
    grouped = (
        df.groupby(["window_start", "window_end", "event_type"])
        .agg(event_count=("event_count", "sum"), value_sum=("value_sum", "sum"), max_value=("max_value", "max"))
        .reset_index()
    )
    grouped["avg_value"] = grouped["value_sum"] / grouped["event_count"]
    grouped = grouped.drop(columns=["value_sum"]).sort_values("window_start")
    return grouped.to_dict(orient="records")
