"""Per-micro-batch throughput and processing-lag measurement."""
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, expr


def compute_batch_metrics(batch_df: DataFrame, batch_id: int, batch_start_ts: float) -> dict:
    """Compute throughput and end-to-end processing lag for one micro-batch.

    Returns a plain dict (not a Spark row) so callers can write it straight into
    a Delta table via a single-row DataFrame or aggregate it further.
    """
    if batch_df.rdd.isEmpty():
        return {
            "batch_id": batch_id,
            "event_count": 0,
            "events_per_sec": 0.0,
            "avg_lag_ms": 0.0,
            "p95_lag_ms": 0.0,
            "processed_at": batch_start_ts,
        }

    lag_df = batch_df.withColumn(
        "lag_ms", (current_timestamp().cast("double") - col("event_time").cast("double")) * 1000
    )
    stats = lag_df.agg(
        expr("count(*) as event_count"),
        expr("avg(lag_ms) as avg_lag_ms"),
        expr("percentile_approx(lag_ms, 0.95) as p95_lag_ms"),
    ).first()

    elapsed = max(time.time() - batch_start_ts, 1e-6)
    event_count = int(stats["event_count"])

    return {
        "batch_id": batch_id,
        "event_count": event_count,
        "events_per_sec": round(event_count / elapsed, 2),
        "avg_lag_ms": round(float(stats["avg_lag_ms"] or 0.0), 2),
        "p95_lag_ms": round(float(stats["p95_lag_ms"] or 0.0), 2),
        "processed_at": batch_start_ts,
    }
