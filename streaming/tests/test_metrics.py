import time

from metrics import compute_batch_metrics


def test_compute_batch_metrics_empty_batch(spark):
    empty_df = spark.createDataFrame([], "event_time timestamp")
    result = compute_batch_metrics(empty_df, batch_id=0, batch_start_ts=time.time())
    assert result["event_count"] == 0
    assert result["events_per_sec"] == 0.0


def test_compute_batch_metrics_counts_events(spark):
    df = spark.createDataFrame(
        [("2024-01-01 00:00:00",)] * 5, ["event_time"]
    ).selectExpr("cast(event_time as timestamp) as event_time")
    result = compute_batch_metrics(df, batch_id=1, batch_start_ts=time.time())
    assert result["event_count"] == 5
    assert result["avg_lag_ms"] > 0  # event_time is far in the past
