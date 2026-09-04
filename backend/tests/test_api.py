from conftest import _write_alerts, _write_event_stats, _write_events, _write_pipeline_metrics


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_recent_events_empty_when_no_table(client):
    resp = client.get("/api/events/recent")
    assert resp.status_code == 200
    assert resp.json() == []


def test_recent_events_returns_written_rows(client, data_root):
    _write_events(data_root, n=5)
    resp = client.get("/api/events/recent?limit=3")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 3
    # most recent (e0) first
    assert body[0]["event_id"] == "e0"


def test_windowed_stats_sums_partial_batches(client, data_root):
    _write_event_stats(data_root)
    resp = client.get("/api/stats/windowed?minutes=1440")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    row = body[0]
    # two partial rows: count 3 @ avg 20, count 1 @ avg 60 -> total count 4, avg (60+60)/4=30
    assert row["event_count"] == 4
    assert row["avg_value"] == 30.0
    assert row["max_value"] == 60.0


def test_alerts_endpoint_returns_written_alert(client, data_root):
    _write_alerts(data_root)
    resp = client.get("/api/alerts")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["alert_type"] == "volume_anomaly"


def test_metrics_summary_aggregates_recent_batches(client, data_root):
    _write_pipeline_metrics(data_root, n=3)
    resp = client.get("/api/metrics/summary")
    assert resp.status_code == 200
    body = resp.json()
    assert body["latest_batch_id"] == 2
    assert body["current_events_per_sec"] > 0


def test_prometheus_metrics_endpoint_exposes_gauges(client, data_root):
    _write_pipeline_metrics(data_root, n=1)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "pipeline_events_per_sec" in resp.text
