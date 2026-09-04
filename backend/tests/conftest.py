import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from deltalake import write_deltalake
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setenv("DELTA_DATA_ROOT", str(root))

    from app.core.config import get_settings

    get_settings.cache_clear()
    yield root
    get_settings.cache_clear()


@pytest.fixture()
def client(data_root):
    from app.main import app

    return TestClient(app)


def _write_events(root: Path, n: int = 5) -> pd.DataFrame:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = pd.DataFrame(
        [
            {
                "event_id": f"e{i}",
                "user_id": f"user_{i % 2 + 1}",
                "event_type": "purchase" if i % 2 == 0 else "page_view",
                "event_time": now - timedelta(seconds=i),
                "value": float(10 * (i + 1)),
                "user_segment": "high_value",
                "country": "US",
            }
            for i in range(n)
        ]
    )
    write_deltalake(str(root / "events"), df, mode="append")
    return df


def _write_event_stats(root: Path) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None).replace(second=0, microsecond=0)
    df = pd.DataFrame(
        [
            {
                "window_start": now,
                "window_end": now + timedelta(minutes=5),
                "event_type": "purchase",
                "event_count": 3,
                "avg_value": 20.0,
                "max_value": 40.0,
            },
            {
                "window_start": now,
                "window_end": now + timedelta(minutes=5),
                "event_type": "purchase",
                "event_count": 1,
                "avg_value": 60.0,
                "max_value": 60.0,
            },
        ]
    )
    write_deltalake(str(root / "event_stats"), df, mode="append")


def _write_alerts(root: Path) -> None:
    now_epoch = datetime.now(timezone.utc).timestamp()
    df = pd.DataFrame(
        [
            {
                "alert_type": "volume_anomaly",
                "message": "test alert",
                "severity": "high",
                "batch_id": 1,
                "created_at": now_epoch,
            }
        ]
    )
    write_deltalake(str(root / "alerts"), df, mode="append")


def _write_pipeline_metrics(root: Path, n: int = 3) -> None:
    now_epoch = datetime.now(timezone.utc).timestamp()
    df = pd.DataFrame(
        [
            {
                "batch_id": i,
                "event_count": 100 + i,
                "events_per_sec": 50.0 + i,
                "avg_lag_ms": 200.0,
                "p95_lag_ms": 400.0 + i,
                "processed_at": now_epoch - (n - i),
            }
            for i in range(n)
        ]
    )
    write_deltalake(str(root / "pipeline_metrics"), df, mode="append")
