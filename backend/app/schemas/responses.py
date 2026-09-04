from datetime import datetime

from pydantic import BaseModel


class EventOut(BaseModel):
    event_id: str
    user_id: str
    event_type: str
    event_time: datetime
    value: float
    user_segment: str | None = None
    country: str | None = None


class WindowedStatOut(BaseModel):
    window_start: datetime
    window_end: datetime
    event_type: str
    event_count: int
    avg_value: float
    max_value: float


class AlertOut(BaseModel):
    alert_type: str
    message: str
    severity: str
    batch_id: int
    created_at: datetime


class MetricPointOut(BaseModel):
    batch_id: int
    event_count: int
    events_per_sec: float
    avg_lag_ms: float
    p95_lag_ms: float
    processed_at: datetime


class MetricsSummaryOut(BaseModel):
    total_events: int
    current_events_per_sec: float
    current_p95_lag_ms: float
    alerts_last_hour: int
    latest_batch_id: int | None
