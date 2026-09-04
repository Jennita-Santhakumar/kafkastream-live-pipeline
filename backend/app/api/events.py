from fastapi import APIRouter, Query

from app.core.delta_reader import read_table
from app.schemas.responses import EventOut

router = APIRouter(prefix="/api/events", tags=["events"])


@router.get("/recent", response_model=list[EventOut])
def recent_events(limit: int = Query(50, ge=1, le=1000)) -> list[dict]:
    df = read_table("events")
    if df.empty:
        return []
    df = df.sort_values("event_time", ascending=False).head(limit)
    return df.to_dict(orient="records")
