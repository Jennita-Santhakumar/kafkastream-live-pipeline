from fastapi import APIRouter, Query

from app.core.delta_reader import read_alerts
from app.schemas.responses import AlertOut

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("", response_model=list[AlertOut])
def list_alerts(limit: int = Query(50, ge=1, le=1000)) -> list[dict]:
    df = read_alerts()
    if df.empty:
        return []
    df = df.sort_values("created_at", ascending=False).head(limit)
    return df.to_dict(orient="records")
