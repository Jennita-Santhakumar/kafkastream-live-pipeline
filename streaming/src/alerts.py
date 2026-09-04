"""Rule-based alerting: flags event_types whose per-batch stats deviate sharply
from their own rolling history (a z-score threshold), plus a hard error-rate rule.

Rolling stats are kept in-process (RollingStats), which is a deliberate
single-node trade-off documented in the README: state resets on job restart
rather than surviving via a fault-tolerant store.
"""
from collections import defaultdict, deque

from pyspark.sql import DataFrame

Z_SCORE_THRESHOLD = 3.0
MIN_HISTORY = 5
HISTORY_LEN = 30


class RollingStats:
    def __init__(self, history_len: int = HISTORY_LEN) -> None:
        self._history: dict[str, deque] = defaultdict(lambda: deque(maxlen=history_len))

    def evaluate(self, event_type: str, value: float) -> tuple[float | None, bool]:
        """Returns (z_score, is_anomalous) for `value` against history, then records it."""
        history = self._history[event_type]
        z_score = None
        is_anomalous = False
        if len(history) >= MIN_HISTORY:
            mean = sum(history) / len(history)
            variance = sum((x - mean) ** 2 for x in history) / len(history)
            stddev = variance**0.5
            if stddev > 1e-9:
                z_score = (value - mean) / stddev
                is_anomalous = abs(z_score) >= Z_SCORE_THRESHOLD
            elif value != mean:
                # Zero-variance history (e.g. a perfectly flat count so far):
                # a real z-score is undefined, but any deviation at all from a
                # constant series is still a genuine anomaly worth flagging.
                is_anomalous = True
        history.append(value)
        return z_score, is_anomalous


def batch_event_type_counts(batch_df: DataFrame) -> list[dict]:
    rows = batch_df.groupBy("event_type").count().collect()
    return [{"event_type": r["event_type"], "count": r["count"]} for r in rows]


def check_alerts(
    validated_df: DataFrame, raw_df: DataFrame, rolling: RollingStats, batch_id: int
) -> list[dict]:
    """Evaluate this micro-batch: volume anomalies (on validated data) and
    error rate (comparing raw parsed count to validated count)."""
    alerts = []
    for row in batch_event_type_counts(validated_df):
        z_score, is_anomalous = rolling.evaluate(row["event_type"], float(row["count"]))
        if is_anomalous:
            deviation = f"{z_score:.2f} std-dev" if z_score is not None else "a zero-variance history"
            alerts.append(
                {
                    "alert_type": "volume_anomaly",
                    "message": (
                        f"event_type={row['event_type']} count={row['count']} in batch "
                        f"{batch_id} deviates {deviation} from its rolling average"
                    ),
                    "severity": "high" if z_score is not None and abs(z_score) >= 5 else "medium",
                    "batch_id": batch_id,
                }
            )

    invalid_rate = _invalid_rate(raw_df, validated_df)
    if invalid_rate is not None and invalid_rate > 0.10:
        alerts.append(
            {
                "alert_type": "high_error_rate",
                "message": f"batch {batch_id} validation-failure rate={invalid_rate:.2%}",
                "severity": "high",
                "batch_id": batch_id,
            }
        )
    return alerts


def _invalid_rate(raw_df: DataFrame, validated_df: DataFrame) -> float | None:
    total = raw_df.count()
    if total == 0:
        return None
    valid = validated_df.count()
    return (total - valid) / total
