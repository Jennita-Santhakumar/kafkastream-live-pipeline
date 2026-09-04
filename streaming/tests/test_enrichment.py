import json
from datetime import datetime, timezone

from enrichment import enrich_events, parse_events, validate
from schema import EVENT_SCHEMA


def _kafka_row(spark, event: dict):
    return spark.createDataFrame([(json.dumps(event),)], ["value"])


def _sample_event(**overrides) -> dict:
    event = {
        "event_id": "e1",
        "user_id": "user_1",
        "event_type": "purchase",
        "event_time": int(datetime.now(timezone.utc).timestamp() * 1000),
        "value": 42.0,
        "properties": {"device": "mobile"},
    }
    event.update(overrides)
    return event


def test_parse_events_extracts_fields(spark):
    raw = _kafka_row(spark, _sample_event())
    parsed = parse_events(raw, EVENT_SCHEMA)
    row = parsed.collect()[0]
    assert row["event_id"] == "e1"
    assert row["user_id"] == "user_1"
    assert row["value"] == 42.0


def test_validate_drops_negative_values(spark):
    raw = spark.createDataFrame(
        [
            _sample_event(event_id="good", value=10.0),
            _sample_event(event_id="bad", value=-5.0),
        ]
    )
    result = validate(raw)
    ids = {r["event_id"] for r in result.collect()}
    assert ids == {"good"}


def test_validate_drops_empty_user_id(spark):
    raw = spark.createDataFrame(
        [
            _sample_event(event_id="good"),
            _sample_event(event_id="bad", user_id=""),
        ]
    )
    result = validate(raw)
    ids = {r["event_id"] for r in result.collect()}
    assert ids == {"good"}


def test_enrich_events_joins_lookup(spark):
    events = spark.createDataFrame([_sample_event(user_id="user_1")])
    lookup = spark.createDataFrame(
        [("user_1", "high_value", "US")], ["user_id", "user_segment", "country"]
    )
    enriched = enrich_events(events, lookup)
    row = enriched.collect()[0]
    assert row["user_segment"] == "high_value"
    assert row["country"] == "US"


def test_enrich_events_left_join_keeps_unmatched_users(spark):
    events = spark.createDataFrame([_sample_event(user_id="unknown_user")])
    lookup = spark.createDataFrame(
        [("user_1", "high_value", "US")], ["user_id", "user_segment", "country"]
    )
    enriched = enrich_events(events, lookup)
    row = enriched.collect()[0]
    assert row["user_segment"] is None
