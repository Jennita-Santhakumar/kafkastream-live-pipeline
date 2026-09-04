"""End-to-end integration test: real Kafka (via testcontainers) -> real
streaming job (both queries) -> real Delta Lake output on local disk.

Slower than the unit tests (spins a container, runs the job for a bounded
window) so it's kept in its own module and separately markable in CI.
"""
import json
import sys
import time
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.kafka import KafkaContainer

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import main as streaming_main  # noqa: E402
from schema import LOOKUP_SCHEMA  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def kafka_container():
    with KafkaContainer("confluentinc/cp-kafka:7.5.0") as kafka:
        yield kafka


def _produce_events(bootstrap_servers: str, topic: str, n: int) -> None:
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    now_ms = int(time.time() * 1000)
    for i in range(n):
        event = {
            "event_id": f"evt-{i}",
            "user_id": "user_1" if i % 2 == 0 else "user_2",
            "event_type": "purchase",
            "event_time": now_ms,
            "value": 25.0,
            "properties": {"device": "mobile"},
        }
        producer.produce(topic, key=event["user_id"].encode(), value=json.dumps(event).encode())
    producer.flush(10)


def test_streaming_job_writes_events_and_stats_to_delta(kafka_container, tmp_path, monkeypatch):
    bootstrap_servers = kafka_container.get_bootstrap_server()
    topic = "events"

    data_root = tmp_path / "data"
    checkpoint_root = tmp_path / "checkpoints"
    data_root.mkdir()
    checkpoint_root.mkdir()

    monkeypatch.setattr(streaming_main, "DATA_ROOT", str(data_root))
    monkeypatch.setattr(streaming_main, "CHECKPOINT_ROOT", str(checkpoint_root))

    spark = streaming_main.build_spark()
    spark.sparkContext.setLogLevel("WARN")

    lookup_df = spark.createDataFrame(
        [("user_1", "high_value", "US"), ("user_2", "standard", "CA")], LOOKUP_SCHEMA
    )
    lookup_path = str(data_root / "lookup" / "users")
    lookup_df.write.format("delta").mode("overwrite").save(lookup_path)
    lookup_df = spark.read.format("delta").load(lookup_path)

    # Spark's Kafka source fails outright (UnknownTopicOrPartitionException)
    # if the topic doesn't exist yet when the query starts, so create it
    # explicitly rather than relying on auto-create-on-first-produce timing.
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    time.sleep(2)

    # The job's Kafka source uses startingOffsets="latest" (correct for a live
    # job: don't replay history on restart), so the queries must be started -
    # and given a moment to establish that starting offset on their first
    # (empty) micro-batch - *before* any events are produced, or those events
    # land before the offset the query considers "latest" and are silently
    # never seen.
    events_query = streaming_main.start_events_query(
        spark, lookup_df, bootstrap_servers, topic, "10000", "2 seconds"
    )
    stats_query = streaming_main.start_windowed_stats_query(
        spark, bootstrap_servers, topic, "10000", "2 seconds"
    )
    events_query.processAllAvailable()
    stats_query.processAllAvailable()

    _produce_events(bootstrap_servers, topic, n=50)

    try:
        events_query.processAllAvailable()
        stats_query.processAllAvailable()
        time.sleep(1)
    finally:
        events_query.stop()
        stats_query.stop()

    events_df = spark.read.format("delta").load(str(data_root / "events"))
    assert events_df.count() == 50
    row = events_df.filter(events_df.user_id == "user_1").first()
    assert row["user_segment"] == "high_value"

    spark.stop()
