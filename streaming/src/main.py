"""Kafka -> validate/dedup/enrich -> Delta Lake structured streaming job.

Runs two independent streaming queries against the same Kafka topic:

1. A foreachBatch query that writes validated+enriched raw events to Delta,
   plus per-batch throughput/lag metrics and rule-based alerts.
2. A native stateful windowed-aggregation query (real watermark + dropDuplicates,
   outputMode="append") that finalizes one row per (window, event_type) once the
   watermark passes, written straight to Delta via the native streaming sink.

Two queries reading the same topic is the standard Structured Streaming pattern
for "I need both raw fan-out and a stateful aggregate" — each gets its own
Kafka consumer and checkpoint, so they don't interfere with each other.
"""
import logging
import os
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, max as spark_max, window

from alerts import RollingStats, check_alerts
from enrichment import deduplicate_streaming, enrich_events, parse_events, validate
from metrics import compute_batch_metrics
from schema import EVENT_SCHEMA

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("streaming")

DATA_ROOT = os.environ.get("DELTA_DATA_ROOT", "/data")
CHECKPOINT_ROOT = os.environ.get("SPARK_CHECKPOINT_ROOT", "/checkpoints")


SPARK_VERSION = os.environ.get("SPARK_VERSION", "3.5.1")
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("KafkaStreamingPipeline")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTITIONS", "4"))
    )
    # extra_packages adds the Kafka connector alongside Delta's own package;
    # both resolve via the Ivy cache baked into the image at build time (see
    # streaming/Dockerfile) so container startup needs no network access.
    return configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PACKAGE]).getOrCreate()


def load_lookup(spark: SparkSession):
    # Delta tables carry their own schema in the transaction log and reject a
    # user-specified one on read (UnsupportedOperationException) - the schema
    # was already enforced when scripts/seed_lookup.py wrote this table.
    path = f"{DATA_ROOT}/lookup/users"
    return spark.read.format("delta").load(path)


def read_kafka(spark: SparkSession, bootstrap_servers: str, topic: str, max_offsets: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )


def make_batch_writer(spark, lookup_df, rolling_stats: RollingStats):
    def write_batch(raw_batch_df, batch_id: int) -> None:
        batch_start = time.time()
        raw_batch_df.persist()
        parsed_df = None
        enriched_df = None
        try:
            parsed_df = parse_events(raw_batch_df, EVENT_SCHEMA)
            parsed_df.persist()

            validated_df = validate(parsed_df)
            deduped_df = validated_df.dropDuplicates(["event_id"])
            enriched_df = enrich_events(deduped_df, lookup_df)
            enriched_df.persist()

            enriched_df.write.format("delta").mode("append").save(f"{DATA_ROOT}/events")

            batch_metrics = compute_batch_metrics(enriched_df, batch_id, batch_start)
            spark.createDataFrame([batch_metrics]).write.format("delta").mode("append").save(
                f"{DATA_ROOT}/pipeline_metrics"
            )

            batch_alerts = check_alerts(deduped_df, parsed_df, rolling_stats, batch_id)
            if batch_alerts:
                for a in batch_alerts:
                    a["created_at"] = batch_start
                spark.createDataFrame(batch_alerts).write.format("delta").mode("append").save(
                    f"{DATA_ROOT}/alerts"
                )
                logger.warning("batch %d raised %d alert(s)", batch_id, len(batch_alerts))

            logger.info(
                "batch %d: %d events, %.1f events/sec, p95 lag %.0fms",
                batch_id,
                batch_metrics["event_count"],
                batch_metrics["events_per_sec"],
                batch_metrics["p95_lag_ms"],
            )
        finally:
            raw_batch_df.unpersist()
            if parsed_df is not None:
                parsed_df.unpersist()
            if enriched_df is not None:
                enriched_df.unpersist()

    return write_batch


def start_events_query(spark, lookup_df, bootstrap_servers, topic, max_offsets, trigger_interval):
    kafka_df = read_kafka(spark, bootstrap_servers, topic, max_offsets)
    rolling_stats = RollingStats()
    return (
        kafka_df.writeStream.foreachBatch(make_batch_writer(spark, lookup_df, rolling_stats))
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/events")
        .trigger(processingTime=trigger_interval)
        .start()
    )


def start_windowed_stats_query(spark, bootstrap_servers, topic, max_offsets, trigger_interval):
    kafka_df = read_kafka(spark, bootstrap_servers, topic, max_offsets)
    parsed = parse_events(kafka_df, EVENT_SCHEMA)
    validated = validate(parsed)
    # A short watermark keeps this demo-scale stack's first finalized window
    # only ~1-2 minutes out instead of the 10+ minutes a production-realistic
    # watermark would impose (append-mode output only fires once the
    # watermark passes a window's end) - see the Design Decisions section in
    # the README.
    deduped = deduplicate_streaming(validated, watermark="20 seconds")

    windowed = (
        deduped.groupBy(window("event_time", "1 minute", "30 seconds"), "event_type")
        .agg(
            count("*").alias("event_count"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
        )
        .selectExpr(
            "window.start as window_start",
            "window.end as window_end",
            "event_type",
            "event_count",
            "avg_value",
            "max_value",
        )
    )

    return (
        windowed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/event_stats")
        .trigger(processingTime=trigger_interval)
        .start(f"{DATA_ROOT}/event_stats")
    )


def run() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "events")
    max_offsets = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "10000")
    trigger_interval = os.environ.get("TRIGGER_INTERVAL", "5 seconds")

    lookup_df = load_lookup(spark)

    events_query = start_events_query(
        spark, lookup_df, bootstrap_servers, topic, max_offsets, trigger_interval
    )
    stats_query = start_windowed_stats_query(
        spark, bootstrap_servers, topic, max_offsets, trigger_interval
    )

    logger.info("streaming queries started: events=%s stats=%s", events_query.id, stats_query.id)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
