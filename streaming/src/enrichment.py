"""Validation, deduplication, and lookup-table enrichment for the events stream."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, from_json


def parse_events(raw_df: DataFrame, schema) -> DataFrame:
    return (
        raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", (col("event_time") / 1000).cast("timestamp"))
    )


def validate(df: DataFrame) -> DataFrame:
    """Drop events that fail basic data-quality checks."""
    return df.filter(
        col("event_time").isNotNull()
        & (col("user_id") != "")
        & (col("event_id") != "")
        & (col("value") >= 0)
    )


def deduplicate_streaming(df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """Drop duplicate event_ids seen within the watermark window.

    Streaming-only: withWatermark requires a streaming DataFrame with an
    event-time column, so this is used by the windowed-aggregation query, not
    inside foreachBatch (where batches are static DataFrames and a plain
    dropDuplicates is used instead).
    """
    return df.withWatermark("event_time", watermark).dropDuplicates(["event_id"])


def enrich_events(df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """Left-join each event with the (small, broadcastable) user lookup table."""
    return df.join(broadcast(lookup_df), on="user_id", how="left")
