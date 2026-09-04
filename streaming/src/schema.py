from pyspark.sql.types import (
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_time", LongType(), nullable=False),  # epoch millis
        StructField("value", DoubleType(), nullable=False),
        StructField("properties", MapType(StringType(), StringType()), nullable=True),
    ]
)

LOOKUP_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), nullable=False),
        StructField("user_segment", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
    ]
)
