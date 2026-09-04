"""Seeds the static user-lookup Delta table the streaming job enriches events
against. Run once against a running stack (or before the streaming container
starts, since it reads this table at startup).
"""
import os
import random
import sys
from pathlib import Path

# Local (non-Docker) run: streaming/src lives next to this repo's scripts/ dir.
# In-container run: the streaming image already puts src/ on PYTHONPATH.
_local_src = Path(__file__).resolve().parents[1] / "streaming" / "src"
if _local_src.exists():
    sys.path.insert(0, str(_local_src))

from main import build_spark  # noqa: E402
from schema import LOOKUP_SCHEMA  # noqa: E402

NUM_USERS = 500
SEGMENTS = ["high_value", "standard", "at_risk", "new"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "BR", "AU"]


def main() -> None:
    data_root = os.environ.get("DELTA_DATA_ROOT", "/data")

    # Reuses main.build_spark() rather than building a second, separately
    # configured SparkSession: it already sets the Delta extension + catalog
    # configs (spark.sql.extensions / spark.sql.catalog.spark_catalog) that a
    # bare configure_spark_with_delta_pip(builder) alone doesn't set.
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    rng = random.Random(42)
    rows = [
        (f"user_{i}", rng.choice(SEGMENTS), rng.choice(COUNTRIES))
        for i in range(1, NUM_USERS + 1)
    ]
    df = spark.createDataFrame(rows, LOOKUP_SCHEMA)
    df.write.format("delta").mode("overwrite").save(f"{data_root}/lookup/users")

    print(f"seeded {df.count()} users to {data_root}/lookup/users")
    spark.stop()


if __name__ == "__main__":
    main()
