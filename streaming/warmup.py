"""Build-time step: create a SparkSession with the same Delta+Kafka package
coordinates the real job uses, so Ivy resolves and caches every jar into this
Docker layer. Run once during `docker build`, not at container startup.
"""
import sys

sys.path.insert(0, "src")

from main import build_spark  # noqa: E402

if __name__ == "__main__":
    spark = build_spark()
    spark.stop()
    print("warmup complete: delta + kafka packages cached")
