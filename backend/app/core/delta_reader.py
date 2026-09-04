"""Read-only access to the Delta tables the streaming job writes.

Uses the `deltalake` package (delta-rs bindings) rather than a Spark session:
the API only needs to read small, already-committed Delta tables, and
spinning up a JVM per request (or even a shared one) would add real latency
and memory overhead for no benefit. This is the one deliberate divergence
between the streaming and serving halves of the pipeline's tech stack.
"""
import logging
import os

import pandas as pd
from deltalake import DeltaTable

from app.core.config import get_settings

logger = logging.getLogger("delta_reader")


def _table_path(name: str) -> str:
    return f"{get_settings().delta_data_root}/{name}"


def read_table(name: str) -> pd.DataFrame:
    """Read a Delta table to a pandas DataFrame. Returns an empty DataFrame if
    the table hasn't been created yet (streaming job hasn't written a first
    batch), which is a normal, expected state right after stack startup.

    Checked with a plain directory check *before* touching `DeltaTable`: the
    API's data mount is read-only, and delta-rs's table-open path tries to
    create the directory when it's missing even for a read, which raises an
    uncatchable `pyo3_runtime.PanicException` (not `Exception`) against a
    read-only filesystem rather than a normal "not found" error.
    """
    path = _table_path(name)
    if not os.path.isdir(os.path.join(path, "_delta_log")):
        logger.info("delta table %s not created yet", name)
        return pd.DataFrame()
    try:
        dt = DeltaTable(path)
        return dt.to_pandas()
    except Exception:
        logger.exception("failed reading delta table %s", name)
        return pd.DataFrame()


def read_alerts() -> pd.DataFrame:
    """`alerts` with `created_at` (epoch-seconds float, see streaming/src/main.py)
    converted to a real pandas timestamp."""
    df = read_table("alerts")
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], unit="s")
    return df


def read_pipeline_metrics() -> pd.DataFrame:
    """`pipeline_metrics` with `processed_at` (epoch-seconds float) converted
    to a real pandas timestamp, sorted ascending."""
    df = read_table("pipeline_metrics")
    if df.empty:
        return df
    df["processed_at"] = pd.to_datetime(df["processed_at"], unit="s")
    return df.sort_values("processed_at")
