from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct,
    max as _max, current_timestamp
)

ALLOWED_EVENT_TYPES = {"click", "view"}


def run_silver_quality_checks(df: DataFrame) -> dict:
    metrics = {}

    # --- Volumen ---
    metrics["row_count"] = df.count()
    metrics["distinct_event_id"] = df.select("event_id").distinct().count()

    # --- Nulls (observabilidad, no bloqueo) ---
    metrics["null_event_id"] = df.filter(col("event_id").isNull()).count()
    metrics["null_event_ts"] = df.filter(col("event_ts").isNull()).count()

    # --- Duplicados lógicos (post-dedup) ---
    metrics["duplicate_event_id"] = (
        df.groupBy("event_id")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    # --- Event types inválidos ---
    metrics["invalid_event_type"] = (
        df.filter(~col("event_type").isin(ALLOWED_EVENT_TYPES))
        .count()
    )

    # --- Outliers: eventos por usuario ---
    metrics["max_events_per_user"] = (
        df.groupBy("user_id")
        .agg(count("*").alias("events"))
        .agg(_max("events").alias("max"))
        .collect()[0]["max"]
    )

    # --- Future events ---
    metrics["future_event_ts"] = (
        df.filter(col("event_ts") > current_timestamp())
        .count()
    )

    return metrics