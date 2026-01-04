from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def run_bronze_quality_checks(df: DataFrame) -> dict:
    rows = df.count()
    distinct_ids = df.select("event_id").distinct().count()

    return {
        "rows": rows,
        "distinct_event_id": distinct_ids,
        "duplicate_event_id": rows - distinct_ids,
        "null_event_id": df.filter(col("event_id").isNull()).count(),
        "null_event_ts": df.filter(col("event_ts").isNull()).count(),
        "distinct_event_type": df.select("event_type").distinct().count()
    }