from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum


def run_gold_quality_checks(
    gold_df: DataFrame,
    silver_df: DataFrame
) -> dict:
    metrics = {}

    # --- Volume ---
    metrics["gold_rows"] = gold_df.count()
    metrics["silver_rows"] = silver_df.count()

    # --- Invariante fuerte ---
    gold_sum = (
        gold_df
        .agg(_sum("event_count").alias("total"))
        .collect()[0]["total"]
    )

    metrics["gold_event_sum"] = gold_sum
    metrics["gold_equals_silver"] = (
        gold_sum == metrics["silver_rows"]
    )

    # --- Nulls ---
    metrics["null_event_date"] = (
        gold_df.filter(col("event_date").isNull()).count()
    )

    metrics["null_event_type"] = (
        gold_df.filter(col("event_type").isNull()).count()
    )

    # --- Métricas inválidas ---
    metrics["non_positive_event_count"] = (
        gold_df.filter(col("event_count") <= 0).count()
    )

    return metrics