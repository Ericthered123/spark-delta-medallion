from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def validate_silver(df: DataFrame) -> None:
    # 1. No null keys
    if df.filter(col("event_id").isNull()).count() > 0:
        raise ValueError("Silver contract violated: event_id is NULL")

    # 2. Uniqueness
    if df.count() != df.select("event_id").distinct().count():
        raise ValueError("Silver contract violated: duplicate event_id detected")