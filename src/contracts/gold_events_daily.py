from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def validate_gold(df: DataFrame) -> None:
    if df.filter(col("event_count") <= 0).count() > 0:
        raise ValueError("Gold contract violated: event_count <= 0")

    if df.filter(
        col("event_date").isNull() | col("event_type").isNull()
    ).count() > 0:
        raise ValueError("Gold contract violated: null dimensions")