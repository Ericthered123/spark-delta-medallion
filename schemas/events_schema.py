from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)

events_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", StringType(), True),      # event time (raw)
    StructField("ingestion_ts", TimestampType(), True)  # system time
])