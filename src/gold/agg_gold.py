from pyspark.sql.functions import col, to_date, count
from src.common.spark_session import get_spark

SILVER_PATH = "data/delta/silver_events"
GOLD_PATH = "data/delta/gold_events_daily"

spark = get_spark("gold_aggregations")

# 1️ Read Silver (trusted source)
silver_df = spark.read.format("delta").load(SILVER_PATH)

# 2️ Business aggregation
gold_df = (
    silver_df
    .withColumn("event_date", to_date(col("event_ts")))
    .groupBy("event_date", "event_type")
    .agg(
        count("*").alias("event_count")
    )
)

# 3️ Write Gold (recomputable layer)
(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)

spark.stop()