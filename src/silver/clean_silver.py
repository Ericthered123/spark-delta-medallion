from pyspark.sql.functions import col, to_timestamp
from delta.tables import DeltaTable

from src.common.spark_session import get_spark

BRONZE_PATH = "data/delta/bronze_events"
SILVER_PATH = "data/delta/silver_events"

spark = get_spark("silver_cleaning")

# 1️ Read bronze (fuente inmutable)
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# 2️ Normalization and data contract
silver_df = (
    bronze_df
    .withColumn("event_ts", to_timestamp("event_ts"))
    .select(
        col("event_id").cast("long").alias("event_id"),
        col("user_id").cast("long").alias("user_id"),
        col("event_type").alias("event_type"),
        col("event_ts"),
        col("ingestion_ts")
    )
)

# 3️ Upsert idempotent
if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    (
        silver_table.alias("t")
        .merge(
            silver_df.alias("s"),
            "t.event_id = s.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(SILVER_PATH)
    )

spark.stop()
