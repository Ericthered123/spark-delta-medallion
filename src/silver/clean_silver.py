from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from src.contracts.silver_events import validate_silver
from src.quality.silver_checks import run_silver_quality_checks
from src.quality.persist import persist_metrics
from src.common.spark_session import get_spark
import uuid

run_id = str(uuid.uuid4())


BRONZE_PATH = "data/delta/bronze_events"
SILVER_PATH = "data/delta/silver_events"

spark = get_spark("silver_cleaning")

# 1️ Read bronze (fuente inmutable)
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# 2️ Normalization and data contract
normalized_df = (
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

# 3️ minimum detail (Silver doesnt accept null keys)
normalized_df = normalized_df.filter(col("event_id").isNotNull())

# 4️ Determinist deduplication 
# Rule: stick with the most recent event by event_id
window_spec = (
    Window
    .partitionBy("event_id")
    .orderBy(col("ingestion_ts").desc())
)

silver_df = (
    normalized_df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

#  Quality checks , ideally a try-catch block could be used here

metrics = run_silver_quality_checks(silver_df)

persist_metrics(
    spark=spark,
    run_id=run_id,
    layer="silver",
    dataset="silver_events",
    metric_type="technical",
    metrics=metrics
)

#  Silver contract validation
validate_silver(silver_df)

# 5️ idempotent Upsert
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
