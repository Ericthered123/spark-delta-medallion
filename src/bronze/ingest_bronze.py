from delta import DeltaTable
from pyspark.sql.functions import current_timestamp, input_file_name
from src.quality.bronze_events import run_bronze_quality_checks
from src.quality.persist import persist_metrics
from src.common.spark_session import get_spark
import uuid
import os
from src.metadata.processing_metadata import(get_last_processed, update_last_processed)


run_id = str(uuid.uuid4())


RAW_PATH = "data/raw/"
BRONZE_PATH = "data/delta/bronze_events"

spark = get_spark("bronze_ingestion")
all_files = sorted(os.listdir(RAW_PATH))

# Read last processed file from metadata
last_file = get_last_processed(spark, layer="bronze", dataset="events")

# Avoid re-ingesting already processed files
if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    bronze_table = DeltaTable.forPath(spark, BRONZE_PATH)
    processed_files = [
        row["source_file"].split("/")[-1]
        for row in bronze_table.toDF().select("source_file").distinct().collect()
    ]
else:
    processed_files = []

# only new files
new_files = [f for f in all_files if f not in processed_files and (not last_file or f > last_file)]

if not new_files:
    print("No new files to ingest")
    spark.stop()
    exit(0)
# Read raw CSV files
raw_df = (
    spark.read
    .option("header", "true")
    .csv([f"{RAW_PATH}/{f}" for f in new_files])
)

# Enrich with ingestion metadata
bronze_df = (
    raw_df
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", input_file_name())
)

metrics= run_bronze_quality_checks(bronze_df)

persist_metrics(
    run_id=run_id,
    spark=spark,
    layer="bronze",
    dataset="bronze_events",
    metric_type="technical",
    metrics=metrics
)


# Append-only write to Bronze Delta table
(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .save(BRONZE_PATH)
)

update_last_processed(
    spark,
    layer="bronze",
    dataset="events",
    last_processed=max(new_files)
)

spark.stop()

