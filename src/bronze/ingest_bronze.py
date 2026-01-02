from pyspark.sql.functions import current_timestamp, input_file_name
from src.common.spark_session import get_spark
from schemas.events_schema import events_schema

RAW_PATH = "data/raw/"
BRONZE_PATH = "data/delta/bronze_events"

spark = get_spark("bronze_ingestion")

raw_df = (
    spark.read
    .schema(events_schema)
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("badRecordsPath", "data/bad_records/bronze_events")
    .csv(RAW_PATH)
)

if raw_df.rdd.isEmpty():
    print("⚠️ No new data to ingest.")
    spark.stop()
    exit(0)

bronze_df = (
    raw_df
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", input_file_name())
)

(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .save(BRONZE_PATH)
)

spark.stop()
