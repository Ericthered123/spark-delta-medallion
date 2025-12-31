from pyspark.sql.functions import current_timestamp, input_file_name
from common.spark_session import get_spark

RAW_PATH = "data/raw/"
BRONZE_PATH = "data/delta/bronze_events"

spark = get_spark("bronze_ingestion")

# Read raw CSV files
raw_df = (
    spark.read
    .option("header", "true")
    .csv(RAW_PATH)
)

# Enrich with ingestion metadata
bronze_df = (
    raw_df
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("source_file", input_file_name())
)

# Append-only write to Bronze Delta table
(
    bronze_df
    .write
    .format("delta")
    .mode("append")
    .save(BRONZE_PATH)
)

spark.stop()
