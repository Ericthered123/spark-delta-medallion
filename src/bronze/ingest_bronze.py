from pyspark.sql.functions import current_timestamp, input_file_name
from src.quality.bronze_events import run_bronze_quality_checks
from src.quality.persist import persist_metrics
from src.common.spark_session import get_spark
import uuid

run_id = str(uuid.uuid4())


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

spark.stop()
