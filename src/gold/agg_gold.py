from pyspark.sql.functions import col, to_date, count
from src.common.spark_session import get_spark
from src.contracts.gold_events_daily import validate_gold
from src.quality.gold_checks import run_gold_quality_checks
from src.quality.persist import persist_metrics
import uuid


run_id = str(uuid.uuid4())

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

metrics= run_gold_quality_checks(
    gold_df=gold_df,
    silver_df=silver_df
)

technical_metrics = {
    "gold_rows": metrics["gold_rows"],
    "null_event_date": metrics["null_event_date"],
    "null_event_type": metrics["null_event_type"],
    "non_positive_event_count": metrics["non_positive_event_count"],
}

persist_metrics(
    run_id=run_id,
    spark=spark,
    layer="gold",
    dataset="gold_events_daily",
    metric_type="technical",
    metrics=technical_metrics
)

business_metrics = {
    "gold_event_sum": metrics["gold_event_sum"],
    "gold_silver_diff": metrics["gold_silver_diff"],
    "gold_equals_silver": metrics["gold_equals_silver"],
}

persist_metrics(
    run_id=run_id,
    spark=spark,
    layer="gold",
    dataset="gold_events_daily",
    metric_type="business",
    metrics=business_metrics
)



# Gold contract validation
validate_gold(gold_df)

# 3️ Write Gold (recomputable layer)
(
    gold_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_PATH)
)

spark.stop()