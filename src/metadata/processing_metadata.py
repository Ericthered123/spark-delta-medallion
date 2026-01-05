from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import Row

METADATA_PATH = "data/delta/processing_metadata"


def get_last_processed(
    spark: SparkSession,
    layer: str,
    dataset: str
):
    if not spark._jsparkSession.catalog().tableExists( # type: ignore
        f"delta.`{METADATA_PATH}`"
    ):  # type: ignore
        return None

    df = (
        spark.read.format("delta")
        .load(METADATA_PATH)
        .filter(
            (col("layer") == layer) &
            (col("dataset") == dataset)
        )
        .orderBy(col("run_ts").desc())
        .limit(1)
    )

    rows = df.collect()
    return rows[0]["last_processed"] if rows else None


def update_last_processed(
    spark: SparkSession,
    layer: str,
    dataset: str,
    last_processed: str
):
    row = Row(
        layer=layer,
        dataset=dataset,
        last_processed=last_processed
    )

    df = (
        spark.createDataFrame([row])
        .withColumn("run_ts", current_timestamp())
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .save(METADATA_PATH)
    )