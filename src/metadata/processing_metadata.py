from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import Row
from delta.tables import DeltaTable
 

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
    df = (
        spark.createDataFrame([
            Row(
                layer=layer,
                dataset=dataset,
                last_processed=last_processed
            )
        ])
        .withColumn("run_ts", current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, METADATA_PATH):
        table = DeltaTable.forPath(spark, METADATA_PATH)

        (
            table.alias("t")
            .merge(
                df.alias("s"),
                "t.layer = s.layer AND t.dataset = s.dataset"
            )
            .whenMatchedUpdate(set={
                "last_processed": "s.last_processed",
                "run_ts": "s.run_ts"
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").save(METADATA_PATH)