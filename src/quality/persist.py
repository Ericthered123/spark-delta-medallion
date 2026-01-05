from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import Row
from uuid import uuid4


QUALITY_PATH = "data/delta/quality_metrics"

def persist_metrics(
    spark: SparkSession,
    layer: str,
    run_id: str,
    dataset: str,
    metric_type: str,
    metrics: dict
    
):
    


    rows = [
        Row(
            run_id=run_id,
            layer=layer,
            dataset=dataset,
            metric_type=metric_type,
            metric_name=k,
            metric_value=int(v)
        )
        for k, v in metrics.items()
    ]

    df = spark.createDataFrame(rows) \
              .withColumn("run_ts", current_timestamp())

    (
        df.write
        .format("delta")
        .mode("append")
        .save(f"{QUALITY_PATH}/{dataset}")
    )
    return metrics