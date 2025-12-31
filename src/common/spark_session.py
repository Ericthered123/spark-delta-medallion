from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession configured with Delta Lake support.
    Centralizing SparkSession creation ensures consistent configuration
    across all pipeline layers (Bronze, Silver, Gold).
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )

    return spark