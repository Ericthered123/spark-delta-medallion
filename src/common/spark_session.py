
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession configured with Delta Lake support.
    Centralizing SparkSession creation ensures consistent configuration
    across all pipeline layers (Bronze, Silver, Gold).
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark