from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
spark.range(3).show()
spark.stop()
