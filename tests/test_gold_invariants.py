def test_gold_equals_silver(spark):
    from pyspark.sql import Row
    from pyspark.sql.functions import count, to_date, col

    silver = spark.createDataFrame([
        Row(event_id=1, event_type="click", event_ts="2024-01-01 10:01:00"),
        Row(event_id=2, event_type="view",  event_ts="2024-01-01 10:02:00"),
        Row(event_id=3, event_type="click", event_ts="2024-01-01 10:03:00"),
    ])

    gold = (
        silver
        .withColumn("event_date", to_date(col("event_ts")))
        .groupBy("event_date", "event_type")
        .agg(count("*").alias("event_count"))
    )

    gold_sum = gold.agg({"event_count": "sum"}).collect()[0][0]

    assert gold_sum == silver.count()