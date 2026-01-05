def test_silver_dedup_keeps_latest(spark):
    from pyspark.sql import Row
    from pyspark.sql.functions import row_number, col
    from pyspark.sql.window import Window

    data = [
        Row(event_id=1, user_id=101, event_type="click",
            event_ts="2024-01-01 10:01:00",
            ingestion_ts="2026-01-01 10:00:00"),
        Row(event_id=1, user_id=101, event_type="click",
            event_ts="2024-01-01 10:01:00",
            ingestion_ts="2026-01-02 10:00:00"),
    ]

    df = spark.createDataFrame(data)

    window = Window.partitionBy("event_id").orderBy(col("ingestion_ts").desc())

    result = (
        df.withColumn("rn", row_number().over(window))
          .filter(col("rn") == 1)
          .drop("rn")
    )

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["ingestion_ts"] == "2026-01-02 10:00:00"