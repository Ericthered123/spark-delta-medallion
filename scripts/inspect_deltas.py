import argparse
from pyspark.sql.functions import col
from src.common.spark_session import get_spark

DELTA_PATHS = {
    "bronze": "data/delta/bronze_events",
    "silver": "data/delta/silver_events",
    "gold":   "data/delta/gold_events",
    "metadata": "data/delta/processing_metadata"
}


QUALITY_TABLES = {
    "bronze": "data/delta/quality_metrics/bronze_events",
    "silver": "data/delta/quality_metrics/silver_events",
    "gold":   "data/delta/quality_metrics/gold_events_daily",
}

def inspect(path: str, name: str, limit: int, stats: bool):
    spark = get_spark(f"inspect-{name}")

    df = spark.read.format("delta").load(path)

    print(f"\n Inspecting: {name}")
    print("=" * 60)

    print("\n Schema")
    df.printSchema()

    print("\n Sample rows")
    df.show(limit, truncate=False)

    if stats:
        print("\n Stats")
        print(f"Rows: {df.count()}")
        for c in df.columns:
            print(f" - {c}: {df.filter(col(c).isNull()).count()} nulls")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect Delta tables")

    parser.add_argument(
        "domain",
        choices=["data", "quality"],
        help="Which domain to inspect"
    )

    parser.add_argument(
        "layer",
        help="bronze | silver | gold | metadata"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20
    )

    parser.add_argument(
        "--stats",
        action="store_true"
    )

    args = parser.parse_args()

    if args.domain == "data":
        path = DELTA_PATHS.get(args.layer)
    else:
        path = QUALITY_TABLES.get(args.layer)

    if not path:
        raise ValueError("Invalid layer")

    inspect(
        path=path,
        name=f"{args.domain.upper()}::{args.layer}",
        limit=args.limit,
        stats=args.stats
    )