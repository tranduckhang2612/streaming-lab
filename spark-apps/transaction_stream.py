from __future__ import annotations

import argparse
from typing import Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=False),
        StructField("merchant_category", StringType(), nullable=False),
        StructField("amount_vnd", LongType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("produced_at", TimestampType(), nullable=False),
        StructField("sequence", LongType(), nullable=False),
        StructField("simulated_late", BooleanType(), nullable=False),
    ]
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read transaction events from Kafka with Spark Structured Streaming."
    )
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="raw-events")
    parser.add_argument(
        "--checkpoint-location",
        default="file:///opt/spark/work-dir/checkpoints/transaction-window-v2",
    )
    parser.add_argument("--watermark", default="30 seconds")
    parser.add_argument("--window-duration", default="1 minute")
    parser.add_argument("--trigger-interval", default="5 seconds")
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default="earliest",
    )
    return parser


def read_transactions(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )


def parse_transactions(raw: DataFrame, watermark: str) -> DataFrame:
    decoded = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )
    parsed = decoded.withColumn(
        "transaction",
        F.from_json(F.col("raw_value"), TRANSACTION_SCHEMA),
    ).select(
        "kafka_key",
        "raw_value",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "transaction.*",
    )

    required_fields_present = (
        F.col("transaction_id").isNotNull()
        & F.col("account_id").isNotNull()
        & F.col("amount_vnd").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("produced_at").isNotNull()
    )

    return (
        parsed.withColumn(
            "validation_error",
            F.when(~required_fields_present, F.lit("invalid_json_or_missing_field"))
            .when(F.col("kafka_key") != F.col("account_id"), F.lit("key_mismatch"))
            .when(F.col("currency") != F.lit("VND"), F.lit("unsupported_currency")),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "event_lateness_ms",
            F.unix_millis("produced_at") - F.unix_millis("event_time"),
        )
        .withColumn(
            "processing_latency_ms",
            F.unix_millis("ingested_at") - F.unix_millis("produced_at"),
        )
        .withWatermark("event_time", watermark)
    )


def aggregate_transactions(
    transactions: DataFrame,
    window_duration: str,
) -> DataFrame:
    """Aggregate valid transactions by event-time window and category."""
    valid_transactions = transactions.filter(F.col("validation_error").isNull())

    aggregates = valid_transactions.groupBy(
        F.window(F.col("event_time"), window_duration).alias("event_window"),
        F.col("merchant_category"),
    ).agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount_vnd").alias("total_amount_vnd"),
        F.round(F.avg("amount_vnd"), 2).alias("average_amount_vnd"),
        F.min("amount_vnd").alias("min_amount_vnd"),
        F.max("amount_vnd").alias("max_amount_vnd"),
        F.sum(F.col("simulated_late").cast("long")).alias(
            "simulated_late_count"
        ),
        F.round(F.avg("event_lateness_ms"), 2).alias(
            "average_event_lateness_ms"
        ),
    )

    return aggregates.select(
        F.col("event_window.start").alias("window_start"),
        F.col("event_window.end").alias("window_end"),
        "merchant_category",
        "transaction_count",
        "total_amount_vnd",
        "average_amount_vnd",
        "min_amount_vnd",
        "max_amount_vnd",
        "simulated_late_count",
        "average_event_lateness_ms",
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    spark = (
        SparkSession.builder.appName("transaction-window-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = read_transactions(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )
    transactions = parse_transactions(raw, args.watermark)
    window_results = aggregate_transactions(transactions, args.window_duration)

    query = (
        window_results.writeStream.format("console")
        .queryName("transaction-window-aggregates")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    print(
        "Spark transaction window stream started: "
        f"topic={args.topic}, watermark={args.watermark}, "
        f"window={args.window_duration}, "
        f"trigger={args.trigger_interval}, checkpoint={args.checkpoint_location}",
        flush=True,
    )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
