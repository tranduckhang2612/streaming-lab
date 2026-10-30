from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import signal
import sys
from typing import TYPE_CHECKING, Any, Mapping, Sequence

DEFAULT_CHECKPOINT_ROOT = "file:///opt/spark/work-dir/checkpoints/transaction-window-v3"

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession  # type: ignore
    from pyspark.sql.streaming.listener import StreamingQueryListener  # type: ignore
    from pyspark.sql.types import StructType  # type: ignore
else:
    try:
        from pyspark.sql import DataFrame, SparkSession
    except ImportError:
        DataFrame = object
        SparkSession = object
    try:
        from pyspark.sql.streaming.listener import StreamingQueryListener
    except ImportError:
        try:
            from pyspark.sql.streaming import StreamingQueryListener
        except ImportError:
            StreamingQueryListener = object

try:
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.types import (  # type: ignore
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
except ImportError:
    F = object()  # type: ignore
    TRANSACTION_SCHEMA = None  # type: ignore


def parse_duration_to_seconds(duration: str) -> float:
    match = re.match(r"^\s*([\d.]+)\s*([a-zA-Z]+)?\s*$", duration.strip())
    if not match:
        return 0.0
    val = float(match.group(1))
    unit = (match.group(2) or "seconds").lower()
    if unit in ("s", "sec", "second", "seconds"):
        return val
    if unit in ("m", "min", "minute", "minutes"):
        return val * 60.0
    if unit in ("h", "hr", "hour", "hours"):
        return val * 3600.0
    if unit in ("d", "day", "days"):
        return val * 86400.0
    if unit in ("ms", "milli", "millisecond", "milliseconds"):
        return val / 1000.0
    return val


def sql_literal(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            formatted = value.replace(tzinfo=timezone.utc).isoformat()
        else:
            formatted = value.astimezone(timezone.utc).isoformat()
        return f"'{formatted}'"
    escaped = str(value).replace("\x00", "").replace("'", "''")
    return f"'{escaped}'"


def _sum_if_complete(values: Sequence[int | None]) -> int | None:
    if not values or any(value is None for value in values):
        return None
    return sum(value for value in values if value is not None)


def _offset_difference(end: int | None, start: int | None) -> int | None:
    if end is None or start is None or end < start:
        return None
    return end - start


def extract_batch_metrics(
    progress: dict[str, Any],
    default_topic: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    spark_run_id = progress.get("runId")
    query_id = progress.get("id")
    batch_id = progress.get("batchId", 0)
    observed_at = progress.get("timestamp")

    observed_metrics = progress.get("observedMetrics") or {}
    latency_metrics = observed_metrics.get("transaction-latency") or {}
    latency_avg_ms = latency_metrics.get("latency_avg_ms")
    latency_p95_ms = latency_metrics.get("latency_p95_ms")
    latency_max_ms = latency_metrics.get("latency_max_ms")

    num_input_rows = progress.get("numInputRows")
    input_rows_per_second = progress.get("inputRowsPerSecond")
    processed_rows_per_second = progress.get("processedRowsPerSecond")

    duration_ms = progress.get("durationMs") or {}
    trigger_execution_ms = duration_ms.get("triggerExecution")
    add_batch_ms = duration_ms.get("addBatch")
    get_batch_ms = duration_ms.get("getBatch")
    wal_commit_ms = duration_ms.get("walCommit")
    commit_offsets_ms = duration_ms.get("commitOffsets")
    batch_duration_ms = (
        trigger_execution_ms
        if trigger_execution_ms is not None
        else progress.get("batchDuration")
    )

    reported_state_operators = progress.get("stateOperators")
    state_operators = reported_state_operators or []
    state_rows_total = _sum_if_complete(
        [op.get("numRowsTotal") for op in state_operators]
    )
    state_memory_bytes = _sum_if_complete(
        [op.get("memoryUsedBytes") for op in state_operators]
    )
    rows_dropped_by_watermark = _sum_if_complete(
        [op.get("numRowsDroppedByWatermark") for op in state_operators]
    )
    state_commit_ms = _sum_if_complete(
        [op.get("commitTimeMs") for op in state_operators]
    )
    if reported_state_operators == []:
        # An explicitly stateless query has no state commits to include.
        state_commit_ms = 0
    checkpoint_commit_ms = _sum_if_complete(
        [wal_commit_ms, commit_offsets_ms, state_commit_ms]
    )

    event_time = progress.get("eventTime") or {}
    watermark_at = event_time.get("watermark")
    max_event_time = event_time.get("max")

    sources = progress.get("sources") or []
    partition_rows: list[dict[str, Any]] = []
    partition_lags: list[int | None] = []

    topic = default_topic
    if sources:
        src = sources[0]
        offset_maps = [
            offsets if isinstance(offsets, dict) else {}
            for offsets in (
                src.get("startOffset"), src.get("endOffset"), src.get("latestOffset")
            )
        ]
        topics_in_src = sorted(set().union(*(offsets.keys() for offsets in offset_maps)))
        if len(topics_in_src) == 1:
            topic = topics_in_src[0]
        for source_topic in topics_in_src:
            topic_offsets = [
                offsets[source_topic]
                if isinstance(offsets.get(source_topic), dict)
                else {}
                for offsets in offset_maps
            ]
            partitions = sorted(
                set().union(*(offsets.keys() for offsets in topic_offsets)), key=int,
            )
            for p_str in partitions:
                p_id = int(p_str)
                s_off, e_off, l_off = (
                    int(offsets[p_str]) if offsets.get(p_str) is not None else None
                    for offsets in topic_offsets
                )
                rows_read = _offset_difference(e_off, s_off)
                lag = _offset_difference(l_off, e_off)
                partition_lags.append(lag)
                partition_rows.append(
                    {
                        "spark_run_id": spark_run_id,
                        "batch_id": batch_id,
                        "observed_at": observed_at,
                        "topic": source_topic,
                        "partition_id": p_id,
                        "start_offset": s_off,
                        "end_offset": e_off,
                        "latest_offset": l_off,
                        "rows_read": rows_read,
                        "consumer_lag": lag,
                    }
                )

    partition_count = len(partition_rows) or None
    total_consumer_lag = _sum_if_complete(partition_lags)
    max_partition_lag = (
        max(lag for lag in partition_lags if lag is not None)
        if total_consumer_lag is not None
        else None
    )

    batch_metric = {
        "spark_run_id": spark_run_id,
        "query_id": query_id,
        "partition_count": partition_count,
        "batch_id": batch_id,
        "observed_at": observed_at,
        "topic": topic,
        "num_input_rows": num_input_rows,
        "input_rows_per_second": input_rows_per_second,
        "processed_rows_per_second": processed_rows_per_second,
        "batch_duration_ms": batch_duration_ms,
        "trigger_execution_ms": trigger_execution_ms,
        "add_batch_ms": add_batch_ms,
        "get_batch_ms": get_batch_ms,
        "wal_commit_ms": wal_commit_ms,
        "commit_offsets_ms": commit_offsets_ms,
        "checkpoint_commit_ms": checkpoint_commit_ms,
        "watermark_at": watermark_at,
        "max_event_time": max_event_time,
        "state_rows_total": state_rows_total,
        "state_memory_bytes": state_memory_bytes,
        "rows_dropped_by_watermark": rows_dropped_by_watermark,
        "latency_avg_ms": latency_avg_ms,
        "latency_p95_ms": latency_p95_ms,
        "latency_max_ms": latency_max_ms,
        "total_consumer_lag": total_consumer_lag,
        "max_partition_lag": max_partition_lag,
        "progress_json": json.dumps(progress, ensure_ascii=False),
    }

    return batch_metric, partition_rows


class PostgresWriter:
    def __init__(
        self,
        spark: SparkSession,
        host: str = "postgres",
        port: int = 5432,
        database: str = "streaming_db",
        user: str = "streaming_user",
        password: str = "streaming_password",
    ) -> None:
        self.spark = spark
        self.url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.user = user
        self.password = password
    def _get_connection(self) -> Any:
        props = self.spark._jvm.java.util.Properties()
        props.setProperty("user", self.user)
        props.setProperty("password", self.password)

        # 1. Try directly instantiating Driver via Py4J
        try:
            driver = self.spark._jvm.org.postgresql.Driver()
            conn = driver.connect(self.url, props)
            if conn is not None:
                return conn
        except Exception:
            pass

        # 2. Try loading driver class via context class loader
        try:
            cl = self.spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
            driver_class = cl.loadClass("org.postgresql.Driver")
            driver = driver_class.newInstance()
            conn = driver.connect(self.url, props)
            if conn is not None:
                return conn
        except Exception:
            pass

        # 3. Fallback to DriverManager
        try:
            self.spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
        except Exception:
            pass
        return self.spark._jvm.java.sql.DriverManager.getConnection(
            self.url, props
        )

    def execute(self, sql: str) -> None:
        conn = self._get_connection()
        try:
            stmt = conn.createStatement()
            try:
                stmt.execute(sql)
            finally:
                stmt.close()
        finally:
            conn.close()

    def record_pipeline_start(
        self,
        spark_run_id: str,
        query_id: str,
        topic: str,
        checkpoint_location: str,
        watermark_delay_seconds: float,
        window_duration_seconds: float,
        trigger_interval_seconds: float,
        configuration: Mapping[str, object],
        partition_count: int | None = None,
    ) -> None:
        config_json = json.dumps(configuration, ensure_ascii=False)
        sql = f"""
INSERT INTO streaming.pipeline_runs (
    spark_run_id,
    query_id,
    topic,
    partition_count,
    checkpoint_location,
    watermark_delay_seconds,
    window_duration_seconds,
    trigger_interval_seconds,
    started_at,
    status,
    configuration
) VALUES (
    {sql_literal(spark_run_id)}::UUID,
    {sql_literal(query_id)}::UUID,
    {sql_literal(topic)},
    {sql_literal(partition_count)},
    {sql_literal(checkpoint_location)},
    {sql_literal(watermark_delay_seconds)},
    {sql_literal(window_duration_seconds)},
    {sql_literal(trigger_interval_seconds)},
    CURRENT_TIMESTAMP,
    'running',
    {sql_literal(config_json)}::JSONB
)
ON CONFLICT (spark_run_id) DO UPDATE SET
    query_id = EXCLUDED.query_id,
    topic = EXCLUDED.topic,
    partition_count = COALESCE(EXCLUDED.partition_count, streaming.pipeline_runs.partition_count),
    checkpoint_location = EXCLUDED.checkpoint_location,
    watermark_delay_seconds = EXCLUDED.watermark_delay_seconds,
    window_duration_seconds = EXCLUDED.window_duration_seconds,
    trigger_interval_seconds = EXCLUDED.trigger_interval_seconds,
    status = 'running',
    configuration = EXCLUDED.configuration,
    finished_at = NULL;
"""
        self.execute(sql)

    def record_pipeline_finish(
        self,
        spark_run_id: str,
        status: str = "stopped",
    ) -> None:
        sql = f"""
UPDATE streaming.pipeline_runs SET
    finished_at = CURRENT_TIMESTAMP,
    status = {sql_literal(status)}
WHERE spark_run_id = {sql_literal(spark_run_id)}::UUID;
"""
        self.execute(sql)

    def save_window_results(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return

        values_list: list[str] = []
        for r in rows:
            values_list.append(
                f"("
                f"{sql_literal(r['topic'])}, "
                f"{sql_literal(r['window_start'])}, "
                f"{sql_literal(r['window_end'])}, "
                f"{sql_literal(r['merchant_category'])}, "
                f"{sql_literal(r['transaction_count'])}, "
                f"{sql_literal(r['total_amount_vnd'])}, "
                f"{sql_literal(r['average_amount_vnd'])}, "
                f"{sql_literal(r['min_amount_vnd'])}, "
                f"{sql_literal(r['max_amount_vnd'])}, "
                f"{sql_literal(r['simulated_late_count'])}, "
                f"{sql_literal(r['average_event_lateness_ms'])}, "
                f"CURRENT_TIMESTAMP"
                f")"
            )

        values_sql = ",\n".join(values_list)
        sql = f"""
INSERT INTO streaming.transaction_window_results (
    topic,
    window_start,
    window_end,
    merchant_category,
    transaction_count,
    total_amount_vnd,
    average_amount_vnd,
    min_amount_vnd,
    max_amount_vnd,
    simulated_late_count,
    average_event_lateness_ms,
    updated_at
) VALUES
{values_sql}
ON CONFLICT (topic, window_start, window_end, merchant_category) DO UPDATE SET
    transaction_count = EXCLUDED.transaction_count,
    total_amount_vnd = EXCLUDED.total_amount_vnd,
    average_amount_vnd = EXCLUDED.average_amount_vnd,
    min_amount_vnd = EXCLUDED.min_amount_vnd,
    max_amount_vnd = EXCLUDED.max_amount_vnd,
    simulated_late_count = EXCLUDED.simulated_late_count,
    average_event_lateness_ms = EXCLUDED.average_event_lateness_ms,
    updated_at = CURRENT_TIMESTAMP;
"""
        self.execute(sql)

    def save_batch_metrics(
        self,
        batch_metric: dict[str, Any],
        partition_rows: list[dict[str, Any]],
    ) -> None:
        bm = batch_metric
        sql_ensure_run = f"""
INSERT INTO streaming.pipeline_runs (
    spark_run_id, query_id, topic, partition_count, checkpoint_location,
    watermark_delay_seconds, window_duration_seconds, trigger_interval_seconds,
    started_at, status
) VALUES (
    {sql_literal(bm['spark_run_id'])}::UUID,
    {sql_literal(bm['query_id'])}::UUID,
    {sql_literal(bm['topic'])},
    {sql_literal(bm['partition_count'])},
    'unknown',
    0, 0, 0,
    CURRENT_TIMESTAMP,
    'running'
) ON CONFLICT (spark_run_id) DO UPDATE SET
    query_id = EXCLUDED.query_id,
    topic = EXCLUDED.topic,
    partition_count = COALESCE(EXCLUDED.partition_count, streaming.pipeline_runs.partition_count);
"""

        sql_batch = f"""
INSERT INTO streaming.pipeline_batch_metrics (
    spark_run_id, batch_id, observed_at, topic, num_input_rows,
    input_rows_per_second, processed_rows_per_second, batch_duration_ms,
    trigger_execution_ms, add_batch_ms, get_batch_ms, wal_commit_ms,
    commit_offsets_ms, checkpoint_commit_ms, watermark_at, max_event_time,
    state_rows_total, state_memory_bytes, rows_dropped_by_watermark,
    latency_avg_ms, latency_p95_ms, latency_max_ms,
    total_consumer_lag, max_partition_lag, progress_json
) VALUES (
    {sql_literal(bm['spark_run_id'])}::UUID,
    {sql_literal(bm['batch_id'])},
    {sql_literal(bm['observed_at'])}::TIMESTAMPTZ,
    {sql_literal(bm['topic'])},
    {sql_literal(bm['num_input_rows'])},
    {sql_literal(bm['input_rows_per_second'])},
    {sql_literal(bm['processed_rows_per_second'])},
    {sql_literal(bm['batch_duration_ms'])},
    {sql_literal(bm['trigger_execution_ms'])},
    {sql_literal(bm['add_batch_ms'])},
    {sql_literal(bm['get_batch_ms'])},
    {sql_literal(bm['wal_commit_ms'])},
    {sql_literal(bm['commit_offsets_ms'])},
    {sql_literal(bm['checkpoint_commit_ms'])},
    {sql_literal(bm['watermark_at'])}::TIMESTAMPTZ,
    {sql_literal(bm['max_event_time'])}::TIMESTAMPTZ,
    {sql_literal(bm['state_rows_total'])},
    {sql_literal(bm['state_memory_bytes'])},
    {sql_literal(bm['rows_dropped_by_watermark'])},
    {sql_literal(bm['latency_avg_ms'])},
    {sql_literal(bm['latency_p95_ms'])},
    {sql_literal(bm['latency_max_ms'])},
    {sql_literal(bm['total_consumer_lag'])},
    {sql_literal(bm['max_partition_lag'])},
    {sql_literal(bm['progress_json'])}::JSONB
)
ON CONFLICT (spark_run_id, batch_id) DO UPDATE SET
    num_input_rows = EXCLUDED.num_input_rows,
    input_rows_per_second = EXCLUDED.input_rows_per_second,
    processed_rows_per_second = EXCLUDED.processed_rows_per_second,
    batch_duration_ms = EXCLUDED.batch_duration_ms,
    trigger_execution_ms = EXCLUDED.trigger_execution_ms,
    add_batch_ms = EXCLUDED.add_batch_ms,
    get_batch_ms = EXCLUDED.get_batch_ms,
    wal_commit_ms = EXCLUDED.wal_commit_ms,
    commit_offsets_ms = EXCLUDED.commit_offsets_ms,
    checkpoint_commit_ms = EXCLUDED.checkpoint_commit_ms,
    watermark_at = EXCLUDED.watermark_at,
    max_event_time = EXCLUDED.max_event_time,
    state_rows_total = EXCLUDED.state_rows_total,
    state_memory_bytes = EXCLUDED.state_memory_bytes,
    rows_dropped_by_watermark = EXCLUDED.rows_dropped_by_watermark,
    latency_avg_ms = EXCLUDED.latency_avg_ms,
    latency_p95_ms = EXCLUDED.latency_p95_ms,
    latency_max_ms = EXCLUDED.latency_max_ms,
    total_consumer_lag = EXCLUDED.total_consumer_lag,
    max_partition_lag = EXCLUDED.max_partition_lag,
    progress_json = EXCLUDED.progress_json;
"""

        sql_parts = ""
        if partition_rows:
            part_values = []
            for pr in partition_rows:
                part_values.append(
                    f"("
                    f"{sql_literal(pr['spark_run_id'])}::UUID, "
                    f"{sql_literal(pr['batch_id'])}, "
                    f"{sql_literal(pr['observed_at'])}::TIMESTAMPTZ, "
                    f"{sql_literal(pr['topic'])}, "
                    f"{sql_literal(pr['partition_id'])}, "
                    f"{sql_literal(pr['start_offset'])}, "
                    f"{sql_literal(pr['end_offset'])}, "
                    f"{sql_literal(pr['latest_offset'])}, "
                    f"{sql_literal(pr['rows_read'])}, "
                    f"{sql_literal(pr['consumer_lag'])}"
                    f")"
                )
            part_values_str = ",\n".join(part_values)
            sql_parts = f"""
INSERT INTO streaming.kafka_partition_metrics (
    spark_run_id, batch_id, observed_at, topic, partition_id,
    start_offset, end_offset, latest_offset, rows_read, consumer_lag
) VALUES
{part_values_str}
ON CONFLICT (spark_run_id, batch_id, topic, partition_id) DO UPDATE SET
    start_offset = EXCLUDED.start_offset,
    end_offset = EXCLUDED.end_offset,
    latest_offset = EXCLUDED.latest_offset,
    rows_read = EXCLUDED.rows_read,
    consumer_lag = EXCLUDED.consumer_lag;
"""

        full_sql = f"BEGIN;\n{sql_ensure_run}\n{sql_batch}\n{sql_parts}\nCOMMIT;"
        self.execute(full_sql)


class PipelineMetricsListener(StreamingQueryListener):
    def __init__(self, db_writer: PostgresWriter, default_topic: str) -> None:
        super().__init__()
        self.db_writer = db_writer
        self.default_topic = default_topic

    def onQueryStarted(self, event: Any) -> None:
        print(
            f"Streaming query started: id={event.id}, runId={event.runId}",
            flush=True,
        )

    def onQueryProgress(self, event: Any) -> None:
        try:
            progress_dict = json.loads(event.progress.json)
            batch_metric, partition_rows = extract_batch_metrics(
                progress_dict, self.default_topic
            )
            self.db_writer.save_batch_metrics(batch_metric, partition_rows)
        except Exception as exc:
            print(
                f"Failed to record batch metrics to PostgreSQL: {exc}",
                file=sys.stderr,
                flush=True,
            )

    def onQueryTerminated(self, event: Any) -> None:
        print(
            f"Streaming query terminated: id={event.id}, runId={event.runId}",
            flush=True,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read transaction events from Kafka with Spark Structured Streaming."
    )
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="raw-events")
    parser.add_argument(
        "--checkpoint-location",
        default=os.getenv("SPARK_CHECKPOINT_LOCATION"),
        help=(
            "Checkpoint path override. Defaults to "
            f"{DEFAULT_CHECKPOINT_ROOT}/<topic>."
        ),
    )
    parser.add_argument("--watermark", default="30 seconds")
    parser.add_argument("--window-duration", default="1 minute")
    parser.add_argument("--trigger-interval", default="5 seconds")
    parser.add_argument(
        "--starting-offsets",
        choices=("earliest", "latest"),
        default="earliest",
    )
    parser.add_argument(
        "--postgres-host",
        default=os.getenv("POSTGRES_HOST", "postgres"),
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
    )
    parser.add_argument(
        "--postgres-db",
        default=os.getenv("POSTGRES_DB", "streaming_db"),
    )
    parser.add_argument(
        "--postgres-user",
        default=os.getenv("POSTGRES_USER", "streaming_user"),
    )
    parser.add_argument(
        "--postgres-password",
        default=os.getenv("POSTGRES_PASSWORD", "streaming_password"),
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    args = build_parser().parse_args(argv)
    if not args.checkpoint_location:
        args.checkpoint_location = f"{DEFAULT_CHECKPOINT_ROOT}/{args.topic}"
    return args


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
    parse_schema = StructType(
        [*TRANSACTION_SCHEMA.fields, StructField("_corrupt_record", StringType())]
    )
    parsed = decoded.withColumn(
        "transaction",
        F.from_json(
            F.col("raw_value"),
            parse_schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    ).select(
        "kafka_key",
        "raw_value",
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "transaction.*",
    )

    required_fields_present = F.lit(True)
    for field in TRANSACTION_SCHEMA.fields:
        value = F.col(field.name)
        present = value.isNotNull()
        if isinstance(field.dataType, StringType):
            present = present & (F.length(F.trim(value)) > 0)
        required_fields_present = required_fields_present & present

    return (
        parsed.withColumn(
            "validation_error",
            F.when(
                F.col("_corrupt_record").isNotNull() | ~required_fields_present,
                F.lit("invalid_json_or_missing_field"),
            )
            .when(
                ~F.col("kafka_key").eqNullSafe(F.col("account_id")),
                F.lit("key_mismatch"),
            )
            .when(F.col("currency") != F.lit("VND"), F.lit("unsupported_currency")),
        )
        .drop("_corrupt_record")
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
    """Aggregate valid transactions by Kafka topic, event-time window and category."""
    valid_transactions = transactions.filter(F.col("validation_error").isNull())

    aggregates = valid_transactions.groupBy(
        F.col("topic"),
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
        "topic",
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


def wait_for_query(query: Any) -> None:
    stop_requested = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        # Py4J calls from a signal handler can interrupt another Py4J call.
        stop_requested = True

    previous_handlers = {
        signum: signal.signal(signum, request_stop)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        while not query.awaitTermination(timeout=1):
            if stop_requested:
                print("\nStop signal received. Stopping streaming query...", flush=True)
                query.stop()
                return
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    spark = (
        SparkSession.builder.appName("transaction-window-stream")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    db_writer = PostgresWriter(
        spark=spark,
        host=args.postgres_host,
        port=args.postgres_port,
        database=args.postgres_db,
        user=args.postgres_user,
        password=args.postgres_password,
    )

    listener = PipelineMetricsListener(db_writer, args.topic)
    spark.streams.addListener(listener)

    raw = read_transactions(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        starting_offsets=args.starting_offsets,
    )
    transactions = parse_transactions(raw, args.watermark)
    transactions = transactions.observe(
        "transaction-latency",
        F.avg("processing_latency_ms").alias("latency_avg_ms"),
        F.expr("percentile_approx(processing_latency_ms, 0.95)").alias(
            "latency_p95_ms"
        ),
        F.max("processing_latency_ms").alias("latency_max_ms"),
    )
    window_results = aggregate_transactions(transactions, args.window_duration)

    def process_window_batch(batch_df: DataFrame, batch_id: int) -> None:
        rows = [row.asDict() for row in batch_df.collect()]
        if rows:
            print(
                f"[Batch {batch_id}] Upserting {len(rows)} window aggregations to PostgreSQL...",
                flush=True,
            )
            db_writer.save_window_results(rows)

    query = (
        window_results.writeStream
        .queryName("transaction-window-aggregates")
        .outputMode("update")
        .foreachBatch(process_window_batch)
        .option("checkpointLocation", args.checkpoint_location)
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    print(
        "Spark transaction window stream started: "
        f"query_id={query.id}, spark_run_id={query.runId}, "
        f"topic={args.topic}, watermark={args.watermark}, "
        f"window={args.window_duration}, "
        f"trigger={args.trigger_interval}, checkpoint={args.checkpoint_location}",
        flush=True,
    )

    db_writer.record_pipeline_start(
        spark_run_id=str(query.runId),
        query_id=str(query.id),
        topic=args.topic,
        checkpoint_location=args.checkpoint_location,
        watermark_delay_seconds=parse_duration_to_seconds(args.watermark),
        window_duration_seconds=parse_duration_to_seconds(args.window_duration),
        trigger_interval_seconds=parse_duration_to_seconds(args.trigger_interval),
        configuration={
            "topic": args.topic,
            "watermark": args.watermark,
            "window_duration": args.window_duration,
            "trigger_interval": args.trigger_interval,
            "checkpoint_location": args.checkpoint_location,
            "starting_offsets": args.starting_offsets,
        },
    )

    status = "stopped"
    try:
        wait_for_query(query)
    except Exception as exc:
        print(
            f"Streaming query terminated with error: {exc}",
            file=sys.stderr,
            flush=True,
        )
        status = "failed"
        raise
    finally:
        try:
            db_writer.record_pipeline_finish(str(query.runId), status=status)
        except Exception as exc:
            print(
                f"Failed to record pipeline finish: {exc}",
                file=sys.stderr,
                flush=True,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
