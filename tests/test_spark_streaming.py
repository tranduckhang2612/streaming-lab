from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import unittest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRANSACTION_STREAM_PATH = PROJECT_ROOT / "spark-apps" / "transaction_stream.py"
TRANSACTION_STREAM_SPEC = importlib.util.spec_from_file_location(
    "transaction_stream", TRANSACTION_STREAM_PATH
)
if TRANSACTION_STREAM_SPEC is None or TRANSACTION_STREAM_SPEC.loader is None:
    raise ImportError(f"Could not load {TRANSACTION_STREAM_PATH}")
transaction_stream = importlib.util.module_from_spec(TRANSACTION_STREAM_SPEC)
TRANSACTION_STREAM_SPEC.loader.exec_module(transaction_stream)

extract_batch_metrics = transaction_stream.extract_batch_metrics
parse_duration_to_seconds = transaction_stream.parse_duration_to_seconds
sql_literal = transaction_stream.sql_literal


class SparkStreamingHelperTests(unittest.TestCase):
    def test_parse_duration_to_seconds(self) -> None:
        self.assertEqual(parse_duration_to_seconds("30 seconds"), 30.0)
        self.assertEqual(parse_duration_to_seconds("1 minute"), 60.0)
        self.assertEqual(parse_duration_to_seconds("5 seconds"), 5.0)
        self.assertEqual(parse_duration_to_seconds("2 hours"), 7200.0)
        self.assertEqual(parse_duration_to_seconds("500 milliseconds"), 0.5)
        self.assertEqual(parse_duration_to_seconds("500 ms"), 0.5)
        self.assertEqual(parse_duration_to_seconds("invalid"), 0.0)

    def test_sql_literal(self) -> None:
        self.assertEqual(sql_literal(None), "NULL")
        self.assertEqual(sql_literal(True), "TRUE")
        self.assertEqual(sql_literal(False), "FALSE")
        self.assertEqual(sql_literal(123), "123")
        self.assertEqual(sql_literal(45.67), "45.67")
        self.assertEqual(sql_literal("grocery"), "'grocery'")
        self.assertEqual(sql_literal("O'Reilly"), "'O''Reilly'")

        dt = datetime(2026, 10, 25, 12, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(sql_literal(dt), "'2026-10-25T12:00:00+00:00'")

    def test_extract_batch_metrics(self) -> None:
        progress_sample = {
            "id": "e8d9c020-0000-0000-0000-000000000001",
            "runId": "a1b2c3d4-1111-2222-3333-444455556666",
            "batchId": 5,
            "timestamp": "2026-10-25T14:00:05.123Z",
            "batchDuration": 350,
            "numInputRows": 150,
            "observedMetrics": {
                "transaction-latency": {
                    "latency_avg_ms": 42.5,
                    "latency_p95_ms": 80,
                    "latency_max_ms": 120,
                }
            },
            "inputRowsPerSecond": 30.0,
            "processedRowsPerSecond": 428.57,
            "durationMs": {
                "triggerExecution": 350,
                "getBatch": 15,
                "addBatch": 250,
                "walCommit": 20,
                "commitOffsets": 10,
            },
            "eventTime": {
                "watermark": "2026-10-25T13:59:30.000Z",
                "max": "2026-10-25T14:00:05.000Z",
            },
            "stateOperators": [
                {
                    "numRowsTotal": 25,
                    "memoryUsedBytes": 10240,
                    "numRowsDroppedByWatermark": 2,
                    "commitTimeMs": 35,
                }
            ],
            "sources": [
                {
                    "description": "KafkaV2[Subscribe[raw-events]]",
                    "startOffset": {
                        "raw-events": {"0": 100, "1": 200, "2": 300}
                    },
                    "endOffset": {
                        "raw-events": {"0": 150, "1": 250, "2": 350}
                    },
                    "latestOffset": {
                        "raw-events": {"0": 160, "1": 255, "2": 350}
                    },
                    "numInputRows": 150,
                }
            ],
        }

        batch_metric, partition_rows = extract_batch_metrics(
            progress_sample, default_topic="raw-events"
        )

        self.assertEqual(
            batch_metric["spark_run_id"],
            "a1b2c3d4-1111-2222-3333-444455556666",
        )
        self.assertEqual(batch_metric["batch_id"], 5)
        self.assertEqual(batch_metric["num_input_rows"], 150)
        self.assertEqual(batch_metric["query_id"], progress_sample["id"])
        self.assertEqual(batch_metric["partition_count"], 3)
        self.assertEqual(batch_metric["latency_avg_ms"], 42.5)
        self.assertEqual(batch_metric["latency_p95_ms"], 80)
        self.assertEqual(batch_metric["latency_max_ms"], 120)
        self.assertEqual(batch_metric["wal_commit_ms"], 20)
        self.assertEqual(batch_metric["commit_offsets_ms"], 10)

        # checkpoint_commit_ms = walCommit (20) + commitOffsets (10) + commitTimeMs (35) = 65
        self.assertEqual(batch_metric["checkpoint_commit_ms"], 65)
        self.assertEqual(batch_metric["watermark_at"], "2026-10-25T13:59:30.000Z")
        self.assertEqual(batch_metric["state_rows_total"], 25)
        self.assertEqual(batch_metric["rows_dropped_by_watermark"], 2)

        # Partition metrics
        self.assertEqual(len(partition_rows), 3)
        # Partition 0: start 100, end 150 (read 50), latest 160 (lag 10)
        p0 = next(p for p in partition_rows if p["partition_id"] == 0)
        self.assertEqual(p0["rows_read"], 50)
        self.assertEqual(p0["consumer_lag"], 10)

        # Partition 1: start 200, end 250 (read 50), latest 255 (lag 5)
        p1 = next(p for p in partition_rows if p["partition_id"] == 1)
        self.assertEqual(p1["rows_read"], 50)
        self.assertEqual(p1["consumer_lag"], 5)

        # Partition 2: start 300, end 350 (read 50), latest 350 (lag 0)
        p2 = next(p for p in partition_rows if p["partition_id"] == 2)
        self.assertEqual(p2["rows_read"], 50)
        self.assertEqual(p2["consumer_lag"], 0)

        # Total lag = 10 + 5 + 0 = 15; Max lag = 10
        self.assertEqual(batch_metric["total_consumer_lag"], 15)
        self.assertEqual(batch_metric["max_partition_lag"], 10)


if __name__ == "__main__":
    unittest.main()
