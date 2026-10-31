from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import signal
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

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


class StreamingQueryLifecycleTests(unittest.TestCase):
    def test_signal_does_not_stop_query_inside_an_active_gateway_call(self) -> None:
        stop = Mock()

        def awaiting(timeout):
            signal.raise_signal(signal.SIGTERM)
            stop.assert_not_called()
            return False

        query = SimpleNamespace(awaitTermination=awaiting, stop=stop)
        previous_handler = signal.getsignal(signal.SIGTERM)
        transaction_stream.wait_for_query(query)
        stop.assert_called_once_with()
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous_handler)

    def test_idle_query_heartbeats_recover_after_database_failure(self) -> None:
        clock = [0]
        calls = []

        def awaiting(timeout):
            clock[0] += timeout
            return clock[0] >= 15

        def heartbeat():
            calls.append(clock[0])
            if len(calls) == 1:
                raise OSError("database unavailable")

        query = SimpleNamespace(awaitTermination=awaiting, stop=Mock())
        errors = io.StringIO()
        with patch.object(transaction_stream.time, "monotonic", side_effect=lambda: clock[0]), \
                patch("sys.stderr", errors):
            transaction_stream.wait_for_query(query, heartbeat=heartbeat)
        self.assertEqual(calls, [5, 10])
        self.assertIn("database unavailable", errors.getvalue())
        query.stop.assert_not_called()

    def test_stop_signal_takes_priority_over_due_heartbeat(self) -> None:
        heartbeat = Mock()

        def awaiting(timeout):
            signal.raise_signal(signal.SIGTERM)
            return False

        query = SimpleNamespace(awaitTermination=awaiting, stop=Mock())
        with patch.object(transaction_stream.time, "monotonic", side_effect=[0, 5]):
            transaction_stream.wait_for_query(query, heartbeat=heartbeat)
        heartbeat.assert_not_called()
        query.stop.assert_called_once_with()

    def test_query_failure_restores_signal_handlers(self) -> None:
        query = SimpleNamespace(awaitTermination=Mock(side_effect=RuntimeError("failed")))
        handlers = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
        with self.assertRaisesRegex(RuntimeError, "failed"):
            transaction_stream.wait_for_query(query, heartbeat=Mock())
        for signum, handler in handlers.items():
            self.assertEqual(signal.getsignal(signum), handler)


class PipelineLifecycleWriterTests(unittest.TestCase):
    def test_terminal_write_retries_a_temporary_outage(self) -> None:
        writer = transaction_stream.PostgresWriter(spark=None)
        with patch.object(writer, "execute", side_effect=[OSError("offline"), None]) as execute, \
                patch.object(transaction_stream.time, "sleep") as sleep:
            writer.record_pipeline_finish("b213c031-371c-4f95-8b8d-e08ab4bd953f")
        self.assertEqual(execute.call_count, 2)
        sleep.assert_called_once_with(1)

    def test_terminal_write_gives_up_after_three_attempts(self) -> None:
        writer = transaction_stream.PostgresWriter(spark=None)
        with patch.object(writer, "execute", side_effect=OSError("offline")) as execute, \
                patch.object(transaction_stream.time, "sleep") as sleep:
            with self.assertRaisesRegex(OSError, "offline"):
                writer.record_pipeline_finish("b213c031-371c-4f95-8b8d-e08ab4bd953f")
        self.assertEqual(execute.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_lifecycle_timeout_covers_connect_and_query_without_reconnecting(self) -> None:
        spark = Mock()
        writer = transaction_stream.PostgresWriter(spark=spark)
        spark._jvm.org.postgresql.Driver.return_value.connect.side_effect = OSError("offline")
        with self.assertRaisesRegex(OSError, "offline"):
            writer.record_pipeline_heartbeat("b213c031-371c-4f95-8b8d-e08ab4bd953f")
        spark._jvm.org.postgresql.Driver.return_value.connect.assert_called_once()
        spark._jvm.java.sql.DriverManager.getConnection.assert_not_called()
        props = spark._jvm.java.util.Properties.return_value
        for name in ("loginTimeout", "connectTimeout", "socketTimeout", "cancelSignalTimeout"):
            self.assertIn(unittest.mock.call(name, "3"), props.setProperty.call_args_list)

        spark._jvm.org.postgresql.Driver.return_value.connect.side_effect = None
        connection = spark._jvm.org.postgresql.Driver.return_value.connect.return_value
        writer.record_pipeline_heartbeat("b213c031-371c-4f95-8b8d-e08ab4bd953f")
        connection.createStatement.return_value.setQueryTimeout.assert_called_once_with(3)
        connection.createStatement.return_value.close.assert_called_once_with()
        connection.close.assert_called_once_with()


class WindowResultsWriterTests(unittest.TestCase):
    def test_upserts_preserve_other_topics_in_the_same_window(self) -> None:
        # The result table and upsert use SQL shared by SQLite and PostgreSQL.
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("ATTACH DATABASE ':memory:' AS streaming")
        schema = (
            PROJECT_ROOT / "database/postgres/init/streaming_schema.sql"
        ).read_text(encoding="utf-8")
        table_start = schema.index(
            "CREATE TABLE IF NOT EXISTS streaming.transaction_window_results"
        )
        table_end = schema.index("CREATE TABLE IF NOT EXISTS streaming.pipeline_runs")
        connection.executescript(schema[table_start:table_end])
        writer = transaction_stream.PostgresWriter(spark=None)
        window = {
            "window_start": datetime(2026, 10, 30, 1, 0, tzinfo=timezone.utc),
            "window_end": datetime(2026, 10, 30, 1, 1, tzinfo=timezone.utc),
            "merchant_category": "grocery",
            "transaction_count": 1,
            "total_amount_vnd": 1000,
            "average_amount_vnd": 1000,
            "min_amount_vnd": 1000,
            "max_amount_vnd": 1000,
            "simulated_late_count": 0,
            "average_event_lateness_ms": 0,
        }
        with patch.object(writer, "execute", side_effect=connection.execute):
            writer.save_window_results(
                [
                    {**window, "topic": "raw-events-p1"},
                    {**window, "topic": "raw-events-p3"},
                ]
            )
            updated = {
                **window,
                "topic": "raw-events-p1",
                "transaction_count": 2,
                "total_amount_vnd": 2000,
            }
            writer.save_window_results([updated])
            writer.save_window_results([updated])

        self.assertEqual(
            connection.execute(
                "SELECT topic, transaction_count, total_amount_vnd "
                "FROM streaming.transaction_window_results ORDER BY topic"
            ).fetchall(),
            [("raw-events-p1", 2, 2000), ("raw-events-p3", 1, 1000)],
        )


@unittest.skipUnless(os.getenv("RUN_SPARK_TESTS") == "1", "requires a Spark runtime")
class WindowAggregationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            transaction_stream.SparkSession.builder.master("local[1]")
            .appName("test-window-topic-isolation")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_idle_stream_keeps_heartbeating_after_a_database_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            raw = self.spark.readStream.schema("value STRING").json(directory)
            query = raw.writeStream.format("memory").queryName("idle_heartbeat_test").start()
            calls = []

            def heartbeat():
                calls.append(None)
                if len(calls) == 1:
                    raise OSError("database unavailable")
                signal.raise_signal(signal.SIGTERM)

            try:
                with patch.object(transaction_stream, "PIPELINE_HEARTBEAT_INTERVAL_SECONDS", 1), \
                        patch("sys.stderr", io.StringIO()):
                    transaction_stream.wait_for_query(query, heartbeat=heartbeat)
                self.assertEqual(len(calls), 2)
                self.assertFalse(query.isActive)
                self.assertTrue(all(p["numInputRows"] == 0 for p in query.recentProgress))
            finally:
                query.stop()

    def raw_events(self, events):
        return self.spark.createDataFrame(
            [
                (key, value, "raw-events", 0, offset, datetime(2026, 10, 30, 1, 0, 10))
                for offset, (key, value) in enumerate(events)
            ],
            "key STRING, value STRING, topic STRING, partition INT, offset LONG, "
            "timestamp TIMESTAMP",
        )

    def valid_event(self):
        return {
            "transaction_id": "b213c031-371c-4f95-8b8d-e08ab4bd953f",
            "account_id": "account-001",
            "merchant_id": "merchant-001",
            "merchant_category": "grocery",
            "amount_vnd": 1000,
            "currency": "VND",
            "event_time": "2026-10-30T01:00:10.000Z",
            "produced_at": "2026-10-30T01:00:10.000Z",
            "sequence": 1,
            "simulated_late": False,
        }

    def test_validation_rejects_missing_null_empty_and_malformed_events(self) -> None:
        valid = self.valid_event()
        cases = [("valid", "account-001", json.dumps(valid), None)]
        contract = json.loads(
            (PROJECT_ROOT / "contracts/transaction-schema.json").read_text()
        )
        for field in contract["required"]:
            missing = {key: value for key, value in valid.items() if key != field}
            cases.append((f"missing_{field}", "account-001", json.dumps(missing),
                          "invalid_json_or_missing_field"))
            cases.append((f"null_{field}", "account-001", json.dumps({**valid, field: None}),
                          "invalid_json_or_missing_field"))
            if contract["properties"][field].get("type") == "string" or field == "currency":
                for empty in ("", "   "):
                    cases.append((f"empty_{field}_{empty!r}", "account-001",
                                  json.dumps({**valid, field: empty}),
                                  "invalid_json_or_missing_field"))
        for field, value in [
            ("amount_vnd", "not-a-number"),
            ("sequence", "not-a-number"),
            ("simulated_late", "not-a-boolean"),
            ("event_time", "not-a-timestamp"),
            ("produced_at", "not-a-timestamp"),
        ]:
            cases.append((f"wrong_type_{field}", "account-001",
                          json.dumps({**valid, field: value}),
                          "invalid_json_or_missing_field"))
        for value in (None, "null", "[]", "{broken", json.dumps(valid)[:-1]):
            cases.append((f"malformed_{value}", "account-001", value,
                          "invalid_json_or_missing_field"))
        for key in (None, "", "account-999"):
            cases.append((f"key_{key}", key, json.dumps(valid), "key_mismatch"))
        cases.append(("currency", "account-001", json.dumps({**valid, "currency": "USD"}),
                      "unsupported_currency"))

        parsed = transaction_stream.parse_transactions(
            self.raw_events([(key, value) for _, key, value, _ in cases])
        )
        actual = {
            row.offset: row.validation_error
            for row in parsed.select("offset", "validation_error").collect()
        }
        for offset, (name, _, _, expected) in enumerate(cases):
            with self.subTest(case=name):
                self.assertEqual(actual[offset], expected)

        rows = transaction_stream.aggregate_transactions(parsed, "1 minute").collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].transaction_count, 1)
        self.assertEqual(rows[0].total_amount_vnd, 1000)
        self.assertEqual(rows[0].simulated_late_count, 0)
        self.assertEqual(rows[0].average_event_lateness_ms, 0)

    def test_batch_with_only_invalid_events_has_no_window_results(self) -> None:
        valid = self.valid_event()
        parsed = transaction_stream.parse_transactions(
            self.raw_events([
                ("account-001", json.dumps({**valid, "merchant_category": None})),
                ("account-001", json.dumps({**valid, "simulated_late": None})),
            ]),
        )
        self.assertEqual(
            transaction_stream.aggregate_transactions(parsed, "1 minute").collect(), []
        )

    def test_business_rules_match_contract_and_exclude_invalid_transactions(self) -> None:
        valid = self.valid_event()
        properties = json.loads(
            (PROJECT_ROOT / "contracts/transaction-schema.json").read_text()
        )["properties"]
        accepted = [
            {**valid, "merchant_category": category}
            for category in properties["merchant_category"]["enum"]
        ] + [
            {**valid, "amount_vnd": properties["amount_vnd"]["minimum"] + 1},
            {**valid, "sequence": properties["sequence"]["minimum"] + 1},
            {**valid, "transaction_id": valid["transaction_id"].upper()},
            {**valid, "transaction_id": "00000000-0000-0000-0000-000000000000"},
            {**valid, "account_id": "account-1000", "merchant_id": "merchant-1000"},
            {**valid, "account_id": "account-000", "merchant_id": "merchant-000"},
        ]
        cases = [(f"valid_{i}", event, None) for i, event in enumerate(accepted)]
        invalid = [
            ("amount_vnd", [-1000, 0, properties["amount_vnd"]["minimum"] - 1],
             "invalid_amount_vnd"),
            ("sequence", [-1, properties["sequence"]["minimum"] - 1], "invalid_sequence"),
            ("merchant_category", ["not-in-contract", "Grocery", "grocery "],
             "unsupported_merchant_category"),
            ("transaction_id", ["tx1", valid["transaction_id"].replace("-", ""),
                                "g213c031-371c-4f95-8b8d-e08ab4bd953f",
                                valid["transaction_id"] + "\n"], "invalid_transaction_id"),
            ("account_id", ["account-01", "account-abc", "Account-001", "account-001\n"],
             "invalid_account_id"),
            ("merchant_id", ["merchant-01", "merchant-abc", "Merchant-001", "merchant-001\n"],
             "invalid_merchant_id"),
        ]
        for field, values, error in invalid:
            cases.extend((f"{field}_{value!r}", {**valid, field: value}, error)
                         for value in values)
        # Match the Kafka key even for malformed account IDs to test the ID rule itself.
        raw = self.raw_events([
            (event["account_id"], json.dumps(event)) for _, event, _ in cases
        ])
        parsed = transaction_stream.parse_transactions(raw)
        errors = {
            row.offset: row.validation_error
            for row in parsed.select("offset", "validation_error").collect()
        }
        for offset, (name, _, expected) in enumerate(cases):
            with self.subTest(case=name):
                self.assertEqual(errors[offset], expected)

        expected_results = {}
        for event in accepted:
            category = event["merchant_category"]
            count, amount = expected_results.get(category, (0, 0))
            expected_results[category] = (count + 1, amount + event["amount_vnd"])
        prepared = transaction_stream.prepare_transactions(raw, "30 seconds")
        actual_results = {
            row.merchant_category: (row.transaction_count, row.total_amount_vnd)
            for row in transaction_stream.aggregate_transactions(prepared, "1 minute").collect()
        }
        self.assertEqual(actual_results, expected_results)

    def test_future_timestamp_guard_allows_clock_skew_and_old_events(self) -> None:
        reference = datetime(2026, 10, 30, 1, 0, 10, tzinfo=timezone.utc)

        def stamp(value):
            return value.isoformat().replace("+00:00", "Z")

        valid = self.valid_event()
        events = [
            valid,
            {**valid, "event_time": stamp(reference + timedelta(seconds=30))},
            {**valid, "event_time": stamp(reference + timedelta(seconds=31))},
            {**valid, "produced_at": stamp(reference + timedelta(seconds=31))},
            {**valid, "event_time": stamp(reference - timedelta(minutes=5)),
             "simulated_late": True},
        ]
        clock = transaction_stream.F.lit(reference)
        with patch.object(transaction_stream.F, "current_timestamp", return_value=clock):
            parsed = transaction_stream.parse_transactions(
                self.raw_events([("account-001", json.dumps(event)) for event in events]),
                max_future_skew_seconds=30,
            )
        errors = {
            row.offset: row.validation_error
            for row in parsed.select("offset", "validation_error").collect()
        }
        self.assertEqual(errors, {0: None, 1: None, 2: "future_timestamp",
                                  3: "future_timestamp", 4: None})

    def test_rejected_events_cannot_advance_stream_watermark(self) -> None:
        # Keep both valid events inside one window regardless of the test start second.
        base_time = (datetime.now(timezone.utc) - timedelta(minutes=1)).replace(
            second=10, microsecond=0
        )

        def stamp(value):
            return value.isoformat().replace("+00:00", "Z")

        valid = {**self.valid_event(), "event_time": stamp(base_time),
                 "produced_at": stamp(base_time)}
        future = stamp(base_time + timedelta(days=3650))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming = root / "incoming"
            incoming.mkdir()
            raw = self.spark.readStream.schema(
                "key STRING, value STRING, topic STRING, partition INT, offset LONG, "
                "timestamp TIMESTAMP"
            ).json(str(incoming))
            transactions = transaction_stream.prepare_transactions(raw, "30 seconds")
            transactions = transactions.observe(
                "transaction-latency",
                transaction_stream.F.avg("processing_latency_ms").alias("latency_avg_ms"),
                transaction_stream.F.expr(
                    "percentile_approx(processing_latency_ms, 0.95)"
                ).alias("latency_p95_ms"),
                transaction_stream.F.max("processing_latency_ms").alias("latency_max_ms"),
            )
            updates = []

            def consume(batch, _batch_id):
                updates.extend(row.asDict() for row in batch.collect())

            query = (
                transaction_stream.aggregate_transactions(transactions, "1 minute")
                .writeStream.outputMode("update").foreachBatch(consume)
                .option("checkpointLocation", str(root / "checkpoint")).start()
            )
            try:
                def feed(name, events):
                    rows = [dict(key="account-001", value=json.dumps(event),
                                 topic="raw-events", partition=0, offset=i,
                                 timestamp=stamp(base_time))
                            for i, event in enumerate(events)]
                    staging = root / name
                    staging.write_text("\n".join(json.dumps(row) for row in rows))
                    staging.replace(incoming / name)
                    query.processAllAvailable()

                feed("first.json", [valid, {**valid, "currency": "USD", "event_time": future}])
                # A batch containing only rejected events must not move the watermark.
                feed("invalid.json", [
                    {**valid, "event_time": future},
                    {**valid, "event_time": future, "produced_at": future},
                    {**valid, "amount_vnd": -1000},
                    {**valid, "merchant_category": "not-in-contract"},
                    {**valid, "sequence": 0},
                ])
                self.assertEqual(len(updates), 1)
                feed("second.json", [{**valid, "sequence": 2,
                                     "event_time": stamp(base_time + timedelta(seconds=1))}])
                self.assertEqual(updates[-1]["transaction_count"], 2)
                self.assertEqual(updates[-1]["total_amount_vnd"], 2000)
                count_before_late = len(updates)
                feed("late.json", [{**valid, "event_time": stamp(base_time - timedelta(minutes=5)),
                                   "simulated_late": True}])
                self.assertEqual(len(updates), count_before_late)
                progress = list(query.recentProgress)
                dropped = sum(op.get("numRowsDroppedByWatermark", 0)
                              for batch in progress for op in batch.get("stateOperators", []))
                self.assertGreater(dropped, 0)
                watermarks = [datetime.fromisoformat(batch["eventTime"]["watermark"].replace("Z", "+00:00"))
                              for batch in progress if batch.get("eventTime", {}).get("watermark")]
                self.assertTrue(watermarks)
                self.assertLessEqual(max(watermarks), base_time + timedelta(seconds=1))
            finally:
                query.stop()

    def test_aggregation_separates_actual_kafka_topics(self) -> None:
        event_time = datetime(2026, 10, 30, 1, 0, 10, tzinfo=timezone.utc)
        transactions = self.spark.createDataFrame(
            [
                ("raw-events-p1", event_time, "grocery", "tx1", 1000, False, 0, None),
                ("raw-events-p1", event_time, "grocery", "tx2", 3000, True, 1000, None),
                ("raw-events-p3", event_time, "grocery", "tx3", 9000, False, 0, None),
            ],
            "topic STRING, event_time TIMESTAMP, merchant_category STRING, "
            "transaction_id STRING, amount_vnd LONG, simulated_late BOOLEAN, "
            "event_lateness_ms LONG, validation_error STRING",
        )
        results = {
            row.topic: (row.transaction_count, row.total_amount_vnd)
            for row in transaction_stream.aggregate_transactions(
                transactions, "1 minute"
            ).collect()
        }
        self.assertEqual(
            results, {"raw-events-p1": (2, 4000), "raw-events-p3": (1, 9000)}
        )


class BatchMetricsTests(unittest.TestCase):
    def progress(self, **metrics):
        return {
            "id": "e8d9c020-0000-0000-0000-000000000001",
            "runId": "a1b2c3d4-1111-2222-3333-444455556666",
            "batchId": 0,
            "timestamp": "2026-10-30T01:00:00.000Z",
            **metrics,
        }

    def test_missing_metrics_remain_unknown(self) -> None:
        metric, partitions = extract_batch_metrics(self.progress(), "raw-events")
        for field in (
            "partition_count", "num_input_rows", "checkpoint_commit_ms",
            "state_rows_total", "state_memory_bytes", "rows_dropped_by_watermark",
            "latency_avg_ms", "total_consumer_lag", "max_partition_lag",
        ):
            with self.subTest(field=field):
                self.assertIsNone(metric[field])
        self.assertEqual(partitions, [])

    def test_first_batch_does_not_invent_start_offset_or_lag(self) -> None:
        metric, partitions = extract_batch_metrics(
            self.progress(numInputRows=2, sources=[{
                "startOffset": None,
                "endOffset": {"raw-events": {"0": 102}},
                "latestOffset": None,
            }]),
            "raw-events",
        )
        self.assertEqual(metric["num_input_rows"], 2)
        self.assertEqual(metric["partition_count"], 1)
        self.assertIsNone(metric["total_consumer_lag"])
        self.assertIsNone(metric["max_partition_lag"])
        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions[0]["end_offset"], 102)
        for field in ("start_offset", "latest_offset", "rows_read", "consumer_lag"):
            with self.subTest(field=field):
                self.assertIsNone(partitions[0][field])

    def test_missing_end_offset_preserves_other_known_offsets(self) -> None:
        metric, partitions = extract_batch_metrics(
            self.progress(sources=[{
                "startOffset": {"raw-events": {"0": 100}},
                "endOffset": None,
                "latestOffset": {"raw-events": {"0": 105}},
            }]),
            "raw-events",
        )
        self.assertEqual(metric["partition_count"], 1)
        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions[0]["start_offset"], 100)
        self.assertEqual(partitions[0]["latest_offset"], 105)
        for field in ("end_offset", "rows_read", "consumer_lag"):
            with self.subTest(field=field):
                self.assertIsNone(partitions[0][field])

    def test_partial_partition_lags_do_not_produce_a_partial_total(self) -> None:
        metric, partitions = extract_batch_metrics(
            self.progress(sources=[{
                "startOffset": {"raw-events": {"0": 100, "1": 200}},
                "endOffset": {"raw-events": {"0": 102, "1": 202}},
                "latestOffset": {"raw-events": {"0": 105, "1": None}},
            }]),
            "raw-events",
        )
        self.assertEqual([row["rows_read"] for row in partitions], [2, 2])
        self.assertEqual([row["consumer_lag"] for row in partitions], [3, None])
        self.assertIsNone(metric["total_consumer_lag"])
        self.assertIsNone(metric["max_partition_lag"])

    def test_inconsistent_offsets_do_not_become_zero_measurements(self) -> None:
        metric, partitions = extract_batch_metrics(
            self.progress(sources=[{
                "startOffset": {"raw-events": {"0": 105}},
                "endOffset": {"raw-events": {"0": 102}},
                "latestOffset": {"raw-events": {"0": 100}},
            }]),
            "raw-events",
        )
        self.assertEqual(partitions[0]["start_offset"], 105)
        self.assertEqual(partitions[0]["end_offset"], 102)
        self.assertIsNone(partitions[0]["rows_read"])
        self.assertIsNone(partitions[0]["consumer_lag"])
        self.assertIsNone(metric["total_consumer_lag"])

    def test_partial_offsets_across_topics_keep_their_topic_identity(self) -> None:
        subscription = "raw-events-p1,raw-events-p3"
        metric, partitions = extract_batch_metrics(
            self.progress(sources=[{
                "startOffset": {"raw-events-p1": {"0": 100}},
                "endOffset": {"raw-events-p1": {"0": 102}},
                "latestOffset": {
                    "raw-events-p1": {"0": 105}, "raw-events-p3": {"0": 7},
                },
            }]),
            subscription,
        )
        self.assertEqual(metric["topic"], subscription)
        self.assertEqual(metric["partition_count"], 2)
        self.assertEqual([row["topic"] for row in partitions], ["raw-events-p1", "raw-events-p3"])
        self.assertEqual(partitions[0]["consumer_lag"], 3)
        self.assertEqual(partitions[1]["latest_offset"], 7)
        self.assertIsNone(partitions[1]["start_offset"])
        self.assertIsNone(partitions[1]["consumer_lag"])
        self.assertIsNone(metric["total_consumer_lag"])

    def test_explicit_zero_metrics_are_preserved(self) -> None:
        metric, partitions = extract_batch_metrics(
            self.progress(
                numInputRows=0,
                durationMs={"walCommit": 0, "commitOffsets": 0},
                stateOperators=[{
                    "numRowsTotal": 0, "memoryUsedBytes": 0,
                    "numRowsDroppedByWatermark": 0, "commitTimeMs": 0,
                }],
                sources=[{
                    name: {"raw-events": {"0": 0}}
                    for name in ("startOffset", "endOffset", "latestOffset")
                }],
            ),
            "raw-events",
        )
        for field in (
            "num_input_rows", "checkpoint_commit_ms", "state_rows_total",
            "state_memory_bytes", "rows_dropped_by_watermark",
            "total_consumer_lag", "max_partition_lag",
        ):
            with self.subTest(field=field):
                self.assertEqual(metric[field], 0)
        for field in ("start_offset", "end_offset", "latest_offset", "rows_read", "consumer_lag"):
            with self.subTest(field=field):
                self.assertEqual(partitions[0][field], 0)

    def test_incomplete_state_metrics_remain_unknown(self) -> None:
        metric, _ = extract_batch_metrics(
            self.progress(
                durationMs={"walCommit": 10, "commitOffsets": 20},
                stateOperators=[{
                    "numRowsTotal": 5, "memoryUsedBytes": 100,
                    "numRowsDroppedByWatermark": 1, "commitTimeMs": 30,
                }, {}],
            ),
            "raw-events",
        )
        for field in (
            "state_rows_total", "state_memory_bytes",
            "rows_dropped_by_watermark", "checkpoint_commit_ms",
        ):
            with self.subTest(field=field):
                self.assertIsNone(metric[field])

    def test_checkpoint_total_requires_all_applicable_components(self) -> None:
        for durations, operators, expected in (
            ({"walCommit": 10}, [], None),
            ({"commitOffsets": 20}, [], None),
            ({"walCommit": 10, "commitOffsets": 20}, None, None),
            ({"walCommit": 10, "commitOffsets": 20}, [], 30),
        ):
            with self.subTest(durations=durations, operators=operators):
                metric, _ = extract_batch_metrics(
                    self.progress(durationMs=durations, stateOperators=operators),
                    "raw-events",
                )
                self.assertEqual(metric["checkpoint_commit_ms"], expected)

    def test_null_progress_sections_do_not_break_extraction(self) -> None:
        metric, partitions = extract_batch_metrics(
            self.progress(
                observedMetrics=None, durationMs=None, stateOperators=None,
                eventTime=None, sources=None,
            ),
            "raw-events",
        )
        self.assertIsNone(metric["checkpoint_commit_ms"])
        self.assertIsNone(metric["partition_count"])
        self.assertEqual(partitions, [])


class SparkStreamingHelperTests(unittest.TestCase):
    def test_future_skew_configuration_is_non_negative(self) -> None:
        with patch.dict(os.environ, {"SPARK_MAX_FUTURE_SKEW_SECONDS": "45"}):
            self.assertEqual(transaction_stream.parse_args([]).max_future_skew_seconds, 45)
            args = transaction_stream.parse_args(["--max-future-skew-seconds", "0"])
            self.assertEqual(args.max_future_skew_seconds, 0)
        with patch.dict(os.environ, {"SPARK_MAX_FUTURE_SKEW_SECONDS": ""}):
            self.assertEqual(transaction_stream.parse_args([]).max_future_skew_seconds, 30)
        with patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit):
                transaction_stream.parse_args(["--max-future-skew-seconds", "-1"])

    def test_checkpoint_defaults_are_stable_and_separate_topics(self) -> None:
        with patch.dict(os.environ, {"SPARK_CHECKPOINT_LOCATION": ""}):
            default_args = transaction_stream.parse_args([])
            first_run = transaction_stream.parse_args(["--topic", "raw-events-p1"])
            restarted = transaction_stream.parse_args(["--topic", "raw-events-p1"])
            other_topic = transaction_stream.parse_args(["--topic", "raw-events-p3"])

        self.assertEqual(
            default_args.checkpoint_location,
            "file:///opt/spark/work-dir/checkpoints/transaction-window-v3/raw-events",
        )
        self.assertEqual(first_run.checkpoint_location, restarted.checkpoint_location)
        self.assertNotEqual(first_run.checkpoint_location, other_topic.checkpoint_location)

    def test_empty_compose_checkpoint_uses_topic_default(self) -> None:
        with patch.dict(os.environ, {"SPARK_CHECKPOINT_LOCATION": ""}):
            args = transaction_stream.parse_args(
                ["--topic", "raw-events-p6", "--checkpoint-location", ""]
            )

        self.assertEqual(
            args.checkpoint_location,
            "file:///opt/spark/work-dir/checkpoints/transaction-window-v3/raw-events-p6",
        )

    def test_checkpoint_overrides_keep_existing_paths(self) -> None:
        existing_path = "file:///opt/spark/work-dir/checkpoints/transaction-window-v2"
        with patch.dict(os.environ, {"SPARK_CHECKPOINT_LOCATION": existing_path}):
            env_args = transaction_stream.parse_args([])
            cli_args = transaction_stream.parse_args(
                ["--checkpoint-location", "file:///custom/checkpoint"]
            )

        self.assertEqual(env_args.checkpoint_location, existing_path)
        self.assertEqual(cli_args.checkpoint_location, "file:///custom/checkpoint")

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
