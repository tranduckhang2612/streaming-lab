"""Opt-in SQL regressions on an isolated, disposable PostgreSQL container.

RUN_POSTGRES_TESTS=1 python -m unittest discover -s tests -p test_pipeline_liveness.py -v
"""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import unittest
from uuid import uuid4

from app.database import ExperimentStore
from app.accuracy import GroundTruthConfig, save_accuracy_evidence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "transaction_stream_liveness", PROJECT_ROOT / "spark-apps/transaction_stream.py"
)
if SPEC is None or SPEC.loader is None:
    raise ImportError("Cannot load transaction_stream")
stream = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stream)


@unittest.skipUnless(os.getenv("RUN_POSTGRES_TESTS") == "1", "requires Docker PostgreSQL")
class PipelineLivenessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.project = f"streaming-liveness-test-{uuid4().hex[:12]}"
        cls.compose = [
            "docker", "compose", "-p", cls.project,
            "-f", str(PROJECT_ROOT / "tests/docker-compose.postgres-test.yml"),
        ]
        subprocess.run(
            [*cls.compose, "up", "-d", "postgres"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        cls.addClassCleanup(
            subprocess.run, [*cls.compose, "down", "-v", "--remove-orphans"],
            check=False, capture_output=True, timeout=15,
        )
        for _ in range(50):
            ready = subprocess.run(
                [*cls.compose, "exec", "-T", "postgres", "pg_isready",
                 "-h", "127.0.0.1", "-U", "postgres"],
                capture_output=True, timeout=5,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("Test PostgreSQL did not become ready")
        cls.schema = (PROJECT_ROOT / "database/postgres/init/streaming_schema.sql").read_text()
        cls.sql(cls.schema)

    @classmethod
    def sql(cls, sql, timeout_seconds=None):
        result = subprocess.run(
            [*cls.compose, "exec", "-T", "postgres", "psql", "-X", "-qAt",
             "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres"],
            input=sql, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    def setUp(self):
        self.sql(
            "TRUNCATE streaming.experiment_runs, "
            "streaming.transaction_window_results, streaming.pipeline_runs CASCADE;"
        )
        self.writer = stream.PostgresWriter(spark=None)
        # Exercise the actual writer SQL against PostgreSQL without needing a JVM.
        self.writer.execute = self.sql

    def start(self, query_id=None, run_id=None):
        run_id = run_id or str(uuid4())
        self.writer.record_pipeline_start(
            run_id, query_id or str(uuid4()), "test-events", "/test/checkpoint",
            30, 60, 5, {},
        )
        return run_id

    def state(self, run_id):
        result = json.loads(self.sql(
            "SELECT row_to_json(s) FROM streaming.pipeline_run_status s "
            f"WHERE spark_run_id = '{run_id}'::UUID;"
        ))
        # This derived age changes on every SELECT, even if the record is untouched.
        result.pop("heartbeat_age_seconds")
        return result

    @staticmethod
    def scenario_result(**changes):
        result = {
            "scenario": "scenario-1",
            "topic": "test-events",
            "partition_count": 1,
            "active_partitions": 1,
            "partition_event_counts": {"0": 30},
            "partition_balance_cv_percent": 0,
            "target_rate_events_s": 10,
            "target_late_ratio_percent": 10,
            "late_delay_seconds": 45,
            "duration_seconds": 2,
            "seed": 42,
            "started_at_utc": "2026-10-31T00:00:01Z",
            "finished_at_utc": "2026-10-31T00:00:03Z",
            "expected_events": 30,
            "attempted": 30,
            "delivered": 30,
            "failed": 0,
            "simulated_late": 3,
            "remaining": 0,
            "elapsed_seconds": 2,
            "delivered_rate_events_s": 15,
            "rate_achievement_percent": 150,
            "delivery_success_percent": 100,
            "actual_late_ratio_percent": 10,
            "status": "passed",
            "spark_status": "passed",
        }
        result.update(changes)
        return result

    def test_heartbeat_distinguishes_idle_stale_unknown_and_terminal_runs(self):
        run_id = self.start()
        self.assertEqual(self.state(run_id)["effective_status"], "running")
        self.sql(
            "UPDATE streaming.pipeline_runs SET last_heartbeat_at = "
            f"CURRENT_TIMESTAMP - INTERVAL '31 seconds' WHERE spark_run_id = '{run_id}';"
        )
        stale = self.state(run_id)
        self.assertEqual(stale["effective_status"], "unresponsive")
        self.assertEqual(stale["status"], "running")
        self.assertIsNone(stale["finished_at"])
        self.writer.record_pipeline_heartbeat(run_id)
        self.assertEqual(self.state(run_id)["effective_status"], "running")

        self.sql(f"UPDATE streaming.pipeline_runs SET last_heartbeat_at = NULL "
                 f"WHERE spark_run_id = '{run_id}';")
        self.assertEqual(self.state(run_id)["effective_status"], "unknown")
        self.writer.record_pipeline_finish(run_id, "failed")
        terminal = self.state(run_id)
        self.writer.record_pipeline_heartbeat(run_id)
        self.writer.record_pipeline_finish(run_id, "stopped")
        self.assertEqual(self.state(run_id), terminal)
        self.assertEqual(terminal["effective_status"], "failed")

    def test_restart_supersedes_only_the_same_checkpoint_query(self):
        query_id = str(uuid4())
        old = self.start(query_id)
        other = self.start()
        terminal = self.start(query_id)
        self.writer.record_pipeline_finish(terminal)
        new = self.start(query_id)
        previous = self.state(old)
        self.assertEqual(previous["effective_status"], "superseded")
        self.assertEqual(previous["superseded_by_run_id"], terminal)
        self.assertIsNone(previous["finished_at"])
        self.writer.record_pipeline_heartbeat(old)
        self.assertEqual(self.state(old), previous)
        self.assertEqual(self.state(other)["effective_status"], "running")
        self.assertEqual(self.state(terminal)["effective_status"], "stopped")
        self.assertEqual(self.state(new)["effective_status"], "running")

    def test_delayed_progress_cannot_revive_a_stopped_or_superseded_run(self):
        query_id = str(uuid4())
        old = self.start(query_id)
        new = self.start(query_id)
        self.writer.record_pipeline_finish(new)
        for run_id, expected in ((old, "superseded"), (new, "stopped")):
            batch, partitions = stream.extract_batch_metrics({
                "id": query_id, "runId": run_id, "batchId": 0,
                "timestamp": "2026-10-31T00:00:00Z", "numInputRows": 0,
            }, "test-events")
            self.writer.save_batch_metrics(batch, partitions)
            self.assertEqual(self.state(run_id)["effective_status"], expected)
        self.assertEqual(self.sql("SELECT COUNT(*) FROM streaming.pipeline_batch_metrics;"), "2")

    def test_schema_preserves_legacy_runs_and_can_be_reapplied(self):
        run_id = self.start()
        self.sql("DROP VIEW streaming.experiment_scenario_metrics; "
                 "DROP VIEW streaming.pipeline_run_status; "
                 "ALTER TABLE streaming.pipeline_runs DROP COLUMN last_heartbeat_at, "
                 "DROP COLUMN superseded_by_run_id;")
        try:
            self.sql(self.schema)
            self.sql(self.schema)
            old = self.state(run_id)
            self.assertEqual(old["status"], "running")
            self.assertEqual(old["effective_status"], "unknown")
            self.assertIsNone(old["last_heartbeat_at"])
            self.assertIsNone(old["finished_at"])
        finally:
            self.sql(self.schema)

    def test_schema_upgrades_all_legacy_experiment_type_constraints(self):
        self.sql("""
ALTER TABLE streaming.experiment_runs
    DROP CONSTRAINT experiment_runs_experiment_type_check;
ALTER TABLE streaming.experiment_runs
    ADD CONSTRAINT experiment_runs_experiment_type_check CHECK (
        experiment_type IN ('custom', 'rate', 'late_ratio', 'partition')
    ),
    ADD CONSTRAINT experiment_runs_valid_type CHECK (
        experiment_type IN ('custom', 'rate', 'late_ratio', 'partition')
    );
""")
        self.sql(self.schema)
        self.sql(self.schema)

        store = ExperimentStore(PROJECT_ROOT)
        store._execute = self.sql
        run_id = store.create_run(
            "watermark",
            "test-events",
            "2026-10-31T00:00:00Z",
            {"watermarks": [0, 10, 30]},
        )
        saved = json.loads(self.sql(f"""
SELECT json_build_object(
    'experiment_type', experiment_type,
    'configuration', configuration
)
FROM streaming.experiment_runs
WHERE run_id = '{run_id}'::UUID;
"""))
        self.assertEqual(saved["experiment_type"], "watermark")
        self.assertEqual(saved["configuration"]["watermarks"], [0, 10, 30])

        checkpoint_run_id = store.create_run(
            "checkpoint",
            "test-events",
            "2026-10-31T00:01:00Z",
            {"checkpoint_recovery": True},
        )
        self.assertEqual(
            self.sql(
                "SELECT experiment_type FROM streaming.experiment_runs "
                f"WHERE run_id = '{checkpoint_run_id}'::UUID;"
            ),
            "checkpoint",
        )
        self.assertEqual(
            self.sql("""
SELECT COUNT(*)
FROM pg_constraint
WHERE conrelid = 'streaming.experiment_runs'::REGCLASS
  AND contype = 'c'
  AND pg_get_constraintdef(oid) LIKE '%experiment_type%'
  AND pg_get_constraintdef(oid) NOT LIKE '%checkpoint%';
"""),
            "0",
        )

    def test_invalid_legacy_pipeline_watermark_does_not_abort_schema(self):
        query_id = str(uuid4())
        spark_run_id = self.start(query_id=query_id)
        store = ExperimentStore(PROJECT_ROOT)
        store._execute = self.sql
        experiment_run_id = store.create_run(
            "watermark", "test-events", "2026-10-31T00:00:00Z", {}
        )
        store.save_scenario(
            run_id=experiment_run_id,
            sequence_number=1,
            result=self.scenario_result(
                spark_run_id=spark_run_id,
                spark_query_id=query_id,
                spark_checkpoint_location="/test/checkpoint",
                spark_baseline_batch_id=0,
                spark_final_batch_id=1,
                watermark_delay_seconds=None,
            ),
            log_text="legacy invalid watermark",
        )
        self.sql("""
ALTER TABLE streaming.pipeline_runs
    DROP CONSTRAINT pipeline_runs_watermark_delay_seconds_check;
UPDATE streaming.pipeline_runs
SET watermark_delay_seconds = -1;
""")
        try:
            self.sql(self.schema)
            self.sql(self.schema)
            self.assertEqual(
                self.sql(
                    "SELECT watermark_delay_seconds IS NULL "
                    "FROM streaming.experiment_scenarios "
                    f"WHERE run_id = '{experiment_run_id}'::UUID;"
                ),
                "t",
            )
            self.assertEqual(
                self.sql(
                    "SELECT watermark_delay_seconds IS NULL "
                    "FROM streaming.experiment_scenario_metrics "
                    f"WHERE experiment_run_id = '{experiment_run_id}'::UUID;"
                ),
                "t",
            )
            with self.assertRaises(RuntimeError):
                self.writer.record_pipeline_start(
                    str(uuid4()), str(uuid4()), "bad-watermark", "/bad",
                    -1, 60, 5, {},
                )
            with self.assertRaises(RuntimeError):
                self.sql(
                    "UPDATE streaming.experiment_scenarios "
                    "SET watermark_delay_seconds = 'NaN'::NUMERIC "
                    f"WHERE run_id = '{experiment_run_id}'::UUID;"
                )
        finally:
            self.sql(
                "UPDATE streaming.pipeline_runs "
                "SET watermark_delay_seconds = 0 "
                f"WHERE spark_run_id = '{spark_run_id}'::UUID;"
            )
            self.sql(self.schema)

    def test_schema_upgrades_legacy_window_keys_and_nullable_metrics_without_data_loss(self):
        run_id = self.start()
        batch, parts = stream.extract_batch_metrics({
            "id": self.state(run_id)["query_id"], "runId": run_id, "batchId": 0,
            "timestamp": "2026-10-31T00:00:00Z", "numInputRows": 10,
            "sources": [{"startOffset": {"test-events": {"0": 0}},
                         "endOffset": {"test-events": {"0": 10}},
                         "latestOffset": {"test-events": {"0": 15}}}],
        }, "test-events")
        self.writer.save_batch_metrics(batch, parts)
        self.sql("""
ALTER TABLE streaming.transaction_window_results DROP COLUMN topic;
ALTER TABLE streaming.transaction_window_results ADD CONSTRAINT old_custom_window_pk
    PRIMARY KEY (window_start, window_end, merchant_category);
INSERT INTO streaming.transaction_window_results (
    window_start, window_end, merchant_category, transaction_count,
    total_amount_vnd, average_amount_vnd, min_amount_vnd, max_amount_vnd,
    simulated_late_count, average_event_lateness_ms, updated_at
) VALUES (
    '2026-10-31T00:00:00Z', '2026-10-31T00:01:00Z', 'grocery',
    2, 3000, 1500, 1000, 2000, 0, 0, '2026-10-31T00:01:00Z'
);
ALTER TABLE streaming.pipeline_batch_metrics ALTER COLUMN num_input_rows SET NOT NULL;
ALTER TABLE streaming.kafka_partition_metrics
    ALTER COLUMN start_offset SET NOT NULL, ALTER COLUMN end_offset SET NOT NULL,
    ALTER COLUMN latest_offset SET NOT NULL, ALTER COLUMN rows_read SET NOT NULL,
    ALTER COLUMN consumer_lag SET NOT NULL;
""")
        window_before = json.loads(self.sql(
            "SELECT to_jsonb(w) FROM streaming.transaction_window_results w;"
        ))
        metrics_before = self.sql("SELECT row_to_json(m) FROM streaming.pipeline_batch_metrics m;")
        partitions_before = self.sql("SELECT row_to_json(p) FROM streaming.kafka_partition_metrics p;")
        try:
            self.sql(self.schema)
            window_after = json.loads(self.sql(
                "SELECT to_jsonb(w) FROM streaming.transaction_window_results w;"
            ))
            self.assertEqual(window_after.pop("topic"), "<legacy-unknown>")
            self.assertEqual(window_after, window_before)
            self.assertEqual(self.sql("SELECT row_to_json(m) FROM streaming.pipeline_batch_metrics m;"), metrics_before)
            self.assertEqual(self.sql("SELECT row_to_json(p) FROM streaming.kafka_partition_metrics p;"), partitions_before)
            key_oid_sql = ("SELECT oid FROM pg_constraint WHERE contype='p' AND "
                           "conrelid='streaming.transaction_window_results'::REGCLASS;")
            key_oid = self.sql(key_oid_sql)
            self.sql(self.schema)
            self.assertEqual(self.sql(key_oid_sql), key_oid)

            # Same window/category can now hold independent real Kafka topics.
            self.sql("""
INSERT INTO streaming.transaction_window_results (
    topic, window_start, window_end, merchant_category, transaction_count,
    total_amount_vnd, average_amount_vnd, min_amount_vnd, max_amount_vnd,
    simulated_late_count, average_event_lateness_ms, updated_at
)
SELECT 'test-events', window_start, window_end, merchant_category, transaction_count,
       total_amount_vnd, average_amount_vnd, min_amount_vnd, max_amount_vnd,
       simulated_late_count, average_event_lateness_ms, updated_at
FROM streaming.transaction_window_results WHERE topic='<legacy-unknown>';
""")
            self.sql(self.schema)
            self.assertEqual(self.sql("SELECT COUNT(*) FROM streaming.transaction_window_results;"), "2")
            self.assertEqual(self.sql(key_oid_sql), key_oid)

            # Missing measurements must be accepted on the upgraded tables too.
            missing_batch = {**batch, "batch_id": 1, "num_input_rows": None}
            missing_part = {**parts[0], "batch_id": 1, **dict.fromkeys((
                "start_offset", "end_offset", "latest_offset", "rows_read", "consumer_lag",
            ))}
            self.writer.save_batch_metrics(missing_batch, [missing_part])
            saved = json.loads(self.sql("""
SELECT json_build_array(m.num_input_rows, p.start_offset, p.end_offset,
                       p.latest_offset, p.rows_read, p.consumer_lag)
FROM streaming.pipeline_batch_metrics m
JOIN streaming.kafka_partition_metrics p USING (spark_run_id, batch_id)
WHERE m.batch_id=1;
"""))
            self.assertEqual(saved, [None] * 6)
        finally:
            self.sql(self.schema)

    def test_experiment_view_uses_only_batches_inside_saved_bounds(self):
        query_id = str(uuid4())
        spark_run_id = self.start(query_id=query_id)
        self.sql(f"""
INSERT INTO streaming.pipeline_batch_metrics (
    spark_run_id, batch_id, observed_at, topic, num_input_rows,
    input_rows_per_second, processed_rows_per_second, add_batch_ms,
    checkpoint_commit_ms, rows_dropped_by_watermark, latency_avg_ms,
    latency_p95_ms, latency_max_ms, total_consumer_lag, max_partition_lag,
    state_rows_total, state_memory_bytes, batch_duration_ms, progress_json
) VALUES
    ('{spark_run_id}', 0, '2026-10-31T00:00:00Z', 'test-events',
     999, 999, 999, 1, 999, 999, 999, 999, 999, 999, 999, 999, 999, 999, '{{}}'),
    ('{spark_run_id}', 1, '2026-10-31T00:00:01Z', 'test-events',
     10, 10, 9, 1, 10, 0, 5, 8, 10, 4, 3, 100, 1000, 100, '{{}}'),
    ('{spark_run_id}', 2, '2026-10-31T00:00:02Z', 'test-events',
     20, 20, 18, 1, 30, 2, 7, 12, 15, 2, 1, 200, 2000, 300, '{{}}'),
    ('{spark_run_id}', 3, '2026-10-31T00:00:03Z', 'test-events',
     777, 777, 777, 1, 777, 777, 777, 777, 777, 777, 777, 777, 777, 777, '{{}}');
""")
        store = ExperimentStore(PROJECT_ROOT)
        store._execute = self.sql
        experiment_run_id = store.create_run(
            "partition", "test-events", "2026-10-31T00:00:00Z", {}
        )
        store.save_scenario(
            run_id=experiment_run_id,
            sequence_number=1,
            result=self.scenario_result(
                spark_run_id=spark_run_id,
                spark_query_id=query_id,
                spark_checkpoint_location="/test/checkpoint",
                spark_baseline_batch_id=0,
                spark_final_batch_id=2,
                watermark_delay_seconds=30,
            ),
            log_text="ok",
        )

        metrics = json.loads(self.sql(f"""
SELECT row_to_json(v)
FROM streaming.experiment_scenario_metrics v
WHERE experiment_run_id = '{experiment_run_id}'::UUID
  AND sequence_number = 1;
"""))
        self.assertEqual(metrics["metric_batch_count"], 2)
        self.assertEqual(metrics["measured_input_batch_count"], 2)
        self.assertEqual(metrics["spark_input_rows"], 30)
        self.assertEqual(float(metrics["avg_input_rows_per_second"]), 15)
        self.assertEqual(metrics["max_total_consumer_lag"], 4)
        self.assertEqual(float(metrics["avg_batch_p95_latency_ms"]), 10)
        self.assertEqual(float(metrics["max_batch_p95_latency_ms"]), 12)
        self.assertEqual(float(metrics["p95_checkpoint_commit_ms"]), 29)
        self.assertEqual(metrics["rows_dropped_by_watermark"], 2)
        self.assertEqual(metrics["max_state_memory_bytes"], 2000)
        self.assertEqual(float(metrics["p95_batch_duration_ms"]), 290)
        self.assertEqual(float(metrics["late_delay_seconds"]), 45)
        self.assertEqual(metrics["simulated_late"], 3)
        self.assertEqual(float(metrics["window_duration_seconds"]), 60)
        self.assertEqual(float(metrics["trigger_interval_seconds"]), 5)
        self.assertTrue(metrics["metrics_bounds_complete"])

        self.sql(f"""
UPDATE streaming.pipeline_batch_metrics
SET num_input_rows = NULL, rows_dropped_by_watermark = NULL
WHERE spark_run_id = '{spark_run_id}'::UUID AND batch_id = 2;
""")
        partial = json.loads(self.sql(f"""
SELECT row_to_json(v)
FROM streaming.experiment_scenario_metrics v
WHERE experiment_run_id = '{experiment_run_id}'::UUID
  AND sequence_number = 1;
"""))
        self.assertEqual(partial["metric_batch_count"], 2)
        self.assertEqual(partial["measured_input_batch_count"], 1)
        self.assertEqual(partial["measured_dropped_batch_count"], 1)
        self.assertIsNone(partial["spark_input_rows"])
        self.assertIsNone(partial["rows_dropped_by_watermark"])
        self.assertTrue(partial["metrics_bounds_complete"])

        store.save_scenario(
            run_id=experiment_run_id,
            sequence_number=2,
            result=self.scenario_result(
                scenario="generator-only",
                spark_status=None,
            ),
            log_text="no spark",
        )
        no_spark = json.loads(self.sql(f"""
SELECT row_to_json(v)
FROM streaming.experiment_scenario_metrics v
WHERE experiment_run_id = '{experiment_run_id}'::UUID
  AND sequence_number = 2;
"""))
        self.assertEqual(no_spark["metric_batch_count"], 0)
        self.assertIsNone(no_spark["avg_input_rows_per_second"])
        self.assertFalse(no_spark["metrics_bounds_complete"])

        self.sql(
            "DELETE FROM streaming.pipeline_runs "
            f"WHERE spark_run_id = '{spark_run_id}'::UUID;"
        )
        self.assertEqual(
            self.sql(
                "SELECT COUNT(*) FROM streaming.experiment_scenarios "
                f"WHERE run_id = '{experiment_run_id}'::UUID;"
            ),
            "2",
        )
        self.assertEqual(
            self.sql(
                "SELECT spark_run_id IS NULL FROM streaming.experiment_scenarios "
                f"WHERE run_id = '{experiment_run_id}'::UUID "
                "AND sequence_number = 1;"
            ),
            "t",
        )

    def test_accuracy_is_scenario_scoped_and_penalizes_extra_rows(self):
        query_id = str(uuid4())
        spark_run_id = self.start(query_id=query_id)
        store = ExperimentStore(PROJECT_ROOT)
        store._execute = self.sql
        experiment_run_id = store.create_run(
            "watermark", "test-events", "2026-10-31T00:00:00Z", {}
        )
        row = {
            "topic": "test-events",
            "window_start": "2026-10-31T00:00:00+00:00",
            "window_end": "2026-10-31T00:01:00+00:00",
            "merchant_category": "grocery",
            "transaction_count": 3,
            "total_amount_vnd": 4000,
            "average_amount_vnd": 1333.33,
            "min_amount_vnd": 1000,
            "max_amount_vnd": 2000,
            "simulated_late_count": 1,
            "average_event_lateness_ms": 15000,
        }
        self.writer.save_window_results(
            [row],
            experiment_run_id=experiment_run_id,
            scenario_name="watermark-10s",
            batch_id=1,
        )
        events = tuple({
            "transaction_id": f"00000000-0000-4000-8000-{sequence:012d}",
            "account_id": "account-001",
            "merchant_id": "merchant-001",
            "merchant_category": "grocery",
            "amount_vnd": amount,
            "currency": "VND",
            "event_time": f"2026-10-31T00:00:0{sequence}.000Z",
            "produced_at": (
                f"2026-10-31T00:00:0{sequence}.000Z"
                if sequence == 1 else "2026-10-31T00:00:47.000Z"
            ),
            "sequence": sequence,
            "simulated_late": sequence == 2,
        } for sequence, amount in ((1, 1000), (2, 2000)))
        save_accuracy_evidence(
            store,
            GroundTruthConfig(
                run_id=experiment_run_id,
                scenario_name="watermark-10s",
                topic="test-events",
                watermark_delay_seconds=10,
                window_duration_seconds=60,
            ),
            events,
            "a" * 64,
        )
        store.save_scenario(
            run_id=experiment_run_id,
            sequence_number=1,
            result=self.scenario_result(
                scenario="watermark-10s",
                spark_run_id=spark_run_id,
                spark_query_id=query_id,
                spark_checkpoint_location="/test/checkpoint",
                spark_baseline_batch_id=0,
                spark_final_batch_id=1,
                watermark_delay_seconds=10,
                ground_truth_status="complete",
                ground_truth_manifest_sha256="a" * 64,
                ground_truth_event_count=2,
                ground_truth_window_count=1,
            ),
            log_text="accuracy evidence",
        )
        summary = json.loads(self.sql(f"""
SELECT row_to_json(a)
FROM streaming.experiment_scenario_accuracy a
WHERE experiment_run_id = '{experiment_run_id}'::UUID
  AND scenario_name = 'watermark-10s';
"""))
        self.assertTrue(summary["accuracy_complete"])
        self.assertEqual(summary["total_expected_count"], 2)
        self.assertEqual(summary["total_actual_count"], 3)
        self.assertEqual(summary["total_missing_count"], 0)
        self.assertEqual(summary["total_extra_count"], 1)
        self.assertEqual(float(summary["overall_count_accuracy_percent"]), 66.667)

    def test_schema_backfills_valid_legacy_spark_links_and_ignores_bad_json(self):
        query_id = str(uuid4())
        spark_run_id = self.start(query_id=query_id)
        experiment_run_id = str(uuid4())
        valid = json.dumps({
            "spark_run_id": spark_run_id,
            "spark_query_id": query_id,
            "spark_checkpoint_location": "/legacy/checkpoint",
            "spark_baseline_batch_id": 0,
            "spark_final_batch_id": 2,
            "spark_status": "passed",
        })
        malformed = json.dumps({
            "spark_run_id": "not-a-uuid",
            "spark_checkpoint_location": "/orphan/checkpoint",
            "spark_baseline_batch_id": 10,
            "spark_final_batch_id": 2,
            "watermark_delay_seconds": 12.5,
        })
        negative_watermark = json.dumps({"watermark_delay_seconds": -1})
        oversized_watermark = json.dumps({
            "watermark_delay_seconds": 1_000_000_000,
        })
        self.sql("""
DROP VIEW streaming.experiment_scenario_metrics;
ALTER TABLE streaming.experiment_scenarios
    DROP COLUMN spark_run_id CASCADE,
    DROP COLUMN spark_query_id,
    DROP COLUMN spark_checkpoint_location,
    DROP COLUMN spark_baseline_batch_id,
    DROP COLUMN spark_final_batch_id,
    DROP COLUMN watermark_delay_seconds;
""")
        try:
            self.sql(f"""
INSERT INTO streaming.experiment_runs (
    run_id, experiment_type, topic, started_at, status, configuration
) VALUES (
    '{experiment_run_id}', 'partition', 'test-events',
    '2026-10-31T00:00:00Z', 'passed', '{{}}'
);
INSERT INTO streaming.experiment_scenarios (
    run_id, sequence_number, scenario_name, topic,
    started_at, finished_at, status, result_data
) VALUES
    ('{experiment_run_id}', 1, 'legacy-valid', 'test-events',
     '2026-10-31T00:00:01Z', '2026-10-31T00:00:03Z', 'passed',
     $json${valid}$json$::JSONB),
    ('{experiment_run_id}', 2, 'legacy-malformed', 'test-events',
     '2026-10-31T00:00:01Z', '2026-10-31T00:00:03Z', 'failed',
     $json${malformed}$json$::JSONB),
    ('{experiment_run_id}', 3, 'legacy-negative-watermark', 'test-events',
     '2026-10-31T00:00:01Z', '2026-10-31T00:00:03Z', 'failed',
     $json${negative_watermark}$json$::JSONB),
    ('{experiment_run_id}', 4, 'legacy-oversized-watermark', 'test-events',
     '2026-10-31T00:00:01Z', '2026-10-31T00:00:03Z', 'failed',
     $json${oversized_watermark}$json$::JSONB);
""")
            self.sql(self.schema)
            self.sql(self.schema)
            linked = json.loads(self.sql(f"""
SELECT json_build_object(
    'spark_run_id', spark_run_id,
    'spark_query_id', spark_query_id,
    'checkpoint', spark_checkpoint_location,
    'baseline', spark_baseline_batch_id,
    'final', spark_final_batch_id,
    'watermark', watermark_delay_seconds
)
FROM streaming.experiment_scenarios
WHERE run_id = '{experiment_run_id}'::UUID AND sequence_number = 1;
"""))
            self.assertEqual(linked["spark_run_id"], spark_run_id)
            self.assertEqual(linked["spark_query_id"], query_id)
            self.assertEqual(linked["checkpoint"], "/test/checkpoint")
            self.assertEqual(linked["baseline"], 0)
            self.assertEqual(linked["final"], 2)
            self.assertEqual(float(linked["watermark"]), 30)

            bad = json.loads(self.sql(f"""
SELECT json_build_object(
    'spark_run_id', spark_run_id,
    'baseline', spark_baseline_batch_id,
    'final', spark_final_batch_id,
    'watermark', watermark_delay_seconds
)
FROM streaming.experiment_scenarios
WHERE run_id = '{experiment_run_id}'::UUID AND sequence_number = 2;
"""))
            self.assertIsNone(bad["spark_run_id"])
            self.assertEqual(bad["baseline"], 10)
            self.assertIsNone(bad["final"])
            self.assertEqual(float(bad["watermark"]), 12.5)
            invalid_watermarks = json.loads(self.sql(f"""
SELECT json_agg(watermark_delay_seconds ORDER BY sequence_number)
FROM streaming.experiment_scenarios
WHERE run_id = '{experiment_run_id}'::UUID
  AND sequence_number IN (3, 4);
"""))
            self.assertEqual(invalid_watermarks, [None, None])
            self.assertEqual(
                self.sql(
                    "SELECT COUNT(*) FROM streaming.experiment_scenarios "
                    f"WHERE run_id = '{experiment_run_id}'::UUID;"
                ),
                "4",
            )
        finally:
            self.sql(self.schema)

    def test_experiment_progress_uses_only_its_topic_checkpoint_and_latest_run(self):
        checkpoint = "/isolated/scenario"
        query_id = str(uuid4())
        store = ExperimentStore(PROJECT_ROOT)
        store._execute = self.sql
        self.assertIsNone(store.pipeline_progress(checkpoint, "test-events"))
        for topic, end in (("test-events", 999), ("other-topic", 777), ("test-events", 10)):
            run_id = str(uuid4())
            self.writer.record_pipeline_start(run_id, query_id, topic, checkpoint, 30, 60, 5, {})
            self.assertEqual(store.pipeline_progress(checkpoint, topic)["batch_id"], -1)
            batch, parts = stream.extract_batch_metrics({
                "id": query_id, "runId": run_id, "batchId": 0,
                "timestamp": "2026-10-31T00:00:00Z", "numInputRows": end,
                "durationMs": {"addBatch": 1},
                "sources": [{"startOffset": {topic: {"0": 0}},
                             "endOffset": {topic: {"0": end}}}],
            }, topic)
            self.writer.save_batch_metrics(batch, parts)
        snapshot = store.pipeline_progress(checkpoint, "test-events")
        self.assertEqual(snapshot["spark_run_id"], run_id)
        self.assertEqual(snapshot["end_offsets"], {"0": 10})
        self.assertEqual(snapshot["batch_id"], 0)
        self.assertEqual(snapshot["effective_status"], "running")
        self.assertEqual(float(snapshot["watermark_delay_seconds"]), 30)
        self.assertEqual(snapshot["checkpoint_location"], checkpoint)
        idle, parts = stream.extract_batch_metrics({
            "id": query_id, "runId": run_id, "batchId": 1,
            "timestamp": "2026-10-31T00:00:01Z", "numInputRows": 0,
            "sources": [{"endOffset": {"test-events": {"0": 10}}}],
        }, "test-events")
        self.writer.save_batch_metrics(idle, parts)
        self.assertEqual(store.pipeline_progress(checkpoint, "test-events")["batch_id"], 0)


if __name__ == "__main__":
    unittest.main()
