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
        cls.container = f"streaming-liveness-test-{uuid4().hex[:12]}"
        subprocess.run(
            ["docker", "run", "--rm", "-d", "--name", cls.container,
             "--network", "none", "--tmpfs", "/var/lib/postgresql/data",
             "-e", "POSTGRES_HOST_AUTH_METHOD=trust", "postgres:17.11-alpine"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        cls.addClassCleanup(
            subprocess.run, ["docker", "rm", "-f", cls.container],
            check=False, capture_output=True, timeout=15,
        )
        for _ in range(50):
            ready = subprocess.run(
                ["docker", "exec", cls.container, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
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
            ["docker", "exec", "-i", cls.container, "psql", "-X", "-qAt",
             "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres"],
            input=sql, capture_output=True, text=True, timeout=10,
        )
        if result.returncode:
            raise RuntimeError(result.stderr)
        return result.stdout.strip()

    def setUp(self):
        self.sql("TRUNCATE streaming.transaction_window_results, streaming.pipeline_runs CASCADE;")
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
        self.sql("DROP VIEW streaming.pipeline_run_status; "
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
        idle, parts = stream.extract_batch_metrics({
            "id": query_id, "runId": run_id, "batchId": 1,
            "timestamp": "2026-10-31T00:00:01Z", "numInputRows": 0,
            "sources": [{"endOffset": {"test-events": {"0": 10}}}],
        }, "test-events")
        self.writer.save_batch_metrics(idle, parts)
        self.assertEqual(store.pipeline_progress(checkpoint, "test-events")["batch_id"], 0)


if __name__ == "__main__":
    unittest.main()
