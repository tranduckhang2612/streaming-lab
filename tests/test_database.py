"""Unit regressions for experiment persistence SQL."""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.database import ExperimentStore


class DatabaseTests(unittest.TestCase):
    def test_postgres_error_includes_psql_stderr(self) -> None:
        store = ExperimentStore()
        completed = SimpleNamespace(
            returncode=3,
            stdout="",
            stderr="ERROR: legacy constraint rejected checkpoint",
        )
        with patch("app.database.subprocess.run", return_value=completed):
            with self.assertRaisesRegex(
                RuntimeError,
                "legacy constraint rejected checkpoint",
            ):
                store._execute("SELECT 1;")

    def test_checkpoint_scenario_persists_structured_recovery_evidence(self) -> None:
        store = ExperimentStore()
        store._execute = MagicMock()
        before_run = "22222222-2222-4222-8222-222222222222"
        after_run = "44444444-4444-4444-8444-444444444444"
        query_id = "33333333-3333-4333-8333-333333333333"
        result = {
            "scenario": "checkpoint-restart",
            "topic": "test-events",
            "started_at_utc": "2026-01-01T00:00:00Z",
            "finished_at_utc": "2026-01-01T00:03:00Z",
            "status": "passed",
            "checkpoint_recovery": {
                "before_spark_run_id": before_run,
                "after_spark_run_id": after_run,
                "before_query_id": query_id,
                "after_query_id": query_id,
                "checkpoint_location": "/test/recovery",
                "before_stop_offsets": {0: 10},
                "downtime_end_offsets": {0: 20},
                "recovered_offsets": {0: 20},
                "backlog_event_count": 10,
                "phase1_delivered": 10,
                "downtime_delivered": 10,
                "query_id_preserved": True,
                "spark_run_changed": True,
                "checkpoint_reused": True,
                "offsets_recovered": True,
                "identity_status": "passed",
            },
        }

        store.save_scenario(
            run_id="11111111-1111-4111-8111-111111111111",
            sequence_number=1,
            result=result,
            log_text="recovery",
        )

        sql = store._execute.call_args.args[0]
        self.assertIn("INSERT INTO streaming.checkpoint_recovery_evidence", sql)
        self.assertIn(before_run, sql)
        self.assertIn(after_run, sql)
        self.assertIn("WHERE TRUE", sql)


if __name__ == "__main__":
    unittest.main()
