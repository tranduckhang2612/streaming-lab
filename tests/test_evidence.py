"""Tests for report evidence export."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from app.config import PROJECT_ROOT
from app.evidence import export_evidence_bundle


RUN_ID = "11111111-1111-4111-8111-111111111111"


class FakeStore:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def _execute(self, sql: str) -> str:
        self.sql.append(sql)
        if "SELECT r.run_id::TEXT" in sql:
            return RUN_ID
        if "json_build_object" in sql:
            return json.dumps({
                "run_id": RUN_ID,
                "experiment_type": "checkpoint",
                "topic": "raw-events",
                "started_at": "2026-11-11T00:00:00+00:00",
                "finished_at": "2026-11-11T00:01:00+00:00",
                "status": "passed",
                "configuration": {},
                "scenarios": [],
            })
        if "experiment_scenario_metrics" in sql:
            return "scenario_name,accuracy_complete\ncheckpoint-restart,true"
        if "window_accuracy_results" in sql:
            return "scenario_name,missing_count,extra_count\ncheckpoint-restart,0,0"
        if "checkpoint_recovery_results" in sql:
            return "scenario_name,checkpoint_recovery_verified\ncheckpoint-restart,true"
        raise AssertionError(sql)


class EvidenceTests(unittest.TestCase):
    def test_latest_run_exports_report_bundle_and_checksums(self) -> None:
        store = FakeStore()
        with tempfile.TemporaryDirectory() as directory:
            bundle = export_evidence_bundle(
                store,
                output_dir=Path(directory),
                project_root=PROJECT_ROOT,
            )
            self.assertEqual(bundle.run_id, RUN_ID)
            self.assertEqual(bundle.path.name, "checkpoint-11111111")
            expected = {
                "README.md",
                "run.json",
                "scenario-metrics.csv",
                "window-accuracy.csv",
                "checkpoint-recovery.csv",
                "grafana-dashboard.json",
                "dashboard-url.txt",
                "checksums.sha256",
            }
            self.assertEqual(
                {path.name for path in bundle.path.iterdir()},
                expected,
            )
            checksums = (bundle.path / "checksums.sha256").read_text()
            self.assertIn("scenario-metrics.csv", checksums)
            self.assertIn(RUN_ID, (bundle.path / "dashboard-url.txt").read_text())


if __name__ == "__main__":
    unittest.main()
