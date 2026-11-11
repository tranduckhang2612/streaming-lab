"""Static regressions for provisioned Grafana dashboards."""

from __future__ import annotations

import json
from pathlib import Path
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = PROJECT_ROOT / "docker/grafana/dashboards"


class GrafanaDashboardTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dashboards = {
            path.name: json.loads(path.read_text(encoding="utf-8"))
            for path in DASHBOARD_DIR.glob("*.json")
        }

    def test_dashboard_uids_and_panel_ids_are_unique(self) -> None:
        uids = [dashboard["uid"] for dashboard in self.dashboards.values()]
        self.assertEqual(len(uids), len(set(uids)))
        for name, dashboard in self.dashboards.items():
            with self.subTest(dashboard=name):
                ids = [panel["id"] for panel in dashboard["panels"]]
                self.assertEqual(len(ids), len(set(ids)))

    def test_experiment_dashboard_covers_required_evidence(self) -> None:
        dashboard = self.dashboards["experiment_evidence.json"]
        variables = {item["name"] for item in dashboard["templating"]["list"]}
        self.assertEqual(variables, {"experiment_run_id", "scenario_name"})
        titles = {panel["title"] for panel in dashboard["panels"]}
        for required in (
            "Count accuracy theo scenario",
            "Missing / extra events",
            "Late events accepted / dropped",
            "Throughput theo scenario",
            "p95 end-to-end latency theo scenario",
            "Consumer lag theo scenario",
            "p95 checkpoint commit theo scenario",
            "Chi tiết accuracy theo window / category",
            "Checkpoint và Spark run evidence",
            "Checkpoint restart verification",
        ):
            self.assertIn(required, titles)

    def test_experiment_queries_are_run_and_scenario_scoped(self) -> None:
        dashboard = self.dashboards["experiment_evidence.json"]
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                sql = target.get("rawSql", "")
                with self.subTest(panel=panel["title"]):
                    self.assertIn("$experiment_run_id", sql)
                    self.assertIn("$scenario_name", sql)
                    self.assertNotIn("transaction_window_results", sql)
                    self.assertEqual(
                        target["datasource"]["uid"],
                        "postgres-streaming",
                    )

    def test_panel_grid_positions_do_not_overlap(self) -> None:
        dashboard = self.dashboards["experiment_evidence.json"]
        occupied: set[tuple[int, int]] = set()
        for panel in dashboard["panels"]:
            position = panel["gridPos"]
            cells = {
                (x, y)
                for x in range(position["x"], position["x"] + position["w"])
                for y in range(position["y"], position["y"] + position["h"])
            }
            self.assertFalse(occupied & cells, panel["title"])
            occupied.update(cells)

    def test_operational_dashboard_links_scenario_accuracy(self) -> None:
        dashboard = self.dashboards["streaming_pipeline.json"]
        comparison = next(panel for panel in dashboard["panels"] if panel["id"] == 10)
        sql = comparison["targets"][0]["rawSql"]
        self.assertIn("experiment_scenario_accuracy", sql)
        self.assertIn("overall_count_accuracy_percent", sql)
        self.assertIn("accuracy_complete", sql)

    def test_checkpoint_panel_prioritizes_report_verdict_columns(self) -> None:
        dashboard = self.dashboards["experiment_evidence.json"]
        panel = next(item for item in dashboard["panels"] if item["id"] == 12)
        sql = panel["targets"][0]["rawSql"]
        self.assertLess(
            sql.index("checkpoint_recovery_verified"),
            sql.index("before_spark_run_id"),
        )


if __name__ == "__main__":
    unittest.main()
