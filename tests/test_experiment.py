import unittest
from unittest.mock import patch

from app.config import AppConfig, ExperimentConfig, GeneratorConfig, RuntimeConfig
from app.experiment import (
    ExperimentRunner,
    custom_plan,
    late_ratio_plan,
    partition_plan,
    rate_plan,
)
from app.generator import Execution


class FakeKafka:
    def __init__(self) -> None:
        self.offset_reads = 0

    def ensure_topic(self, _topic: str, _partitions: int) -> None:
        pass

    def offsets(self, _topic: str) -> dict[int, int]:
        self.offset_reads += 1
        if self.offset_reads == 1:
            return {0: 0, 1: 0, 2: 0}
        return {0: 2, 1: 2, 2: 6}


class FakeStore:
    def __init__(self) -> None:
        self.saved: list[dict[str, object]] = []

    def prepare(self) -> None:
        pass

    def create_run(self, **_values: object) -> str:
        return "test-run-id"

    def save_scenario(self, **values: object) -> None:
        self.saved.append(values)

    def finish_run(self, *_values: object) -> None:
        pass


class ExperimentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = AppConfig(
            GeneratorConfig(),
            ExperimentConfig(),
            RuntimeConfig(),
        )

    def test_plans_use_one_shared_scenario_model(self) -> None:
        self.assertEqual(len(rate_plan(self.config).scenarios), 3)
        self.assertEqual(len(late_ratio_plan(self.config).scenarios), 3)
        self.assertEqual(len(partition_plan(self.config).scenarios), 3)

    def test_partition_plan_reuses_seed_for_fair_comparison(self) -> None:
        scenarios = partition_plan(self.config).scenarios
        self.assertEqual([item.generator.seed for item in scenarios], [42, 42, 42])
        self.assertEqual(
            [item.generator.resolved_topic for item in scenarios],
            ["raw-events-p1", "raw-events-p3", "raw-events-p6"],
        )

    def test_metrics_require_offset_match(self) -> None:
        summary = {
            "attempted": "10",
            "delivered": "10",
            "failed": "0",
            "late": "1",
            "remaining": "0",
            "elapsed": "1.0",
            "delivered_rate": "10.0",
        }
        metrics = ExperimentRunner._metrics(
            0,
            summary,
            target_rate=10,
            offset_count=9,
            all_partitions_active=True,
            require_all_partitions=False,
        )
        self.assertEqual(metrics["status"], "failed")

    @patch("app.experiment.prepare_environment")
    @patch("app.experiment.execute")
    def test_shared_runner_executes_custom_plan(
        self,
        execute_mock,
        _prepare_mock,
    ) -> None:
        execute_mock.return_value = Execution(
            0,
            "generator output",
            {
                "attempted": "10",
                "delivered": "10",
                "failed": "0",
                "late": "1",
                "remaining": "0",
                "elapsed": "1.0",
                "delivered_rate": "10.0",
            },
        )
        runner = ExperimentRunner(self.config)
        runner.kafka = FakeKafka()
        runner.store = FakeStore()

        report = runner.run(custom_plan(self.config))

        self.assertEqual(report.status, "passed")
        self.assertEqual(report.results[0]["delivered"], 10)
        self.assertEqual(len(runner.store.saved), 1)


if __name__ == "__main__":
    unittest.main()
