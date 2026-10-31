from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
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
from app.spark import SparkPipelineError


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
        self.finished = []

    def prepare(self) -> None:
        pass

    def create_run(self, **_values: object) -> str:
        return "test-run-id"

    def save_scenario(self, **values: object) -> None:
        self.saved.append(values)

    def finish_run(self, *_values: object) -> None:
        self.finished.append(_values)


class FakeSpark:
    def __init__(self, events, fail_start=False, fail_drain=False):
        self.events = events
        self.fail_start = fail_start
        self.fail_drain = fail_drain

    @contextmanager
    def session(self):
        self.events.append("session-start")
        try:
            yield
        finally:
            self.events.append("session-restore")

    @contextmanager
    def scenario(self, topic, _run_id):
        self.events.append(f"start:{topic}")
        try:
            yield SimpleNamespace(topic=topic, checkpoint=f"/isolated/{topic}")
        finally:
            self.events.append(f"stop:{topic}")

    def wait_for_offsets(self, pipeline, target, *, startup=False):
        self.events.append(f"{'ready' if startup else 'drain'}:{pipeline.topic}")
        if (startup and self.fail_start) or (not startup and self.fail_drain):
            raise SparkPipelineError("Spark did not consume the target offsets")
        return {"spark_run_id": f"spark-{pipeline.topic}", "query_id": f"query-{pipeline.topic}",
                "batch_id": 0 if startup else 1,
                "end_offsets": {str(p): offset for p, offset in target.items()}}


class PartitionKafka:
    def __init__(self):
        self.reads = {}

    def ensure_topic(self, _topic, _partitions):
        pass

    def offsets(self, topic):
        count = int(topic.rsplit("p", 1)[-1])
        self.reads[topic] = self.reads.get(topic, 0) + 1
        if self.reads[topic] == 1:
            return {p: 0 for p in range(count)}
        return {p: 10 // count + int(p < 10 % count) for p in range(count)}


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

    def partition_runner(self, events, **spark_options):
        config = self.config.with_experiments(replace(self.config.experiments, partitions=(1, 3)))
        runner = ExperimentRunner(config)
        runner.kafka = PartitionKafka()
        runner.store = FakeStore()
        runner.spark = FakeSpark(events, **spark_options)
        return runner, replace(partition_plan(config), export_csv=False)

    def successful_execution(self, generator, *_args):
        return Execution(0, "generator output", {
            "attempted": "10", "delivered": "10", "failed": "0", "late": "1",
            "remaining": "0", "elapsed": "1.0", "delivered_rate": "10.0",
        })

    @patch("app.experiment.prepare_environment")
    def test_partition_waits_for_the_correct_spark_before_and_after_producing(self, _prepare):
        events = []
        runner, plan = self.partition_runner(events)
        def execute(generator, *args):
            events.append(f"produce:{generator.resolved_topic}")
            return self.successful_execution(generator, *args)
        with patch("app.experiment.execute", side_effect=execute):
            report = runner.run(plan)
        self.assertEqual(report.status, "passed")
        self.assertEqual(events, [
            "session-start", "start:raw-events-p1", "ready:raw-events-p1",
            "produce:raw-events-p1", "drain:raw-events-p1", "stop:raw-events-p1",
            "start:raw-events-p3", "ready:raw-events-p3", "produce:raw-events-p3",
            "drain:raw-events-p3", "stop:raw-events-p3", "session-restore",
        ])
        for saved in runner.store.saved:
            result = saved["result"]
            self.assertEqual(result["spark_run_id"], f"spark-{result['topic']}")
            self.assertEqual(result["spark_status"], "passed")
            self.assertEqual(sum(result["spark_processed_offsets"].values()), 10)

    @patch("app.experiment.prepare_environment")
    @patch("app.experiment.execute")
    def test_spark_startup_failure_prevents_generator_and_fails_experiment(self, execute, _prepare):
        events = []
        runner, plan = self.partition_runner(events, fail_start=True)
        with self.assertRaisesRegex(SparkPipelineError, "target offsets"):
            runner.run(plan)
        execute.assert_not_called()
        self.assertEqual(runner.store.finished[-1][1], "failed")
        self.assertEqual(events[-2:], ["stop:raw-events-p1", "session-restore"])

    @patch("app.experiment.prepare_environment")
    def test_delivery_success_cannot_hide_a_spark_drain_failure(self, _prepare):
        events = []
        runner, plan = self.partition_runner(events, fail_drain=True)
        with patch("app.experiment.execute", side_effect=self.successful_execution):
            report = runner.run(plan)
        self.assertEqual(report.status, "failed")
        self.assertEqual(len(runner.store.saved), 2)
        for result in report.results:
            self.assertEqual(result["delivered"], 10)
            self.assertTrue(result["offsets_match_delivery"])
            self.assertEqual(result["spark_status"], "failed")
            self.assertEqual(result["status"], "failed")
            self.assertIn("target offsets", result["spark_error"])

    @patch("app.experiment.prepare_environment")
    @patch("app.experiment.execute", side_effect=KeyboardInterrupt)
    def test_cancelled_generator_cleans_up_spark_and_closes_experiment(self, _execute, _prepare):
        events = []
        runner, plan = self.partition_runner(events)
        with self.assertRaises(KeyboardInterrupt):
            runner.run(plan)
        self.assertEqual(events[-2:], ["stop:raw-events-p1", "session-restore"])
        self.assertEqual(runner.store.finished[-1][1], "failed")


if __name__ == "__main__":
    unittest.main()
