from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.config import AppConfig, ExperimentConfig, GeneratorConfig, RuntimeConfig
from app.experiment import (
    checkpoint_plan,
    ExperimentRunner,
    custom_plan,
    late_ratio_plan,
    partition_plan,
    rate_plan,
    watermark_plan,
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
        self.created: list[dict[str, object]] = []
        self.saved: list[dict[str, object]] = []
        self.finished = []
        self.sql = []

    def prepare(self) -> None:
        pass

    def create_run(self, **values: object) -> str:
        self.created.append(values)
        return "test-run-id"

    def save_scenario(self, **values: object) -> None:
        self.saved.append(values)

    def finish_run(self, *_values: object) -> None:
        self.finished.append(_values)

    def _execute(self, sql: str) -> str:
        self.sql.append(sql)
        if "FROM streaming.window_accuracy_results" in sql:
            return (
                '{"total_expected_count":20,"total_actual_count":20,'
                '"total_missing_count":0,"total_extra_count":0}'
            )
        return ""


class FakeSpark:
    def __init__(self, events, fail_start=False, fail_drain=False, fail_cleanup=False,
                 reported_watermark=None):
        self.events = events
        self.fail_start = fail_start
        self.fail_drain = fail_drain
        self.fail_cleanup = fail_cleanup
        self.reported_watermark = reported_watermark
        self.watermarks = []

    @contextmanager
    def session(self):
        self.events.append("session-start")
        try:
            yield
        finally:
            self.events.append("session-restore")

    @contextmanager
    def scenario(
        self,
        topic,
        _run_id,
        *,
        scenario_name="scenario",
        watermark_delay_seconds=None,
        checkpoint_location=None,
        starting_offsets="latest",
    ):
        self.watermarks.append(watermark_delay_seconds)
        self.events.append(f"start:{topic}")
        try:
            yield SimpleNamespace(
                topic=topic,
                checkpoint=checkpoint_location or f"/isolated/{topic}",
                watermark_delay_seconds=watermark_delay_seconds,
            )
        finally:
            self.events.append(f"stop:{topic}")
            if self.fail_cleanup:
                raise SparkPipelineError("Spark driver exited with code 137")

    def wait_for_offsets(self, pipeline, target, *, startup=False):
        self.events.append(f"{'ready' if startup else 'drain'}:{pipeline.topic}")
        if (startup and self.fail_start) or (not startup and self.fail_drain):
            raise SparkPipelineError("Spark did not consume the target offsets")
        watermark = self.reported_watermark
        if watermark is None:
            watermark = (
                pipeline.watermark_delay_seconds
                if pipeline.watermark_delay_seconds is not None
                else 30
            )
        return {"spark_run_id": f"spark-{pipeline.topic}", "query_id": f"query-{pipeline.topic}",
                "watermark_delay_seconds": watermark,
                "window_duration_seconds": 60,
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


class RecoveryKafka:
    def __init__(self):
        self.offset_values = (
            {0: 0, 1: 0, 2: 0},
            {0: 4, 1: 3, 2: 3},
            {0: 8, 1: 6, 2: 6},
        )
        self.reads = 0

    def ensure_topic(self, _topic, _partitions):
        pass

    def offsets(self, _topic):
        value = self.offset_values[self.reads]
        self.reads += 1
        return value


class RecoverySpark:
    def __init__(self, events):
        self.events = events
        self.run_number = 0

    @contextmanager
    def session(self):
        self.events.append("session-start")
        try:
            yield
        finally:
            self.events.append("session-restore")

    @contextmanager
    def scenario(
        self,
        topic,
        _run_id,
        *,
        scenario_name="scenario",
        watermark_delay_seconds=None,
        checkpoint_location=None,
        starting_offsets="latest",
    ):
        self.run_number += 1
        checkpoint = checkpoint_location or "/isolated/recovery"
        pipeline = SimpleNamespace(
            topic=topic,
            checkpoint=checkpoint,
            watermark_delay_seconds=watermark_delay_seconds,
            run_number=self.run_number,
        )
        self.events.append(f"start-{self.run_number}")
        try:
            yield pipeline
        finally:
            self.events.append(f"stop-{self.run_number}")

    def wait_for_offsets(self, pipeline, target, *, startup=False):
        self.events.append(
            f"{'ready' if startup else 'drain'}-{pipeline.run_number}"
        )
        return {
            "spark_run_id": (
                "22222222-2222-4222-8222-222222222222"
                if pipeline.run_number == 1
                else "44444444-4444-4444-8444-444444444444"
            ),
            "query_id": "33333333-3333-4333-8333-333333333333",
            "watermark_delay_seconds": 30,
            "window_duration_seconds": 60,
            "batch_id": pipeline.run_number,
            "end_offsets": {str(partition): offset for partition, offset in target.items()},
        }


class ExperimentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = AppConfig(
            GeneratorConfig(),
            ExperimentConfig(),
            RuntimeConfig(),
        )

    def test_plans_use_one_shared_scenario_model(self) -> None:
        custom = custom_plan(self.config)
        rate = rate_plan(self.config)
        late = late_ratio_plan(self.config)
        watermark = watermark_plan(self.config)
        partition = partition_plan(self.config)
        checkpoint = checkpoint_plan(self.config)
        self.assertEqual(len(rate.scenarios), 3)
        self.assertEqual(len(late.scenarios), 3)
        self.assertEqual(len(watermark.scenarios), 3)
        self.assertEqual(len(partition.scenarios), 3)
        self.assertEqual(len(checkpoint.scenarios), 1)
        self.assertFalse(custom.managed_spark)
        self.assertTrue(rate.managed_spark)
        self.assertTrue(late.managed_spark)
        self.assertTrue(watermark.managed_spark)
        self.assertTrue(partition.managed_spark)
        self.assertTrue(checkpoint.managed_spark)
        self.assertTrue(checkpoint.scenarios[0].checkpoint_recovery)
        self.assertEqual(checkpoint.scenarios[0].generator.late_ratio, 0)

    def test_watermark_plan_varies_only_watermark_and_deduplicates_values(self) -> None:
        config = self.config.with_experiments(replace(
            self.config.experiments,
            watermarks=(0, 10, 10, 30),
        ))
        scenarios = watermark_plan(config).scenarios

        self.assertEqual(
            [scenario.name for scenario in scenarios],
            ["watermark-0s", "watermark-10s", "watermark-30s"],
        )
        self.assertEqual(
            [scenario.watermark_delay_seconds for scenario in scenarios],
            [0, 10, 30],
        )
        self.assertTrue(all(
            scenario.generator == scenarios[0].generator
            for scenario in scenarios
        ))

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
        spark_events = []
        runner.spark = FakeSpark(spark_events)

        report = runner.run(custom_plan(self.config))

        self.assertEqual(report.status, "passed")
        self.assertEqual(report.results[0]["delivered"], 10)
        self.assertEqual(len(runner.store.saved), 1)
        self.assertFalse(runner.store.created[0]["configuration"]["managed_spark"])
        self.assertEqual(spark_events, [])

    @patch("app.experiment.prepare_environment")
    def test_rate_and_late_ratio_scenarios_record_exact_spark_bounds(self, _prepare):
        for plan_factory in (rate_plan, late_ratio_plan):
            with self.subTest(experiment=plan_factory.__name__):
                events = []
                runner = ExperimentRunner(self.config)
                runner.kafka = FakeKafka()
                runner.store = FakeStore()
                runner.spark = FakeSpark(events)
                original = plan_factory(self.config)
                plan = replace(
                    original,
                    scenarios=original.scenarios[:1],
                    export_csv=False,
                )
                with patch(
                    "app.experiment.execute",
                    side_effect=self.successful_execution,
                ):
                    report = runner.run(plan)

                self.assertEqual(report.status, "passed")
                result = runner.store.saved[0]["result"]
                self.assertEqual(result["spark_run_id"], "spark-raw-events")
                self.assertEqual(result["spark_query_id"], "query-raw-events")
                self.assertEqual(result["spark_baseline_batch_id"], 0)
                self.assertEqual(result["spark_final_batch_id"], 1)
                self.assertEqual(result["watermark_delay_seconds"], 30)
                self.assertEqual(result["spark_status"], "passed")
                self.assertTrue(
                    runner.store.created[0]["configuration"]["managed_spark"]
                )
                self.assertEqual(events, [
                    "session-start", "start:raw-events", "ready:raw-events",
                    "drain:raw-events", "stop:raw-events", "session-restore",
                ])

    @patch("app.experiment.prepare_environment")
    def test_watermark_scenario_records_requested_and_actual_spark_value(self, _prepare):
        config = self.config.with_experiments(replace(
            self.config.experiments,
            watermarks=(0, 10),
        ))
        events = []
        runner = ExperimentRunner(config)
        runner.kafka = FakeKafka()
        runner.store = FakeStore()
        runner.spark = FakeSpark(events)
        plan = replace(
            watermark_plan(config),
            scenarios=watermark_plan(config).scenarios[:1],
            export_csv=False,
        )
        with patch("app.experiment.execute", side_effect=self.successful_execution):
            report = runner.run(plan)

        self.assertEqual(report.status, "passed")
        self.assertEqual(runner.spark.watermarks, [0])
        self.assertEqual(
            runner.store.created[0]["configuration"]["scenarios"][0]
            ["watermark_delay_seconds"],
            0,
        )
        result = runner.store.saved[0]["result"]
        self.assertEqual(result["watermark_delay_seconds"], 0)
        self.assertEqual(result["spark_baseline_batch_id"], 0)
        self.assertEqual(result["spark_final_batch_id"], 1)

    @patch("app.experiment.prepare_environment")
    def test_watermark_mismatch_stops_before_generator_runs(self, _prepare):
        config = self.config.with_experiments(replace(
            self.config.experiments,
            watermarks=(10,),
        ))
        runner = ExperimentRunner(config)
        runner.kafka = FakeKafka()
        runner.store = FakeStore()
        runner.spark = FakeSpark([], reported_watermark=30)
        plan = replace(watermark_plan(config), export_csv=False)

        with patch("app.experiment.execute") as execute_mock:
            with self.assertRaisesRegex(
                SparkPipelineError,
                "scenario requires 10s",
            ):
                runner.run(plan)

        execute_mock.assert_not_called()
        self.assertEqual(runner.store.saved, [])
        self.assertEqual(runner.store.finished[-1][1], "failed")

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

    def test_complete_manifest_is_saved_as_scenario_ground_truth(self):
        config = self.config.with_experiments(replace(
            self.config.experiments,
            watermarks=(10,),
        ))
        runner = ExperimentRunner(config)
        runner.kafka = FakeKafka()
        runner.store = FakeStore()
        runner.spark = FakeSpark([])
        events = tuple({
            "transaction_id": f"00000000-0000-4000-8000-{sequence:012d}",
            "account_id": "account-001",
            "merchant_id": "merchant-001",
            "merchant_category": "grocery",
            "amount_vnd": 1000,
            "currency": "VND",
            "event_time": "2026-01-01T00:00:01.000Z",
            "produced_at": "2026-01-01T00:00:01.000Z",
            "sequence": sequence,
            "simulated_late": False,
        } for sequence in range(1, 11))
        execution = Execution(
            0,
            "generator output",
            {
                "attempted": "10", "delivered": "10", "failed": "0",
                "late": "0", "remaining": "0", "elapsed": "1.0",
                "delivered_rate": "10.0",
            },
            events,
            "a" * 64,
            "complete",
            None,
        )
        with patch("app.experiment.prepare_environment"), patch(
            "app.experiment.execute", return_value=execution
        ):
            report = runner.run(replace(watermark_plan(config), export_csv=False))

        self.assertEqual(report.status, "passed")
        result = report.results[0]
        self.assertEqual(result["ground_truth_status"], "complete")
        self.assertEqual(result["ground_truth_event_count"], 10)
        self.assertEqual(result["ground_truth_window_count"], 1)
        self.assertIn("experiment_verification_events", runner.store.sql[0])

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
            self.assertEqual(result["watermark_delay_seconds"], 30)
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
    def test_cleanup_failure_after_production_is_saved_as_scenario_evidence(self, _prepare):
        events = []
        runner, plan = self.partition_runner(events, fail_cleanup=True)
        plan = replace(plan, scenarios=plan.scenarios[:1])
        with patch("app.experiment.execute", side_effect=self.successful_execution):
            report = runner.run(plan)
        self.assertEqual(report.status, "failed")
        self.assertEqual(len(runner.store.saved), 1)
        result = runner.store.saved[0]["result"]
        self.assertEqual(result["spark_status"], "failed")
        self.assertEqual(result["status"], "failed")
        self.assertIn("code 137", result["spark_error"])
        self.assertEqual(result["spark_run_id"], "spark-raw-events-p1")

    @patch("app.experiment.prepare_environment")
    def test_checkpoint_recovery_reuses_query_and_drains_downtime_backlog(self, _prepare):
        events = []
        runner = ExperimentRunner(self.config)
        runner.kafka = RecoveryKafka()
        runner.store = FakeStore()
        runner.spark = RecoverySpark(events)

        executions = []
        for phase in (1, 2):
            delivered_events = tuple({
                "transaction_id": f"{phase:08d}-0000-4000-8000-{sequence:012d}",
                "account_id": "account-001",
                "merchant_id": "merchant-001",
                "merchant_category": "grocery",
                "amount_vnd": 1000,
                "currency": "VND",
                "event_time": f"2026-01-01T00:00:{phase:02d}.000Z",
                "produced_at": f"2026-01-01T00:00:{phase:02d}.000Z",
                "sequence": sequence,
                "simulated_late": False,
            } for sequence in range(1, 11))
            executions.append(Execution(
                0,
                f"phase {phase}",
                {
                    "attempted": "10", "delivered": "10", "failed": "0",
                    "late": "0", "remaining": "0", "elapsed": "1.0",
                    "delivered_rate": "10.0",
                },
                delivered_events,
                str(phase) * 64,
                "complete",
                None,
            ))

        with patch("app.experiment.execute", side_effect=executions):
            report = runner.run(replace(checkpoint_plan(self.config), export_csv=False))

        self.assertEqual(report.status, "passed")
        result = report.results[0]
        self.assertEqual(result["delivered"], 20)
        self.assertEqual(result["checkpoint_backlog_event_count"], 10)
        self.assertTrue(result["checkpoint_query_id_preserved"])
        self.assertTrue(result["checkpoint_spark_run_changed"])
        self.assertTrue(result["checkpoint_reused"])
        self.assertTrue(result["checkpoint_offsets_recovered"])
        self.assertTrue(result["checkpoint_identity_verified"])
        self.assertTrue(result["checkpoint_no_loss_no_duplicates"])
        self.assertEqual(result["ground_truth_event_count"], 20)
        self.assertEqual(result["ground_truth_status"], "complete")
        self.assertEqual(events, [
            "session-start",
            "start-1", "ready-1", "drain-1", "stop-1",
            "start-2", "ready-2", "drain-2", "stop-2",
            "session-restore",
        ])
        recovery = runner.store.saved[0]["result"]["checkpoint_recovery"]
        self.assertEqual(recovery["before_query_id"], recovery["after_query_id"])
        self.assertNotEqual(
            recovery["before_spark_run_id"],
            recovery["after_spark_run_id"],
        )

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
