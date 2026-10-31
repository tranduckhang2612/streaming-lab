import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from app.config import RuntimeConfig
from app.spark import SparkPipeline, SparkPipelineError, SparkRun


class SparkOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.store = Mock()
        self.spark = SparkPipeline(self.store, RuntimeConfig(
            spark_startup_timeout_seconds=3, spark_drain_timeout_seconds=3,
        ))
        self.pipeline = SparkRun("test-driver", "test-topic", "/test/checkpoint")

    def test_each_scenario_overrides_topic_driver_and_shared_checkpoint(self):
        with patch.object(self.spark, "_command", return_value=SimpleNamespace(stdout="")) as command, \
                patch.object(self.spark, "_state", return_value={"Running": True, "ExitCode": 0}), \
                patch.dict(os.environ, {"SPARK_CHECKPOINT_LOCATION": "/shared-checkpoint"}):
            checkpoints = []
            for topic in ("events-p1", "events-p3"):
                with self.spark.scenario(topic, "experiment-id") as pipeline:
                    checkpoints.append(pipeline.checkpoint)
                    invocation = command.call_args
                    self.assertEqual(invocation.kwargs["env"]["SPARK_INPUT_TOPIC"], topic)
                    self.assertEqual(invocation.kwargs["env"]["SPARK_DRIVER_HOST"], pipeline.container)
                    self.assertEqual(invocation.kwargs["env"]["SPARK_CHECKPOINT_LOCATION"], pipeline.checkpoint)
                    self.assertNotEqual(pipeline.checkpoint, "/shared-checkpoint")
                    self.assertNotIn("--service-ports", invocation.args[0])
            self.assertNotEqual(*checkpoints)

    def test_failure_cleans_up_own_job_and_restores_original_container(self):
        commands = []

        def command(arguments, **kwargs):
            commands.append(arguments)
            if "ps" in arguments:
                return SimpleNamespace(stdout="original-id\n")
            if "inspect" in arguments:
                return SimpleNamespace(stdout="False\n")
            return SimpleNamespace(stdout="")

        with patch.object(self.spark, "_command", side_effect=command), \
                patch.object(self.spark, "_state", return_value={"Running": True, "ExitCode": 0}):
            with self.assertRaisesRegex(RuntimeError, "generator crashed"):
                with self.spark.session():
                    with self.spark.scenario("test-topic", "experiment-id") as pipeline:
                        raise RuntimeError("generator crashed")
        self.assertIn(["docker", "stop", "--time", "60", "original-id"], commands)
        self.assertIn(["docker", "stop", "--time", "60", pipeline.container], commands)
        self.assertIn(["docker", "rm", "-f", pipeline.container], commands)
        self.assertEqual(commands[-1], ["docker", "start", "original-id"])

    def test_initially_stopped_regular_job_is_not_started(self):
        with patch.object(self.spark, "_command", return_value=SimpleNamespace(stdout="")) as command:
            with self.spark.session():
                pass
        self.assertFalse(any(c.args[0][:2] == ["docker", "start"] for c in command.call_args_list))

    def test_concurrent_oneoff_experiment_is_rejected(self):
        with patch.object(self.spark, "_command", side_effect=[
            SimpleNamespace(stdout=""), SimpleNamespace(stdout="other-job"),
            SimpleNamespace(stdout="True"),
        ]) as command:
            with self.assertRaisesRegex(SparkPipelineError, "already running"):
                with self.spark.session():
                    self.fail("Concurrent experiment should not start")
        self.assertEqual(command.call_count, 3)

    def test_wait_requires_offsets_from_every_partition_even_for_empty_topics(self):
        self.store.pipeline_progress.side_effect = [None, {
            "effective_status": "running", "end_offsets": {"0": 0},
        }, {"effective_status": "running", "end_offsets": {"0": 0, "1": 0}}]
        with patch.object(self.spark, "_state", return_value={"Running": True}), \
                patch("app.spark.time.sleep"):
            result = self.spark.wait_for_offsets(self.pipeline, {0: 0, 1: 0}, startup=True)
        self.assertEqual(result["end_offsets"], {"0": 0, "1": 0})
        self.assertEqual(self.store.pipeline_progress.call_count, 3)
        self.store.pipeline_progress.assert_called_with("/test/checkpoint", "test-topic")

    def test_unresponsive_query_cannot_pass_even_with_sufficient_offsets(self):
        clock = [0]
        self.store.pipeline_progress.return_value = {
            "effective_status": "unresponsive", "end_offsets": {"0": 20},
        }
        def tick(_seconds):
            clock[0] += 1
        with patch.object(self.spark, "_state", return_value={"Running": True}), \
                patch("app.spark.time.monotonic", side_effect=lambda: clock[0]), \
                patch("app.spark.time.sleep", side_effect=tick):
            with self.assertRaisesRegex(SparkPipelineError, "Timed out"):
                self.spark.wait_for_offsets(self.pipeline, {0: 10})

    def test_crashed_container_fails_before_waiting_or_reading_old_metrics(self):
        with patch.object(self.spark, "_state", return_value={"Running": False}):
            with self.assertRaisesRegex(SparkPipelineError, "not running"):
                self.spark.wait_for_offsets(self.pipeline, {0: 10})
        self.store.pipeline_progress.assert_not_called()

    def test_killed_job_cannot_be_reported_as_successfully_stopped(self):
        with patch.object(self.spark, "_command") as command, \
                patch.object(self.spark, "_state", return_value={"Running": False, "ExitCode": 137}):
            with self.assertRaisesRegex(SparkPipelineError, "137"):
                with self.spark.scenario("test-topic", "experiment-id") as pipeline:
                    pass
        self.assertEqual(command.call_args.args[0], ["docker", "rm", "-f", pipeline.container])


if __name__ == "__main__":
    unittest.main()
