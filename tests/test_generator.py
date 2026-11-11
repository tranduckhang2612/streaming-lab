import unittest
from pathlib import Path
import tempfile

from app.config import GeneratorConfig, RuntimeConfig
from app.generator import _read_manifest, build_command, parse_summary


class GeneratorTests(unittest.TestCase):
    def test_parse_summary(self) -> None:
        summary = parse_summary(
            "Finished: attempted=100, delivered=100, failed=0, late=11, "
            "remaining=0, elapsed=2.00s, delivered_rate=50.0/s"
        )
        self.assertIsNotNone(summary)
        assert summary is not None
        self.assertEqual(summary["delivered"], "100")
        self.assertEqual(summary["late"], "11")

    def test_command_contains_configured_values(self) -> None:
        command = build_command(
            GeneratorConfig(partitions=6, rate=500, late_ratio=0.3),
            RuntimeConfig(),
            "test-client",
        )
        self.assertIn("raw-events-p6", command)
        self.assertEqual(command[command.index("--rate") + 1], "500")
        self.assertEqual(command[command.index("--late-ratio") + 1], "0.3")

    def test_manifest_mount_is_before_service_and_argument_is_after_service(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.touch()
            command = build_command(
                GeneratorConfig(), RuntimeConfig(), "test-client", path
            )
        service_index = command.index("event-generator")
        self.assertLess(command.index("--volume"), service_index)
        self.assertGreater(command.index("--event-manifest"), service_index)

    def test_read_manifest_requires_one_row_per_kafka_ack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            path.write_text('{"sequence":1}\n', encoding="utf-8")
            complete = _read_manifest(path, {"delivered": "1"})
            incomplete = _read_manifest(path, {"delivered": "2"})
        self.assertEqual(complete[2:], ("complete", None))
        self.assertEqual(incomplete[2], "incomplete")
        self.assertIn("acknowledged 2", incomplete[3])


if __name__ == "__main__":
    unittest.main()
