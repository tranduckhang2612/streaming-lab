import unittest

from app.config import GeneratorConfig, RuntimeConfig
from app.generator import build_command, parse_summary


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


if __name__ == "__main__":
    unittest.main()
