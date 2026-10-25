import json
from pathlib import Path
import tempfile
import unittest

from app.config import AppConfig, ConfigError, GeneratorConfig


class ConfigTests(unittest.TestCase):
    def test_auto_topic_tracks_partition_count(self) -> None:
        self.assertEqual(GeneratorConfig(partitions=3).resolved_topic, "raw-events")
        self.assertEqual(
            GeneratorConfig(partitions=6).resolved_topic,
            "raw-events-p6",
        )

    def test_loads_user_editable_json(self) -> None:
        values = {
            "generator": {"partitions": 6, "late_ratio": 0.3},
            "experiments": {"rates": [20, 40]},
            "runtime": {"skip_build": True},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(values), encoding="utf-8")
            config = AppConfig.load(path)

        self.assertEqual(config.generator.resolved_topic, "raw-events-p6")
        self.assertEqual(config.generator.late_ratio, 0.3)
        self.assertEqual(config.experiments.rates, (20, 40))
        self.assertTrue(config.runtime.skip_build)

    def test_rejects_invalid_late_ratio(self) -> None:
        with self.assertRaises(ConfigError):
            GeneratorConfig(late_ratio=10)


if __name__ == "__main__":
    unittest.main()
