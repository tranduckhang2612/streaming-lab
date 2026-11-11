import json
from pathlib import Path
import tempfile
import unittest

from app.config import (
    AppConfig,
    ConfigError,
    ExperimentConfig,
    GeneratorConfig,
    RuntimeConfig,
    watermark_seconds_text,
)


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
            "experiments": {"rates": [20, 40], "watermarks": [0, 15, 45]},
            "runtime": {"skip_build": True},
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps(values), encoding="utf-8")
            config = AppConfig.load(path)

        self.assertEqual(config.generator.resolved_topic, "raw-events-p6")
        self.assertEqual(config.generator.late_ratio, 0.3)
        self.assertEqual(config.experiments.rates, (20, 40))
        self.assertEqual(config.experiments.watermarks, (0, 15, 45))
        self.assertTrue(config.runtime.skip_build)

    def test_rejects_invalid_late_ratio(self) -> None:
        with self.assertRaises(ConfigError):
            GeneratorConfig(late_ratio=10)

    def test_spark_timeouts_must_be_positive_and_finite(self) -> None:
        for name in ("spark_startup_timeout_seconds", "spark_drain_timeout_seconds"):
            for value in (0, -1, float("nan"), float("inf")):
                with self.subTest(name=name, value=value), self.assertRaises(ConfigError):
                    RuntimeConfig(**{name: value})

    def test_watermarks_must_be_non_empty_finite_non_negative_numbers(self) -> None:
        self.assertEqual(ExperimentConfig(watermarks=(0,)).watermarks, (0,))
        self.assertEqual(
            ExperimentConfig(watermarks=(-0.0, 1.25)).watermarks,
            (0.0, 1.25),
        )
        self.assertEqual(watermark_seconds_text(0.001), "0.001")
        self.assertEqual(
            watermark_seconds_text(999_999_999.999),
            "999999999.999",
        )
        for values in (
            (),
            (-1,),
            (float("nan"),),
            (float("inf"),),
            (float("-inf"),),
            (True,),
            (1.0001,),
            (1_000_000_000,),
        ):
            with self.subTest(values=values), self.assertRaises(ConfigError):
                ExperimentConfig(watermarks=values)


if __name__ == "__main__":
    unittest.main()
