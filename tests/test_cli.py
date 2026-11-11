from contextlib import redirect_stderr
from io import StringIO
import unittest
from unittest.mock import patch

from app.cli import _effective_config, build_parser
from app.config import AppConfig, ExperimentConfig, GeneratorConfig, RuntimeConfig


class CliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = AppConfig(
            GeneratorConfig(),
            ExperimentConfig(),
            RuntimeConfig(),
        )

    def test_watermark_command_overrides_config_and_generator_values(self) -> None:
        args = build_parser().parse_args([
            "experiment",
            "watermark",
            "--watermarks",
            "0",
            "10",
            "30",
            "--rate",
            "20",
            "--partitions",
            "2",
            "--skip-build",
        ])
        with patch("app.cli.AppConfig.load", return_value=self.config):
            effective = _effective_config(args)

        self.assertEqual(args.experiment_type, "watermark")
        self.assertEqual(effective.experiments.watermarks, (0.0, 10.0, 30.0))
        self.assertEqual(effective.generator.rate, 20)
        self.assertEqual(effective.generator.partitions, 2)
        self.assertTrue(effective.runtime.skip_build)

    def test_watermark_command_rejects_non_finite_values(self) -> None:
        for value in ("-1", "nan", "inf", "-inf", "1.0001", "1000000000"):
            with self.subTest(value=value), redirect_stderr(StringIO()):
                with self.assertRaises(SystemExit):
                    build_parser().parse_args([
                        "experiment",
                        "watermark",
                        "--watermarks",
                        value,
                    ])

    def test_verify_accuracy_command_parser(self) -> None:
        args = build_parser().parse_args([
            "verify-accuracy",
            "--run-id",
            "11111111-1111-4111-8111-111111111111",
            "--scenario",
            "watermark-10s",
        ])
        with patch("app.cli.AppConfig.load", return_value=self.config):
            effective = _effective_config(args)

        self.assertEqual(args.command, "verify-accuracy")
        self.assertEqual(args.run_id, "11111111-1111-4111-8111-111111111111")
        self.assertEqual(args.scenario, "watermark-10s")
        self.assertIsNotNone(effective)

    def test_checkpoint_command_uses_generator_overrides(self) -> None:
        args = build_parser().parse_args([
            "experiment",
            "checkpoint",
            "--rate",
            "25",
            "--duration-seconds",
            "4",
            "--skip-build",
        ])
        with patch("app.cli.AppConfig.load", return_value=self.config):
            effective = _effective_config(args)

        self.assertEqual(args.experiment_type, "checkpoint")
        self.assertEqual(effective.generator.rate, 25)
        self.assertEqual(effective.generator.duration_seconds, 4)
        self.assertTrue(effective.runtime.skip_build)

    def test_export_evidence_command_parser(self) -> None:
        args = build_parser().parse_args([
            "export-evidence",
            "--run-id",
            "11111111-1111-4111-8111-111111111111",
            "--output-dir",
            "report-evidence",
        ])
        with patch("app.cli.AppConfig.load", return_value=self.config):
            effective = _effective_config(args)

        self.assertEqual(args.command, "export-evidence")
        self.assertEqual(args.output_dir.name, "report-evidence")
        self.assertIsNotNone(effective)


if __name__ == "__main__":
    unittest.main()
