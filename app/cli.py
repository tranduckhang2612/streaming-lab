"""Command-line interface. Values passed here override config.json."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

from .config import AppConfig, ConfigError, DEFAULT_CONFIG_PATH
from .experiment import (
    ExperimentRunner,
    RunReport,
    custom_plan,
    late_ratio_plan,
    partition_plan,
    rate_plan,
)


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("cannot be negative")
    return parsed


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def ratio(value: str) -> float:
    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the streaming lab using values from config.json."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Config file path (default: config.json).",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="Run one configured scenario.")
    _add_generator_options(run_parser)

    experiment = commands.add_parser(
        "experiment",
        help="Run a repeatable comparison.",
    )
    kinds = experiment.add_subparsers(dest="experiment_type", required=True)

    rate = kinds.add_parser("rate", help="Compare configured event rates.")
    rate.add_argument("--rates", nargs="+", type=positive_float)
    _add_generator_options(rate, include_rate=False)

    late = kinds.add_parser("late", help="Compare late-event ratios.")
    late.add_argument("--late-ratios", nargs="+", type=ratio)
    _add_generator_options(late, include_late_ratio=False)

    partition = kinds.add_parser("partition", help="Compare partition counts.")
    partition.add_argument("--partitions", nargs="+", type=positive_int)
    _add_generator_options(
        partition,
        include_topic=False,
        include_partitions=False,
    )

    commands.add_parser(
        "show-config",
        help="Validate and print the effective config.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = _effective_config(args)
        if args.command == "show-config":
            _print_config(config)
            return 0

        plans = {
            "run": custom_plan,
            "rate": rate_plan,
            "late": late_ratio_plan,
            "partition": partition_plan,
        }
        plan_name = "run" if args.command == "run" else args.experiment_type
        report = ExperimentRunner(config).run(plans[plan_name](config))
    except (
        ConfigError,
        OSError,
        RuntimeError,
        ValueError,
        subprocess.CalledProcessError,
    ) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    _print_report(report)
    return 0 if report.status == "passed" else 1


def _effective_config(args: argparse.Namespace) -> AppConfig:
    config = AppConfig.load(args.config)
    if args.command == "show-config":
        return config

    generator_fields = (
        "topic",
        "rate",
        "duration_seconds",
        "late_ratio",
        "late_delay_seconds",
        "account_count",
        "merchant_count",
        "min_amount_vnd",
        "max_amount_vnd",
        "seed",
    )
    changes = {
        name: getattr(args, name)
        for name in generator_fields
        if hasattr(args, name) and getattr(args, name) is not None
    }
    if args.command == "run" or (
        args.command == "experiment" and args.experiment_type != "partition"
    ):
        if getattr(args, "partitions", None) is not None:
            changes["partitions"] = args.partitions
    generator = config.generator.with_changes(**changes)

    experiment_changes: dict[str, object] = {}
    if getattr(args, "rates", None) is not None:
        experiment_changes["rates"] = tuple(args.rates)
    if getattr(args, "late_ratios", None) is not None:
        experiment_changes["late_ratios"] = tuple(args.late_ratios)
    if (
        args.command == "experiment"
        and args.experiment_type == "partition"
        and args.partitions is not None
    ):
        experiment_changes["partitions"] = tuple(args.partitions)
    if getattr(args, "export_logs", False):
        experiment_changes["export_logs"] = True

    runtime = config.runtime
    if getattr(args, "skip_build", False):
        runtime = replace(runtime, skip_build=True)
    return (
        config.with_generator(generator)
        .with_experiments(
            config.experiments.with_changes(**experiment_changes)
        )
        .with_runtime(runtime)
    )


def _add_generator_options(
    parser: argparse.ArgumentParser,
    *,
    include_topic: bool = True,
    include_partitions: bool = True,
    include_rate: bool = True,
    include_late_ratio: bool = True,
) -> None:
    if include_topic:
        parser.add_argument("--topic")
    if include_partitions:
        parser.add_argument("--partitions", type=positive_int)
    if include_rate:
        parser.add_argument("--rate", type=positive_float)
    parser.add_argument("--duration-seconds", type=positive_float)
    if include_late_ratio:
        parser.add_argument("--late-ratio", type=ratio)
    parser.add_argument("--late-delay-seconds", type=non_negative_float)
    parser.add_argument("--account-count", type=positive_int)
    parser.add_argument("--merchant-count", type=positive_int)
    parser.add_argument("--min-amount-vnd", type=positive_int)
    parser.add_argument("--max-amount-vnd", type=positive_int)
    parser.add_argument("--seed", type=int)
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Reuse the current generator image.",
    )
    parser.add_argument(
        "--export-logs",
        action="store_true",
        help="Also write local log files; logs always go to PostgreSQL.",
    )


def _print_report(report: RunReport) -> None:
    print("\n=== Run summary ===")
    for result in report.results:
        print(
            f"{result['scenario']}: status={result['status']}, "
            f"delivered={result['delivered']}, "
            f"rate={result['delivered_rate_events_s']}/s, "
            f"late={result['actual_late_ratio_percent']}%"
        )
    if report.csv_path:
        print(f"CSV result: {report.csv_path}")
    print(f"PostgreSQL run_id: {report.run_id}")


def _print_config(config: AppConfig) -> None:
    values = {
        "generator": config.generator.as_dict(),
        "experiments": {
            "rates": config.experiments.rates,
            "late_ratios": config.experiments.late_ratios,
            "partitions": config.experiments.partitions,
            "topic_prefix": config.experiments.topic_prefix,
            "results_dir": config.experiments.results_dir,
            "export_logs": config.experiments.export_logs,
        },
        "runtime": {
            "bootstrap_servers": config.runtime.bootstrap_servers,
            "skip_build": config.runtime.skip_build,
        },
    }
    print(json.dumps(values, indent=2, ensure_ascii=False))
