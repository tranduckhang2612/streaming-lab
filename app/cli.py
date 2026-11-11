"""Command-line interface. Values passed here override config.json."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
import math
from pathlib import Path
import subprocess
import sys
from uuid import UUID

from .accuracy import load_accuracy_summary
from .config import (
    AppConfig,
    ConfigError,
    DEFAULT_CONFIG_PATH,
    PROJECT_ROOT,
    canonical_watermark_seconds,
)
from .experiment import (
    checkpoint_plan,
    ExperimentRunner,
    RunReport,
    custom_plan,
    late_ratio_plan,
    partition_plan,
    rate_plan,
    watermark_plan,
)
from .database import ExperimentStore
from .evidence import export_evidence_bundle


def positive_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise argparse.ArgumentTypeError("cannot be negative")
    return parsed


def watermark_seconds(value: str) -> float:
    try:
        return canonical_watermark_seconds(float(value))
    except (ConfigError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


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


def uuid_value(value: str) -> str:
    try:
        return str(UUID(value))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a valid UUID") from exc


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

    watermark = kinds.add_parser(
        "watermark",
        help="Compare event-time watermark delays in seconds.",
    )
    watermark.add_argument(
        "--watermarks",
        nargs="+",
        type=watermark_seconds,
        help="Watermark delays in seconds, for example: 10 30 60.",
    )
    _add_generator_options(watermark)

    partition = kinds.add_parser("partition", help="Compare partition counts through Kafka and Spark.")
    partition.add_argument("--partitions", nargs="+", type=positive_int)
    _add_generator_options(
        partition,
        include_topic=False,
        include_partitions=False,
    )

    checkpoint = kinds.add_parser(
        "checkpoint",
        help=(
            "Stop Spark, produce a backlog, then restart from the same "
            "checkpoint and verify recovery."
        ),
    )
    _add_generator_options(checkpoint, include_late_ratio=False)

    commands.add_parser(
        "show-config",
        help="Validate and print the effective config.",
    )

    verify = commands.add_parser(
        "verify-accuracy",
        help="Show scenario-scoped accuracy from captured experiment evidence.",
    )
    verify.add_argument(
        "--run-id",
        type=uuid_value,
        help="Experiment run UUID. Defaults to the latest run with evidence.",
    )
    verify.add_argument(
        "--scenario",
        help="Optional scenario name within the selected experiment run.",
    )

    export = commands.add_parser(
        "export-evidence",
        help="Export one experiment run as a report-ready evidence bundle.",
    )
    export.add_argument(
        "--run-id",
        type=uuid_value,
        help="Experiment run UUID. Defaults to the latest run with scenarios.",
    )
    export.add_argument(
        "--output-dir",
        type=Path,
        help="Bundle parent directory (default: evidence/ in the project).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        config = _effective_config(args)
        if args.command == "show-config":
            _print_config(config)
            return 0

        if args.command == "verify-accuracy":
            return _run_verify_accuracy(args, config)

        if args.command == "export-evidence":
            store = ExperimentStore(PROJECT_ROOT)
            store.prepare()
            bundle = export_evidence_bundle(
                store,
                run_id=args.run_id,
                output_dir=args.output_dir,
            )
            print(f"Evidence run_id: {bundle.run_id}")
            print(f"Evidence bundle: {bundle.path}")
            return 0

        plans = {
            "run": custom_plan,
            "rate": rate_plan,
            "late": late_ratio_plan,
            "watermark": watermark_plan,
            "partition": partition_plan,
            "checkpoint": checkpoint_plan,
        }
        plan_name = "run" if args.command == "run" else args.experiment_type
        report = ExperimentRunner(config).run(plans[plan_name](config))
    except (
        ConfigError,
        OSError,
        RuntimeError,
        ValueError,
        subprocess.SubprocessError,
    ) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    _print_report(report)
    return 0 if report.status == "passed" else 1


def _run_verify_accuracy(args: argparse.Namespace, config: AppConfig) -> int:
    """Display evidence already produced by a real experiment scenario."""
    store = ExperimentStore(PROJECT_ROOT)
    store.prepare()
    results = load_accuracy_summary(store, args.run_id, args.scenario)
    if not results:
        print("No matching experiment accuracy evidence was found.", file=sys.stderr)
        return 1

    print("\n=== Scenario Window Accuracy ===")
    print(
        f"{'Scenario':<24} {'WM(s)':>8} {'Expected':>10} {'Actual':>10} "
        f"{'Missing':>9} {'Extra':>8} {'Count%':>9} {'Late drop':>10} {'Complete':>9}"
    )
    print("-" * 115)
    for result in results:
        print(
            f"{str(result.get('scenario_name', '')):<24} "
            f"{str(result.get('watermark_delay_seconds', '-')):>8} "
            f"{result.get('total_expected_count', 0):>10} "
            f"{result.get('total_actual_count', 0):>10} "
            f"{result.get('total_missing_count', 0):>9} "
            f"{result.get('total_extra_count', 0):>8} "
            f"{str(result.get('overall_count_accuracy_percent', '-')):>9} "
            f"{result.get('total_late_events_dropped', 0):>10} "
            f"{str(bool(result.get('accuracy_complete'))):>9}"
        )
    print()
    return 0 if all(row.get("accuracy_complete") is True for row in results) else 1


def _effective_config(args: argparse.Namespace) -> AppConfig:
    config = AppConfig.load(args.config)
    if args.command in ("show-config", "verify-accuracy", "export-evidence"):
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
    if getattr(args, "watermarks", None) is not None:
        experiment_changes["watermarks"] = tuple(args.watermarks)
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
        spark_status = f", spark={result['spark_status']}" if "spark_status" in result else ""
        print(
            f"{result['scenario']}: status={result['status']}, "
            f"delivered={result['delivered']}, "
            f"rate={result['delivered_rate_events_s']}/s, "
            f"late={result['actual_late_ratio_percent']}%{spark_status}"
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
            "watermarks": config.experiments.watermarks,
            "partitions": config.experiments.partitions,
            "topic_prefix": config.experiments.topic_prefix,
            "results_dir": config.experiments.results_dir,
            "export_logs": config.experiments.export_logs,
        },
        "runtime": {
            "bootstrap_servers": config.runtime.bootstrap_servers,
            "skip_build": config.runtime.skip_build,
            "spark_startup_timeout_seconds": config.runtime.spark_startup_timeout_seconds,
            "spark_drain_timeout_seconds": config.runtime.spark_drain_timeout_seconds,
        },
    }
    print(json.dumps(values, indent=2, ensure_ascii=False))
