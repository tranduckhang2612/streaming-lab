"""One runner for custom, rate, late-ratio, and partition scenarios."""

from __future__ import annotations

import csv
from contextlib import nullcontext
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterable

from .config import AppConfig, GeneratorConfig, PROJECT_ROOT
from .database import ExperimentStore
from .generator import execute, prepare_environment, timestamp_id, utc_timestamp
from .kafka import KafkaAdmin, balance_cv, partition_deltas
from .spark import SparkPipeline, SparkPipelineError


RESULT_FIELDS = (
    "scenario",
    "topic",
    "partition_count",
    "active_partitions",
    "partition_event_counts",
    "offset_event_count",
    "kafka_start_offsets",
    "kafka_end_offsets",
    "offsets_match_delivery",
    "all_partitions_active",
    "partition_balance_cv_percent",
    "started_at_utc",
    "finished_at_utc",
    "target_rate_events_s",
    "duration_seconds",
    "expected_events",
    "target_late_ratio_percent",
    "actual_late_ratio_percent",
    "late_delay_seconds",
    "seed",
    "attempted",
    "delivered",
    "failed",
    "simulated_late",
    "remaining",
    "elapsed_seconds",
    "delivered_rate_events_s",
    "rate_achievement_percent",
    "delivery_success_percent",
    "status",
    "generator_finished_at_utc",
    "spark_run_id",
    "spark_query_id",
    "spark_checkpoint_location",
    "spark_baseline_batch_id",
    "spark_final_batch_id",
    "spark_processed_offsets",
    "spark_status",
    "spark_error",
    "log_file",
)


@dataclass(frozen=True)
class Scenario:
    name: str
    generator: GeneratorConfig
    require_all_partitions: bool = False


@dataclass(frozen=True)
class ExperimentPlan:
    experiment_type: str
    scenarios: tuple[Scenario, ...]
    export_csv: bool = True


@dataclass(frozen=True)
class RunReport:
    run_id: str
    status: str
    results: tuple[dict[str, object], ...]
    csv_path: Path | None


def custom_plan(config: AppConfig) -> ExperimentPlan:
    generator = config.generator.with_changes(topic=config.generator.resolved_topic)
    return ExperimentPlan(
        "custom",
        (Scenario("custom-run", generator),),
        export_csv=False,
    )


def rate_plan(config: AppConfig) -> ExperimentPlan:
    topic = config.generator.resolved_topic
    scenarios = tuple(
        Scenario(
            _number_label("rate", rate),
            config.generator.with_changes(
                topic=topic,
                rate=rate,
                seed=config.generator.seed + index,
            ),
        )
        for index, rate in enumerate(config.experiments.rates, start=1)
    )
    return ExperimentPlan("rate", scenarios)


def late_ratio_plan(config: AppConfig) -> ExperimentPlan:
    topic = config.generator.resolved_topic
    scenarios = tuple(
        Scenario(
            _number_label("late", late_ratio * 100, "pct"),
            config.generator.with_changes(
                topic=topic,
                late_ratio=late_ratio,
                seed=config.generator.seed + index,
            ),
        )
        for index, late_ratio in enumerate(
            config.experiments.late_ratios,
            start=1,
        )
    )
    return ExperimentPlan("late_ratio", scenarios)


def partition_plan(config: AppConfig) -> ExperimentPlan:
    scenarios = tuple(
        Scenario(
            f"partitions-{count}",
            config.generator.with_changes(
                topic=f"{config.experiments.topic_prefix}{count}",
                partitions=count,
            ),
            require_all_partitions=True,
        )
        for count in dict.fromkeys(config.experiments.partitions)
    )
    return ExperimentPlan("partition", scenarios)


class ExperimentRunner:
    def __init__(
        self,
        config: AppConfig,
        project_root: Path = PROJECT_ROOT,
    ) -> None:
        self.config = config
        self.project_root = project_root
        self.kafka = KafkaAdmin(project_root)
        self.store = ExperimentStore(project_root)
        self.spark = SparkPipeline(self.store, config.runtime, project_root)

    def run(self, plan: ExperimentPlan) -> RunReport:
        prepare_environment(self.config.runtime, self.project_root)
        self.store.prepare()
        self._ensure_topics(plan.scenarios)

        managed_spark = plan.experiment_type == "partition"
        run_stamp = timestamp_id()
        run_id = self.store.create_run(
            experiment_type=plan.experiment_type,
            topic=self._run_topic(plan.scenarios),
            started_at=utc_timestamp(),
            configuration={
                "experiment_type": plan.experiment_type,
                "managed_spark": managed_spark,
                "spark_startup_timeout_seconds": self.config.runtime.spark_startup_timeout_seconds,
                "spark_drain_timeout_seconds": self.config.runtime.spark_drain_timeout_seconds,
                "scenarios": [
                    scenario.generator.as_dict() for scenario in plan.scenarios
                ],
            },
        )

        results: list[dict[str, object]] = []
        try:
            with self.spark.session() if managed_spark else nullcontext():
                for sequence, scenario in enumerate(plan.scenarios, start=1):
                    result, output = self._run_scenario(
                        scenario, run_stamp, run_id if managed_spark else None
                    )
                    results.append(result)
                    self.store.save_scenario(
                        run_id=run_id,
                        sequence_number=sequence,
                        result=result,
                        log_text=output,
                    )
        except BaseException:
            self.store.finish_run(run_id, "failed", utc_timestamp())
            raise

        status = (
            "passed"
            if results and all(row["status"] == "passed" for row in results)
            else "failed"
        )
        self.store.finish_run(run_id, status, utc_timestamp())
        csv_path = self._write_csv(plan, results, run_stamp)
        return RunReport(run_id, status, tuple(results), csv_path)

    def _ensure_topics(self, scenarios: Iterable[Scenario]) -> None:
        topics: dict[str, int] = {}
        for scenario in scenarios:
            topic = scenario.generator.resolved_topic
            count = scenario.generator.partitions
            if topic in topics and topics[topic] != count:
                raise ValueError(f"Topic {topic!r} has conflicting partition counts")
            topics[topic] = count
        for topic, count in topics.items():
            self.kafka.ensure_topic(topic, count)

    def _run_scenario(
        self,
        scenario: Scenario,
        run_stamp: str,
        experiment_run_id: str | None = None,
    ) -> tuple[dict[str, object], str]:
        generator = scenario.generator
        topic = generator.resolved_topic
        print(
            f"\n=== {scenario.name}: topic={topic}, "
            f"partitions={generator.partitions}, rate={generator.rate:g}/s, "
            f"late={generator.late_ratio:.1%} ===",
            flush=True,
        )
        before = self.kafka.offsets(topic)
        context = (self.spark.scenario(topic, experiment_run_id)
                   if experiment_run_id else nullcontext(None))
        if experiment_run_id:
            self._validate_offsets(before, generator.partitions)
        with context as pipeline:
            ready = None
            if pipeline:
                print(f"Waiting for Spark to read {topic} before producing...", flush=True)
                ready = self.spark.wait_for_offsets(pipeline, before, startup=True)
            result, output = self._produce_scenario(scenario, run_stamp, before)
            if pipeline:
                result.update(
                    spark_run_id=ready["spark_run_id"],
                    spark_query_id=ready["query_id"],
                    spark_checkpoint_location=pipeline.checkpoint,
                    spark_baseline_batch_id=ready["batch_id"],
                )
                try:
                    target = result["kafka_end_offsets"]
                    self._validate_offsets(target, generator.partitions)
                    print(f"Waiting for Spark to finish {topic}: offsets={target}...", flush=True)
                    processed = self.spark.wait_for_offsets(pipeline, target)
                    result["spark_processed_offsets"] = processed["end_offsets"]
                    result["spark_final_batch_id"] = processed["batch_id"]
                    result["spark_status"] = "passed"
                except SparkPipelineError as exc:
                    result.update(spark_status="failed", spark_error=str(exc), status="failed")
                    print(f"Spark scenario failed: {exc}", flush=True)
                result["finished_at_utc"] = utc_timestamp()
            return result, output

    @staticmethod
    def _validate_offsets(offsets: dict[int, int], partitions: int) -> None:
        if set(offsets) != set(range(partitions)) or any(v < 0 for v in offsets.values()):
            raise SparkPipelineError("Kafka did not report offsets for every expected partition.")

    def _produce_scenario(
        self, scenario: Scenario, run_stamp: str, before: dict[int, int],
    ) -> tuple[dict[str, object], str]:
        generator = scenario.generator
        topic = generator.resolved_topic
        started_at = utc_timestamp()
        log_path = self._log_path(run_stamp, scenario.name)
        execution = execute(
            generator,
            self.config.runtime,
            f"streaming-lab-{scenario.name}-{run_stamp}",
            log_path,
            self.project_root,
        )
        finished_at = utc_timestamp()
        after = self.kafka.offsets(topic)
        counts = partition_deltas(before, after, generator.partitions)
        offset_count = sum(counts.values())
        active = sum(value > 0 for value in counts.values())
        all_active = active == generator.partitions

        result: dict[str, object] = {
            "scenario": scenario.name,
            "topic": topic,
            "partition_count": generator.partitions,
            "active_partitions": active,
            "partition_event_counts": counts,
            "offset_event_count": offset_count,
            "kafka_start_offsets": before,
            "kafka_end_offsets": after,
            "all_partitions_active": all_active,
            "partition_balance_cv_percent": balance_cv(counts),
            "started_at_utc": started_at,
            "finished_at_utc": finished_at,
            "generator_finished_at_utc": finished_at,
            "target_rate_events_s": generator.rate,
            "duration_seconds": generator.duration_seconds,
            "expected_events": round(generator.rate * generator.duration_seconds),
            "target_late_ratio_percent": generator.late_ratio * 100,
            "late_delay_seconds": generator.late_delay_seconds,
            "seed": generator.seed,
            "log_file": log_path.name if log_path else "",
        }
        result.update(
            self._metrics(
                execution.return_code,
                execution.summary,
                generator.rate,
                offset_count,
                all_active,
                scenario.require_all_partitions,
            )
        )
        return result, execution.output

    @staticmethod
    def _metrics(
        return_code: int,
        summary: dict[str, str] | None,
        target_rate: float,
        offset_count: int,
        all_partitions_active: bool,
        require_all_partitions: bool,
    ) -> dict[str, object]:
        if summary is None:
            return {
                "attempted": None,
                "delivered": None,
                "failed": None,
                "simulated_late": None,
                "remaining": None,
                "elapsed_seconds": None,
                "delivered_rate_events_s": None,
                "rate_achievement_percent": None,
                "delivery_success_percent": None,
                "actual_late_ratio_percent": None,
                "offsets_match_delivery": None,
                "status": "failed-no-summary",
            }

        attempted = int(summary["attempted"])
        delivered = int(summary["delivered"])
        simulated_late = int(summary["late"])
        delivered_rate = float(summary["delivered_rate"])
        offsets_match = offset_count == delivered
        partitions_ok = all_partitions_active or not require_all_partitions
        passed = return_code == 0 and offsets_match and partitions_ok
        return {
            "attempted": attempted,
            "delivered": delivered,
            "failed": int(summary["failed"]),
            "simulated_late": simulated_late,
            "remaining": int(summary["remaining"]),
            "elapsed_seconds": float(summary["elapsed"]),
            "delivered_rate_events_s": delivered_rate,
            "rate_achievement_percent": round(
                delivered_rate / target_rate * 100,
                2,
            ),
            "delivery_success_percent": round(
                delivered / attempted * 100,
                2,
            ) if attempted else 0.0,
            "actual_late_ratio_percent": round(
                simulated_late / attempted * 100,
                2,
            ) if attempted else 0.0,
            "offsets_match_delivery": offsets_match,
            "status": "passed" if passed else "failed",
        }

    def _write_csv(
        self,
        plan: ExperimentPlan,
        results: list[dict[str, object]],
        run_stamp: str,
    ) -> Path | None:
        if not plan.export_csv:
            return None
        results_dir = Path(self.config.experiments.results_dir)
        if not results_dir.is_absolute():
            results_dir = self.project_root / results_dir
        results_dir.mkdir(parents=True, exist_ok=True)
        path = results_dir / f"{plan.experiment_type}_{run_stamp}.csv"
        with path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=RESULT_FIELDS)
            writer.writeheader()
            for result in results:
                row = {
                    key: _csv_value(result.get(key)) for key in RESULT_FIELDS
                }
                writer.writerow(row)
        return path

    def _log_path(self, run_stamp: str, scenario: str) -> Path | None:
        if not self.config.experiments.export_logs:
            return None
        results_dir = Path(self.config.experiments.results_dir)
        if not results_dir.is_absolute():
            results_dir = self.project_root / results_dir
        return results_dir / f"{run_stamp}_{scenario}.log"

    @staticmethod
    def _run_topic(scenarios: tuple[Scenario, ...]) -> str:
        topics = {scenario.generator.resolved_topic for scenario in scenarios}
        return next(iter(topics)) if len(topics) == 1 else "multiple-topics"


def _number_label(prefix: str, value: float, suffix: str = "") -> str:
    number = f"{value:g}".replace(".", "-")
    return f"{prefix}-{number}{suffix}"


def _csv_value(value: object) -> object:
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "" if value is None else value
