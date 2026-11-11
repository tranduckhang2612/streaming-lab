"""Runner for generator comparisons and checkpoint-recovery evidence."""

from __future__ import annotations

import csv
from contextlib import nullcontext
from dataclasses import dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Iterable

from .accuracy import (
    GroundTruthConfig,
    compute_ground_truth,
    load_window_totals,
    save_accuracy_evidence,
)
from .config import (
    AppConfig,
    GeneratorConfig,
    PROJECT_ROOT,
    watermark_seconds_text,
)
from .database import ExperimentStore
from .generator import Execution, execute, prepare_environment, timestamp_id, utc_timestamp
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
    "watermark_delay_seconds",
    "spark_baseline_batch_id",
    "spark_final_batch_id",
    "spark_processed_offsets",
    "spark_status",
    "spark_error",
    "checkpoint_before_spark_run_id",
    "checkpoint_after_spark_run_id",
    "checkpoint_query_id_preserved",
    "checkpoint_spark_run_changed",
    "checkpoint_reused",
    "checkpoint_backlog_event_count",
    "checkpoint_offsets_recovered",
    "checkpoint_identity_verified",
    "checkpoint_no_loss_no_duplicates",
    "ground_truth_status",
    "ground_truth_manifest_sha256",
    "ground_truth_event_count",
    "ground_truth_window_count",
    "ground_truth_error",
    "log_file",
)


@dataclass(frozen=True)
class Scenario:
    name: str
    generator: GeneratorConfig
    require_all_partitions: bool = False
    watermark_delay_seconds: float | None = None
    checkpoint_recovery: bool = False

    def as_configuration(self) -> dict[str, object]:
        return {
            **self.generator.as_dict(),
            "scenario": self.name,
            "require_all_partitions": self.require_all_partitions,
            "watermark_delay_seconds": self.watermark_delay_seconds,
            "checkpoint_recovery": self.checkpoint_recovery,
        }


@dataclass(frozen=True)
class ExperimentPlan:
    experiment_type: str
    scenarios: tuple[Scenario, ...]
    export_csv: bool = True
    managed_spark: bool = False


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
    return ExperimentPlan("rate", scenarios, managed_spark=True)


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
    return ExperimentPlan("late_ratio", scenarios, managed_spark=True)


def watermark_plan(config: AppConfig) -> ExperimentPlan:
    topic = config.generator.resolved_topic
    scenarios = tuple(
        Scenario(
            f"watermark-{watermark_seconds_text(watermark).replace('.', '-')}s",
            config.generator.with_changes(topic=topic),
            watermark_delay_seconds=watermark,
        )
        for watermark in dict.fromkeys(config.experiments.watermarks)
    )
    return ExperimentPlan("watermark", scenarios, managed_spark=True)


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
    return ExperimentPlan("partition", scenarios, managed_spark=True)


def checkpoint_plan(config: AppConfig) -> ExperimentPlan:
    """Exercise one checkpoint across a real Spark stop/start boundary.

    Late events are disabled deliberately so a missing row cannot be confused
    with an event-time watermark decision. Each phase uses a different seed.
    """
    generator = config.generator.with_changes(
        topic=config.generator.resolved_topic,
        late_ratio=0,
    )
    return ExperimentPlan(
        "checkpoint",
        (Scenario("checkpoint-restart", generator, checkpoint_recovery=True),),
        managed_spark=True,
    )


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

        run_stamp = timestamp_id()
        run_id = self.store.create_run(
            experiment_type=plan.experiment_type,
            topic=self._run_topic(plan.scenarios),
            started_at=utc_timestamp(),
            configuration={
                "experiment_type": plan.experiment_type,
                "managed_spark": plan.managed_spark,
                "spark_startup_timeout_seconds": self.config.runtime.spark_startup_timeout_seconds,
                "spark_drain_timeout_seconds": self.config.runtime.spark_drain_timeout_seconds,
                "scenarios": [
                    scenario.as_configuration() for scenario in plan.scenarios
                ],
            },
        )

        results: list[dict[str, object]] = []
        try:
            with self.spark.session() if plan.managed_spark else nullcontext():
                for sequence, scenario in enumerate(plan.scenarios, start=1):
                    result, output, evidence = self._run_scenario(
                        scenario, run_stamp, run_id if plan.managed_spark else None
                    )
                    if evidence is not None:
                        evidence_config, events, digest = evidence
                        try:
                            result["ground_truth_window_count"] = save_accuracy_evidence(
                                self.store,
                                evidence_config,
                                events,
                                digest,
                            )
                            if scenario.checkpoint_recovery:
                                totals = load_window_totals(
                                    self.store,
                                    run_id,
                                    scenario.name,
                                )
                                no_loss = bool(
                                    totals
                                    and totals["total_expected_count"]
                                        == totals["total_actual_count"]
                                    and totals["total_missing_count"] == 0
                                    and totals["total_extra_count"] == 0
                                )
                                result["checkpoint_accuracy_totals"] = totals
                                result["checkpoint_no_loss_no_duplicates"] = no_loss
                                recovery = dict(result["checkpoint_recovery"])
                                recovery["no_loss_no_duplicates"] = no_loss
                                result["checkpoint_recovery"] = recovery
                                if not no_loss:
                                    result.update(
                                        status="failed",
                                        spark_error=(
                                            "checkpoint resumed but accuracy found "
                                            "missing or duplicate events"
                                        ),
                                    )
                        except Exception as exc:
                            result.update(
                                ground_truth_status="incomplete",
                                ground_truth_error=f"database evidence write failed: {exc}",
                                status="failed",
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
    ) -> tuple[
        dict[str, object],
        str,
        tuple[GroundTruthConfig, tuple[dict[str, object], ...], str] | None,
    ]:
        if scenario.checkpoint_recovery:
            if experiment_run_id is None:
                raise SparkPipelineError(
                    "Checkpoint recovery requires a managed Spark experiment."
                )
            return self._run_checkpoint_recovery(
                scenario,
                run_stamp,
                experiment_run_id,
            )

        generator = scenario.generator
        topic = generator.resolved_topic
        watermark = (
            f", watermark={scenario.watermark_delay_seconds:g}s"
            if scenario.watermark_delay_seconds is not None
            else ""
        )
        print(
            f"\n=== {scenario.name}: topic={topic}, "
            f"partitions={generator.partitions}, rate={generator.rate:g}/s, "
            f"late={generator.late_ratio:.1%}{watermark} ===",
            flush=True,
        )
        before = self.kafka.offsets(topic)
        context = (self.spark.scenario(
                       topic,
                       experiment_run_id,
                       scenario_name=scenario.name,
                       watermark_delay_seconds=scenario.watermark_delay_seconds,
                   )
                   if experiment_run_id else nullcontext(None))
        if experiment_run_id:
            self._validate_offsets(before, generator.partitions)
        result: dict[str, object] | None = None
        execution = None
        output = ""
        try:
            with context as pipeline:
                ready = None
                if pipeline:
                    print(f"Waiting for Spark to read {topic} before producing...", flush=True)
                    ready = self.spark.wait_for_offsets(pipeline, before, startup=True)
                    self._validate_watermark(scenario, ready)
                result, output, execution = self._produce_scenario(
                    scenario,
                    run_stamp,
                    before,
                )
                if pipeline:
                    result.update(
                        spark_run_id=ready["spark_run_id"],
                        spark_query_id=ready["query_id"],
                        spark_checkpoint_location=pipeline.checkpoint,
                        watermark_delay_seconds=ready["watermark_delay_seconds"],
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
                        result.update(
                            spark_status="failed",
                            spark_error=str(exc),
                            status="failed",
                        )
                        print(f"Spark scenario failed: {exc}", flush=True)
                    result["finished_at_utc"] = utc_timestamp()
        except SparkPipelineError as exc:
            # Preserve the generator result and Spark IDs if cleanup discovers
            # a crashed driver after the scenario already produced its events.
            if result is None:
                raise
            previous = result.get("spark_error")
            detail = str(exc)
            if previous and detail not in str(previous):
                detail = f"{previous}; cleanup: {detail}"
            result.update(
                spark_status="failed",
                spark_error=detail,
                status="failed",
                finished_at_utc=utc_timestamp(),
            )
            print(f"Spark scenario failed during cleanup: {exc}", flush=True)
        assert result is not None
        evidence = None
        if pipeline is not None and execution is not None:
            result.update(
                ground_truth_status=execution.manifest_status,
                ground_truth_manifest_sha256=execution.manifest_sha256,
                ground_truth_event_count=len(execution.delivered_events),
                ground_truth_window_count=0,
                ground_truth_error=execution.manifest_error,
            )
            if execution.manifest_status == "incomplete":
                result["status"] = "failed"
            elif (
                execution.manifest_status == "complete"
                and execution.manifest_sha256 is not None
            ):
                try:
                    window_duration = float(ready["window_duration_seconds"])
                    watermark_delay = float(ready["watermark_delay_seconds"])
                    evidence_config = GroundTruthConfig(
                        run_id=experiment_run_id,
                        scenario_name=scenario.name,
                        topic=topic,
                        watermark_delay_seconds=watermark_delay,
                        window_duration_seconds=window_duration,
                    )
                    result["ground_truth_window_count"] = len(
                        compute_ground_truth(
                            evidence_config,
                            execution.delivered_events,
                        )
                    )
                    evidence = (
                        evidence_config,
                        execution.delivered_events,
                        execution.manifest_sha256,
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    result.update(
                        ground_truth_status="incomplete",
                        ground_truth_error=f"ground truth calculation failed: {exc}",
                        status="failed",
                    )
        else:
            result.update(
                ground_truth_status="unavailable",
                ground_truth_manifest_sha256=None,
                ground_truth_event_count=None,
                ground_truth_window_count=None,
                ground_truth_error=None,
            )
        return result, output, evidence

    def _run_checkpoint_recovery(
        self,
        scenario: Scenario,
        run_stamp: str,
        experiment_run_id: str,
    ) -> tuple[
        dict[str, object],
        str,
        tuple[GroundTruthConfig, tuple[dict[str, object], ...], str] | None,
    ]:
        """Produce before and during downtime, then resume one checkpoint."""
        generator = scenario.generator
        topic = generator.resolved_topic
        initial_offsets = self.kafka.offsets(topic)
        self._validate_offsets(initial_offsets, generator.partitions)
        started_at = utc_timestamp()
        print(
            f"\n=== {scenario.name}: checkpoint stop/restart, topic={topic}, "
            f"partitions={generator.partitions}, rate={generator.rate:g}/s ===",
            flush=True,
        )

        before_phase = Scenario(
            f"{scenario.name}-before-stop",
            generator,
        )
        with self.spark.scenario(
            topic,
            experiment_run_id,
            scenario_name=scenario.name,
        ) as before_pipeline:
            print("Waiting for the first Spark run before producing...", flush=True)
            before_ready = self.spark.wait_for_offsets(
                before_pipeline,
                initial_offsets,
                startup=True,
            )
            before_result, before_output, before_execution = self._produce_scenario(
                before_phase,
                run_stamp,
                initial_offsets,
            )
            before_stop_offsets = before_result["kafka_end_offsets"]
            print(
                f"Draining the first Spark run to offsets={before_stop_offsets}...",
                flush=True,
            )
            before_processed = self.spark.wait_for_offsets(
                before_pipeline,
                before_stop_offsets,
            )
            checkpoint_location = before_pipeline.checkpoint

        downtime_generator = generator.with_changes(seed=generator.seed + 1)
        downtime_phase = Scenario(
            f"{scenario.name}-while-stopped",
            downtime_generator,
        )
        print("Spark is stopped; producing the recovery backlog...", flush=True)
        downtime_result, downtime_output, downtime_execution = self._produce_scenario(
            downtime_phase,
            run_stamp,
            before_stop_offsets,
        )
        downtime_end_offsets = downtime_result["kafka_end_offsets"]

        print("Restarting Spark from the same checkpoint...", flush=True)
        with self.spark.scenario(
            topic,
            experiment_run_id,
            scenario_name=scenario.name,
            checkpoint_location=checkpoint_location,
        ) as after_pipeline:
            after_ready = self.spark.wait_for_offsets(
                after_pipeline,
                before_stop_offsets,
                startup=True,
            )
            after_processed = self.spark.wait_for_offsets(
                after_pipeline,
                downtime_end_offsets,
            )

        before_run_id = str(before_processed["spark_run_id"])
        after_run_id = str(after_processed["spark_run_id"])
        before_query_id = str(before_processed["query_id"])
        after_query_id = str(after_processed["query_id"])
        query_id_preserved = before_query_id == after_query_id
        spark_run_changed = before_run_id != after_run_id
        checkpoint_reused = checkpoint_location == after_pipeline.checkpoint
        recovered_offsets = {
            int(partition): int(offset)
            for partition, offset in after_processed["end_offsets"].items()
        }
        target_offsets = {
            int(partition): int(offset)
            for partition, offset in downtime_end_offsets.items()
        }
        offsets_recovered = self._offsets_reached(
            recovered_offsets,
            target_offsets,
        )
        backlog_counts = partition_deltas(
            before_stop_offsets,
            downtime_end_offsets,
            generator.partitions,
        )
        backlog_event_count = sum(backlog_counts.values())
        recovery_verified = all((
            query_id_preserved,
            spark_run_changed,
            checkpoint_reused,
            offsets_recovered,
            backlog_event_count > 0,
        ))

        execution = self._combine_executions(
            before_execution,
            downtime_execution,
        )
        final_offsets = {
            int(partition): int(offset)
            for partition, offset in downtime_end_offsets.items()
        }
        total_counts = partition_deltas(
            initial_offsets,
            final_offsets,
            generator.partitions,
        )
        offset_event_count = sum(total_counts.values())
        active_partitions = sum(count > 0 for count in total_counts.values())
        all_partitions_active = active_partitions == generator.partitions
        combined_metrics = self._metrics(
            execution.return_code,
            execution.summary,
            generator.rate,
            offset_event_count,
            all_partitions_active,
            scenario.require_all_partitions,
        )
        if not recovery_verified:
            combined_metrics["status"] = "failed"

        finished_at = utc_timestamp()
        result: dict[str, object] = {
            "scenario": scenario.name,
            "topic": topic,
            "partition_count": generator.partitions,
            "active_partitions": active_partitions,
            "partition_event_counts": total_counts,
            "offset_event_count": offset_event_count,
            "kafka_start_offsets": initial_offsets,
            "kafka_end_offsets": final_offsets,
            "all_partitions_active": all_partitions_active,
            "partition_balance_cv_percent": balance_cv(total_counts),
            "started_at_utc": started_at,
            "finished_at_utc": finished_at,
            "generator_finished_at_utc": downtime_result["finished_at_utc"],
            "target_rate_events_s": generator.rate,
            "duration_seconds": generator.duration_seconds * 2,
            "expected_events": round(generator.rate * generator.duration_seconds) * 2,
            "target_late_ratio_percent": 0,
            "late_delay_seconds": generator.late_delay_seconds,
            "seed": generator.seed,
            "log_file": "",
            "spark_run_id": after_run_id,
            "spark_query_id": after_query_id,
            "spark_checkpoint_location": after_pipeline.checkpoint,
            "watermark_delay_seconds": after_processed["watermark_delay_seconds"],
            # The second Spark run is new, so include every committed batch from it.
            "spark_baseline_batch_id": -1,
            "spark_final_batch_id": after_processed["batch_id"],
            "spark_processed_offsets": recovered_offsets,
            "spark_status": "passed" if offsets_recovered else "failed",
            "spark_error": None if recovery_verified else (
                "checkpoint identity or backlog recovery invariant failed"
            ),
            "checkpoint_before_spark_run_id": before_run_id,
            "checkpoint_after_spark_run_id": after_run_id,
            "checkpoint_before_query_id": before_query_id,
            "checkpoint_after_query_id": after_query_id,
            "checkpoint_query_id_preserved": query_id_preserved,
            "checkpoint_spark_run_changed": spark_run_changed,
            "checkpoint_reused": checkpoint_reused,
            "checkpoint_before_stop_offsets": before_stop_offsets,
            "checkpoint_downtime_end_offsets": downtime_end_offsets,
            "checkpoint_recovered_offsets": recovered_offsets,
            "checkpoint_backlog_event_count": backlog_event_count,
            "checkpoint_offsets_recovered": offsets_recovered,
            "checkpoint_identity_verified": recovery_verified,
            "checkpoint_phase1_delivered": before_result.get("delivered"),
            "checkpoint_downtime_delivered": downtime_result.get("delivered"),
        }
        result.update(combined_metrics)
        result["checkpoint_recovery"] = {
            "before_spark_run_id": before_run_id,
            "after_spark_run_id": after_run_id,
            "before_query_id": before_query_id,
            "after_query_id": after_query_id,
            "checkpoint_location": after_pipeline.checkpoint,
            "before_stop_offsets": before_stop_offsets,
            "downtime_end_offsets": downtime_end_offsets,
            "recovered_offsets": recovered_offsets,
            "backlog_event_count": backlog_event_count,
            "phase1_delivered": before_result.get("delivered"),
            "downtime_delivered": downtime_result.get("delivered"),
            "query_id_preserved": query_id_preserved,
            "spark_run_changed": spark_run_changed,
            "checkpoint_reused": checkpoint_reused,
            "offsets_recovered": offsets_recovered,
            "identity_status": "passed" if recovery_verified else "failed",
        }

        result.update(
            ground_truth_status=execution.manifest_status,
            ground_truth_manifest_sha256=execution.manifest_sha256,
            ground_truth_event_count=len(execution.delivered_events),
            ground_truth_window_count=0,
            ground_truth_error=execution.manifest_error,
        )
        evidence = None
        if execution.manifest_status != "complete":
            result["status"] = "failed"
        elif execution.manifest_sha256 is not None:
            try:
                evidence_config = GroundTruthConfig(
                    run_id=experiment_run_id,
                    scenario_name=scenario.name,
                    topic=topic,
                    watermark_delay_seconds=float(
                        after_processed["watermark_delay_seconds"]
                    ),
                    window_duration_seconds=float(
                        after_processed["window_duration_seconds"]
                    ),
                )
                result["ground_truth_window_count"] = len(
                    compute_ground_truth(
                        evidence_config,
                        execution.delivered_events,
                    )
                )
                evidence = (
                    evidence_config,
                    execution.delivered_events,
                    execution.manifest_sha256,
                )
            except (KeyError, TypeError, ValueError) as exc:
                result.update(
                    ground_truth_status="incomplete",
                    ground_truth_error=f"ground truth calculation failed: {exc}",
                    status="failed",
                )

        output = (
            "=== before Spark stop ===\n" + before_output
            + "\n=== while Spark stopped ===\n" + downtime_output
        )
        return result, output, evidence

    @staticmethod
    def _offsets_reached(
        observed: dict[int, int],
        target: dict[int, int],
    ) -> bool:
        return set(observed) == set(target) and all(
            observed[partition] >= offset
            for partition, offset in target.items()
        )

    @staticmethod
    def _combine_executions(first: Execution, second: Execution) -> Execution:
        summaries = (first.summary, second.summary)
        if any(summary is None for summary in summaries):
            summary = None
        else:
            complete = [summary for summary in summaries if summary is not None]
            elapsed = sum(float(summary["elapsed"]) for summary in complete)
            delivered = sum(int(summary["delivered"]) for summary in complete)
            summary = {
                "attempted": str(sum(int(item["attempted"]) for item in complete)),
                "delivered": str(delivered),
                "failed": str(sum(int(item["failed"]) for item in complete)),
                "late": str(sum(int(item["late"]) for item in complete)),
                "remaining": str(sum(int(item["remaining"]) for item in complete)),
                "elapsed": f"{elapsed:.3f}",
                "delivered_rate": f"{(delivered / elapsed if elapsed else 0):.3f}",
            }

        events = first.delivered_events + second.delivered_events
        manifest_bytes = b"".join(
            json.dumps(event, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + b"\n"
            for event in events
        )
        manifest_status = (
            "complete"
            if first.manifest_status == second.manifest_status == "complete"
            else "incomplete"
        )
        errors = [
            error for error in (first.manifest_error, second.manifest_error) if error
        ]
        return Execution(
            max(first.return_code, second.return_code),
            first.output + "\n" + second.output,
            summary,
            events,
            hashlib.sha256(manifest_bytes).hexdigest() if manifest_status == "complete" else None,
            manifest_status,
            "; ".join(errors) if errors else None,
        )

    @staticmethod
    def _validate_watermark(
        scenario: Scenario,
        snapshot: dict[str, object],
    ) -> None:
        expected = scenario.watermark_delay_seconds
        if expected is None:
            return
        actual = snapshot.get("watermark_delay_seconds")
        try:
            matches = actual is not None and math.isclose(
                float(actual),
                float(expected),
                rel_tol=0,
                abs_tol=0.0005,
            )
        except (TypeError, ValueError):
            matches = False
        if not matches:
            raise SparkPipelineError(
                f"Spark started with watermark={actual!r}s; "
                f"scenario requires {expected:g}s."
            )

    @staticmethod
    def _validate_offsets(offsets: dict[int, int], partitions: int) -> None:
        if set(offsets) != set(range(partitions)) or any(v < 0 for v in offsets.values()):
            raise SparkPipelineError("Kafka did not report offsets for every expected partition.")

    def _produce_scenario(
        self, scenario: Scenario, run_stamp: str, before: dict[int, int],
    ) -> tuple[dict[str, object], str, Execution]:
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
        if execution.manifest_status == "incomplete":
            result["status"] = "failed"
        return result, execution.output, execution

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
                simulated_late / delivered * 100,
                2,
            ) if delivered else 0.0,
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
