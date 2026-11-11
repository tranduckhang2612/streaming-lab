"""Scenario-scoped evidence for window-result accuracy.

Ground truth is built from the exact events acknowledged by Kafka. Spark
stores its actual aggregates under the same experiment run and scenario, so
verification never relies on regenerated timestamps or a shared topic table.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
import json
from typing import Iterable, Mapping

from .database import ExperimentStore, sql_literal


@dataclass(frozen=True)
class GroundTruthConfig:
    run_id: str
    scenario_name: str
    topic: str
    watermark_delay_seconds: float
    window_duration_seconds: float


@dataclass(frozen=True)
class WindowKey:
    topic: str
    window_start: datetime
    window_end: datetime
    merchant_category: str


@dataclass
class WindowBucket:
    """All delivered events in one event-time window and category."""

    expected_count: int = 0
    expected_total_amount_vnd: int = 0
    expected_amounts: list[int] = field(default_factory=list)
    expected_simulated_late_count: int = 0
    expected_lateness_ms_sum: Decimal = Decimal(0)


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"timestamp must be a string, got {type(value).__name__}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone information")
    return parsed.astimezone(timezone.utc)


def _window_start(event_time: datetime, window_seconds: float) -> datetime:
    """Return the epoch-aligned Spark tumbling-window start without float drift."""
    duration_us = int(
        (Decimal(str(window_seconds)) * Decimal(1_000_000)).to_integral_exact()
    )
    if duration_us <= 0:
        raise ValueError("window_duration_seconds must be greater than zero")
    event_time = event_time.astimezone(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = event_time - epoch
    elapsed_us = (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
    start_us = (elapsed_us // duration_us) * duration_us
    return epoch + timedelta(microseconds=start_us)


def compute_ground_truth(
    config: GroundTruthConfig,
    events: Iterable[Mapping[str, object]],
) -> dict[WindowKey, WindowBucket]:
    """Aggregate every Kafka-acknowledged event as the reference result.

    Watermark behavior is deliberately not predicted here. Any event missing
    from Spark's scenario-scoped actual output becomes a measurable loss.
    """
    buckets: dict[WindowKey, WindowBucket] = defaultdict(WindowBucket)
    duration = timedelta(seconds=config.window_duration_seconds)

    for event in events:
        event_time = _parse_timestamp(event.get("event_time"))
        produced_at = _parse_timestamp(event.get("produced_at"))
        category = event.get("merchant_category")
        amount = event.get("amount_vnd")
        simulated_late = event.get("simulated_late")
        if not isinstance(category, str) or not category:
            raise ValueError("merchant_category must be a non-empty string")
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise ValueError("amount_vnd must be an integer")
        if not isinstance(simulated_late, bool):
            raise ValueError("simulated_late must be a boolean")

        window_start = _window_start(event_time, config.window_duration_seconds)
        key = WindowKey(
            config.topic,
            window_start,
            window_start + duration,
            category,
        )
        bucket = buckets[key]
        bucket.expected_count += 1
        bucket.expected_total_amount_vnd += amount
        bucket.expected_amounts.append(amount)
        bucket.expected_simulated_late_count += int(simulated_late)
        bucket.expected_lateness_ms_sum += (
            Decimal(str((produced_at - event_time).total_seconds())) * Decimal(1000)
        )

    return dict(buckets)


def _rounded_average(total: Decimal, count: int) -> Decimal:
    if count == 0:
        return Decimal(0)
    return (total / Decimal(count)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def save_accuracy_evidence(
    store: ExperimentStore,
    config: GroundTruthConfig,
    events: tuple[dict[str, object], ...],
    manifest_sha256: str,
) -> int:
    """Replace one scenario's raw evidence and ground-truth aggregates atomically."""
    buckets = compute_ground_truth(config, events)
    event_values: list[str] = []
    for event in events:
        event_time = _parse_timestamp(event.get("event_time"))
        produced_at = _parse_timestamp(event.get("produced_at"))
        lateness_ms = int((produced_at - event_time).total_seconds() * 1000)
        event_values.append(
            "(" + ", ".join([
                f"{sql_literal(config.run_id)}::UUID",
                sql_literal(config.scenario_name),
                sql_literal(config.topic),
                f"{sql_literal(event.get('transaction_id'))}::UUID",
                sql_literal(event.get("account_id")),
                sql_literal(event.get("merchant_id")),
                sql_literal(event.get("merchant_category")),
                sql_literal(event.get("amount_vnd")),
                sql_literal(event.get("currency")),
                f"{sql_literal(event.get('event_time'))}::TIMESTAMPTZ",
                f"{sql_literal(event.get('produced_at'))}::TIMESTAMPTZ",
                sql_literal(event.get("sequence")),
                sql_literal(event.get("simulated_late")),
                sql_literal(lateness_ms),
                sql_literal(manifest_sha256),
            ]) + ")"
        )

    ground_values: list[str] = []
    for key, bucket in buckets.items():
        average_amount = _rounded_average(
            Decimal(bucket.expected_total_amount_vnd),
            bucket.expected_count,
        )
        average_lateness = _rounded_average(
            bucket.expected_lateness_ms_sum,
            bucket.expected_count,
        )
        ground_values.append(
            "(" + ", ".join([
                f"{sql_literal(config.run_id)}::UUID",
                sql_literal(config.scenario_name),
                sql_literal(key.topic),
                f"{sql_literal(key.window_start.isoformat())}::TIMESTAMPTZ",
                f"{sql_literal(key.window_end.isoformat())}::TIMESTAMPTZ",
                sql_literal(key.merchant_category),
                sql_literal(config.watermark_delay_seconds),
                sql_literal(config.window_duration_seconds),
                sql_literal(bucket.expected_count),
                sql_literal(bucket.expected_total_amount_vnd),
                sql_literal(average_amount),
                sql_literal(min(bucket.expected_amounts)),
                sql_literal(max(bucket.expected_amounts)),
                sql_literal(bucket.expected_simulated_late_count),
                sql_literal(average_lateness),
                sql_literal(manifest_sha256),
            ]) + ")"
        )

    event_insert = ""
    if event_values:
        event_insert = """
INSERT INTO streaming.experiment_verification_events (
    experiment_run_id, scenario_name, topic, transaction_id,
    account_id, merchant_id, merchant_category, amount_vnd, currency,
    event_time, produced_at, sequence, simulated_late, event_lateness_ms,
    manifest_sha256
) VALUES
""" + ",\n".join(event_values) + ";"

    ground_insert = ""
    if ground_values:
        ground_insert = """
INSERT INTO streaming.experiment_window_ground_truth (
    experiment_run_id, scenario_name, topic, window_start, window_end,
    merchant_category, watermark_delay_seconds, window_duration_seconds,
    expected_count, expected_total_amount_vnd, expected_average_amount_vnd,
    expected_min_amount_vnd, expected_max_amount_vnd,
    expected_simulated_late_count, expected_average_event_lateness_ms,
    manifest_sha256
) VALUES
""" + ",\n".join(ground_values) + ";"

    store._execute(f"""
BEGIN;
DELETE FROM streaming.experiment_verification_events
WHERE experiment_run_id = {sql_literal(config.run_id)}::UUID
  AND scenario_name = {sql_literal(config.scenario_name)};
DELETE FROM streaming.experiment_window_ground_truth
WHERE experiment_run_id = {sql_literal(config.run_id)}::UUID
  AND scenario_name = {sql_literal(config.scenario_name)};
{event_insert}
{ground_insert}
COMMIT;
""")
    return len(buckets)


def load_accuracy_summary(
    store: ExperimentStore,
    run_id: str | None = None,
    scenario_name: str | None = None,
) -> list[dict[str, object]]:
    """Read one real experiment's accuracy summary; default to the latest run."""
    if run_id is None:
        run_filter = """
s.experiment_run_id = (
    SELECT run_id
    FROM streaming.experiment_runs r
    WHERE EXISTS (
        SELECT 1 FROM streaming.experiment_scenarios x
        WHERE x.run_id = r.run_id AND x.ground_truth_status <> 'unavailable'
    )
    ORDER BY r.started_at DESC
    LIMIT 1
)
"""
    else:
        run_filter = f"s.experiment_run_id = {sql_literal(run_id)}::UUID"
    scenario_filter = (
        f" AND s.scenario_name = {sql_literal(scenario_name)}"
        if scenario_name is not None
        else ""
    )
    output = store._execute(f"""
SELECT COALESCE(json_agg(row_to_json(q) ORDER BY q.sequence_number), '[]'::JSON)
FROM (
    SELECT *
    FROM streaming.experiment_scenario_accuracy s
    WHERE {run_filter}{scenario_filter}
) q;
""")
    value = json.loads(output or "[]")
    return value if isinstance(value, list) else []


def load_window_totals(
    store: ExperimentStore,
    run_id: str,
    scenario_name: str,
) -> dict[str, int] | None:
    """Read expected/actual totals before the scenario row is finalized."""
    output = store._execute(f"""
SELECT row_to_json(t)
FROM (
    SELECT
        COALESCE(SUM(expected_count), 0)::BIGINT AS total_expected_count,
        COALESCE(SUM(actual_count), 0)::BIGINT AS total_actual_count,
        COALESCE(SUM(missing_count), 0)::BIGINT AS total_missing_count,
        COALESCE(SUM(extra_count), 0)::BIGINT AS total_extra_count
    FROM streaming.window_accuracy_results
    WHERE experiment_run_id = {sql_literal(run_id)}::UUID
      AND scenario_name = {sql_literal(scenario_name)}
) t;
""")
    value = json.loads(output) if output else None
    return value if isinstance(value, dict) else None
