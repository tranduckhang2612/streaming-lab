"""Unit tests for scenario-scoped window accuracy evidence."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import unittest
from unittest.mock import MagicMock

from app.accuracy import (
    GroundTruthConfig,
    _window_start,
    compute_ground_truth,
    load_accuracy_summary,
    load_window_totals,
    save_accuracy_evidence,
)


RUN_ID = "11111111-1111-4111-8111-111111111111"


def config(**changes) -> GroundTruthConfig:
    values = dict(
        run_id=RUN_ID,
        scenario_name="watermark-10s",
        topic="raw-events",
        watermark_delay_seconds=10,
        window_duration_seconds=60,
    )
    values.update(changes)
    return GroundTruthConfig(**values)


def event(
    sequence: int,
    event_time: str,
    *,
    produced_at: str | None = None,
    amount: int = 1000,
    late: bool = False,
    category: str = "grocery",
) -> dict[str, object]:
    return {
        "transaction_id": f"00000000-0000-4000-8000-{sequence:012d}",
        "account_id": "account-001",
        "merchant_id": "merchant-001",
        "merchant_category": category,
        "amount_vnd": amount,
        "currency": "VND",
        "event_time": event_time,
        "produced_at": produced_at or event_time,
        "sequence": sequence,
        "simulated_late": late,
    }


class AccuracyTests(unittest.TestCase):
    def test_window_start_is_epoch_aligned(self) -> None:
        value = datetime(2026, 1, 1, 0, 0, 59, 999000, tzinfo=timezone.utc)
        self.assertEqual(
            _window_start(value, 60),
            datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
        )

    def test_ground_truth_uses_exact_manifest_timestamps(self) -> None:
        events = (
            event(1, "2026-01-01T00:00:59.999Z", amount=1000),
            event(2, "2026-01-01T00:01:00.000Z", amount=3000),
        )
        buckets = compute_ground_truth(config(), events)
        self.assertEqual(len(buckets), 2)
        self.assertEqual(
            sorted(bucket.expected_total_amount_vnd for bucket in buckets.values()),
            [1000, 3000],
        )

    def test_late_events_remain_in_reference_even_beyond_watermark(self) -> None:
        events = (
            event(
                1,
                "2026-01-01T00:00:00.000Z",
                produced_at="2026-01-01T00:10:00.000Z",
                late=True,
            ),
        )
        buckets = compute_ground_truth(
            config(watermark_delay_seconds=1),
            events,
        )
        bucket = next(iter(buckets.values()))
        self.assertEqual(bucket.expected_count, 1)
        self.assertEqual(bucket.expected_simulated_late_count, 1)

    def test_save_evidence_is_scoped_and_idempotent(self) -> None:
        store = MagicMock()
        events = (
            event(1, "2026-01-01T00:00:01.000Z"),
            event(2, "2026-01-01T00:00:02.000Z", late=True),
        )
        windows = save_accuracy_evidence(store, config(), events, "a" * 64)
        self.assertEqual(windows, 1)
        sql = store._execute.call_args.args[0]
        self.assertIn("DELETE FROM streaming.experiment_verification_events", sql)
        self.assertIn("INSERT INTO streaming.experiment_window_ground_truth", sql)
        self.assertIn(RUN_ID, sql)
        self.assertIn("watermark-10s", sql)

    def test_load_summary_uses_run_and_scenario(self) -> None:
        store = MagicMock()
        store._execute.return_value = json.dumps([
            {"scenario_name": "watermark-10s", "accuracy_complete": True}
        ])
        result = load_accuracy_summary(store, RUN_ID, "watermark-10s")
        self.assertTrue(result[0]["accuracy_complete"])
        sql = store._execute.call_args.args[0]
        self.assertIn(RUN_ID, sql)
        self.assertIn("watermark-10s", sql)

    def test_invalid_manifest_event_is_rejected(self) -> None:
        bad = event(1, "2026-01-01T00:00:00.000Z")
        bad["amount_vnd"] = True
        with self.assertRaisesRegex(ValueError, "amount_vnd"):
            compute_ground_truth(config(), (bad,))

    def test_load_window_totals_does_not_require_scenario_summary(self) -> None:
        store = MagicMock()
        store._execute.return_value = json.dumps({
            "total_expected_count": 20,
            "total_actual_count": 20,
            "total_missing_count": 0,
            "total_extra_count": 0,
        })
        totals = load_window_totals(store, RUN_ID, "checkpoint-restart")
        self.assertEqual(totals["total_expected_count"], 20)
        self.assertIn("streaming.window_accuracy_results", store._execute.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
