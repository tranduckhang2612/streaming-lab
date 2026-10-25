from datetime import datetime, timezone
import unittest

from producer.events import TransactionFactory, TransactionSettings


class TransactionFactoryTests(unittest.TestCase):
    def test_same_seed_produces_same_event(self) -> None:
        produced_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first = TransactionFactory(TransactionSettings(), seed=42).create(
            1,
            produced_at,
        )
        second = TransactionFactory(TransactionSettings(), seed=42).create(
            1,
            produced_at,
        )
        self.assertEqual(first, second)

    def test_late_event_uses_configured_delay(self) -> None:
        settings = TransactionSettings(late_ratio=1.0, late_delay_seconds=45)
        event = TransactionFactory(settings).create(
            1,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        self.assertTrue(event["simulated_late"])
        self.assertNotEqual(event["event_time"], event["produced_at"])


if __name__ == "__main__":
    unittest.main()
