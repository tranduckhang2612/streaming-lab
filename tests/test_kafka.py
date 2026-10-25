import unittest

from app.kafka import (
    balance_cv,
    parse_offsets,
    parse_partition_count,
    partition_deltas,
)


class KafkaTests(unittest.TestCase):
    def test_parse_partition_count(self) -> None:
        description = "Topic: events PartitionCount: 3 ReplicationFactor: 1"
        self.assertEqual(parse_partition_count(description), 3)

    def test_offsets_and_deltas(self) -> None:
        before = parse_offsets("events:0:10\nevents:1:20\n")
        after = parse_offsets("events:0:14\nevents:1:27\n")
        self.assertEqual(partition_deltas(before, after, 2), {0: 4, 1: 7})

    def test_balance_cv(self) -> None:
        self.assertEqual(balance_cv({0: 10}), 0.0)
        self.assertIsNone(balance_cv({0: 0, 1: 0}))


if __name__ == "__main__":
    unittest.main()
