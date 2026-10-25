"""Small Kafka administration API used by the experiment runner."""

from __future__ import annotations

from pathlib import Path
import re
import statistics
import subprocess
from typing import Sequence

from .config import PROJECT_ROOT


PARTITION_COUNT_PATTERN = re.compile(r"PartitionCount:\s*(\d+)")


def parse_partition_count(description: str) -> int | None:
    match = PARTITION_COUNT_PATTERN.search(description)
    return int(match.group(1)) if match else None


def parse_offsets(output: str) -> dict[int, int]:
    offsets: dict[int, int] = {}
    for line in output.splitlines():
        parts = line.strip().rsplit(":", 2)
        if len(parts) != 3:
            continue
        try:
            offsets[int(parts[1])] = int(parts[2])
        except ValueError:
            continue
    return offsets


def partition_deltas(
    before: dict[int, int],
    after: dict[int, int],
    partition_count: int,
) -> dict[int, int]:
    return {
        partition: max(0, after.get(partition, 0) - before.get(partition, 0))
        for partition in range(partition_count)
    }


def balance_cv(counts: dict[int, int]) -> float | None:
    values = list(counts.values())
    mean = statistics.fmean(values) if values else 0.0
    if mean == 0:
        return None
    return round(statistics.pstdev(values) / mean * 100, 2)


class KafkaAdmin:
    def __init__(self, project_root: Path = PROJECT_ROOT) -> None:
        self.project_root = project_root

    def ensure_topic(self, topic: str, partitions: int) -> None:
        topics_tool = "/opt/kafka/bin/kafka-topics.sh"
        self._command(
            [
                topics_tool,
                "--bootstrap-server",
                "kafka:29092",
                "--create",
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                str(partitions),
                "--replication-factor",
                "1",
            ]
        )
        description = self._command(
            [
                topics_tool,
                "--bootstrap-server",
                "kafka:29092",
                "--describe",
                "--topic",
                topic,
            ]
        )
        actual = parse_partition_count(description)
        if actual != partitions:
            raise RuntimeError(
                f"Topic {topic!r} has {actual} partitions; expected {partitions}. "
                "Use another topic name because Kafka cannot decrease partitions."
            )

    def offsets(self, topic: str) -> dict[int, int]:
        output = self._command(
            [
                "/opt/kafka/bin/kafka-get-offsets.sh",
                "--bootstrap-server",
                "kafka:29092",
                "--topic",
                topic,
                "--time",
                "-1",
            ]
        )
        return parse_offsets(output)

    def _command(self, arguments: Sequence[str]) -> str:
        completed = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka", *arguments],
            cwd=self.project_root,
            text=True,
            capture_output=True,
            check=True,
        )
        return completed.stdout
