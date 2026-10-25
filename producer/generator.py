from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
import signal
import sys
import threading
import time
from typing import Sequence

from confluent_kafka import KafkaException, Producer

from producer.events import TransactionFactory, TransactionSettings


@dataclass
class DeliveryStats:
    attempted: int = 0
    delivered: int = 0
    failed: int = 0
    simulated_late: int = 0


class KafkaEventProducer:
    def __init__(self, bootstrap_servers: str, client_id: str) -> None:
        self.stats = DeliveryStats()
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": client_id,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 5,
                "batch.size": 65536,
            }
        )

    def wait_for_topic(self, topic: str, timeout_seconds: float) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error = "topic metadata was unavailable"

        while time.monotonic() < deadline:
            try:
                metadata = self._producer.list_topics(topic, timeout=5)
                topic_metadata = metadata.topics.get(topic)
                if topic_metadata is not None and topic_metadata.error is None:
                    return
                if topic_metadata is not None:
                    last_error = str(topic_metadata.error)
            except KafkaException as exc:
                last_error = str(exc)
            time.sleep(1)

        raise RuntimeError(
            f"Kafka topic {topic!r} was not ready after "
            f"{timeout_seconds:g}s: {last_error}"
        )

    def _delivery_callback(self, error: object, _message: object) -> None:
        if error is None:
            self.stats.delivered += 1
        else:
            self.stats.failed += 1
            print(f"Delivery failed: {error}", file=sys.stderr, flush=True)

    def send(self, topic: str, event: dict[str, object]) -> None:
        payload = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        key = str(event["account_id"]).encode("utf-8")

        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=payload,
                    on_delivery=self._delivery_callback,
                )
                break
            except BufferError:
                self._producer.poll(0.1)

        self.stats.attempted += 1
        if bool(event["simulated_late"]):
            self.stats.simulated_late += 1
        self._producer.poll(0)

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout_seconds: float = 30.0) -> int:
        remaining = self._producer.flush(timeout_seconds)
        if remaining:
            self.stats.failed += remaining
        return remaining


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
    if not 0.0 <= parsed <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate timestamped transactions and publish them to Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "raw-events"),
    )
    parser.add_argument("--rate", type=positive_float, default=10.0)
    parser.add_argument("--late-ratio", type=ratio, default=0.10)
    parser.add_argument(
        "--late-delay-seconds",
        type=non_negative_float,
        default=45.0,
    )
    parser.add_argument("--account-count", type=positive_int, default=20)
    parser.add_argument("--merchant-count", type=positive_int, default=50)
    parser.add_argument("--min-amount-vnd", type=positive_int, default=10_000)
    parser.add_argument("--max-amount-vnd", type=positive_int, default=5_000_000)
    parser.add_argument(
        "--duration-seconds",
        type=non_negative_float,
        default=60.0,
        help="Use 0 to run until interrupted.",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--log-interval-seconds", type=positive_float, default=5.0)
    parser.add_argument(
        "--connection-timeout-seconds",
        type=positive_float,
        default=60.0,
    )
    parser.add_argument("--client-id", default="streaming-lab-generator")
    return parser


def run(args: argparse.Namespace) -> int:
    factory = TransactionFactory(
        TransactionSettings(
            account_count=args.account_count,
            merchant_count=args.merchant_count,
            late_ratio=args.late_ratio,
            late_delay_seconds=args.late_delay_seconds,
            minimum_amount_vnd=args.min_amount_vnd,
            maximum_amount_vnd=args.max_amount_vnd,
        ),
        seed=args.seed,
    )
    client = KafkaEventProducer(args.bootstrap_servers, args.client_id)
    stop_event = threading.Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print(
        "Starting generator: "
        f"bootstrap={args.bootstrap_servers}, topic={args.topic}, "
        f"rate={args.rate:g}/s, late_ratio={args.late_ratio:.1%}, "
        f"late_delay={args.late_delay_seconds:g}s, seed={args.seed}",
        flush=True,
    )
    client.wait_for_topic(args.topic, args.connection_timeout_seconds)

    started_at = time.monotonic()
    next_event_at = started_at
    last_report_at = started_at
    sequence = 1

    try:
        while not stop_event.is_set():
            now = time.monotonic()
            elapsed = now - started_at
            if args.duration_seconds > 0 and elapsed >= args.duration_seconds:
                break

            if now < next_event_at:
                stop_event.wait(min(next_event_at - now, 0.1))
                client.poll()
                continue

            event = factory.create(sequence)
            client.send(args.topic, event)
            sequence += 1
            next_event_at = started_at + (sequence - 1) / args.rate

            if now - last_report_at >= args.log_interval_seconds:
                actual_rate = client.stats.attempted / max(elapsed, 0.001)
                print(
                    "Progress: "
                    f"attempted={client.stats.attempted}, "
                    f"delivered={client.stats.delivered}, "
                    f"failed={client.stats.failed}, "
                    f"late={client.stats.simulated_late}, "
                    f"actual_rate={actual_rate:.1f}/s",
                    flush=True,
                )
                last_report_at = now
    finally:
        remaining = client.flush()

    elapsed = time.monotonic() - started_at
    final_rate = client.stats.delivered / max(elapsed, 0.001)
    print(
        "Finished: "
        f"attempted={client.stats.attempted}, "
        f"delivered={client.stats.delivered}, "
        f"failed={client.stats.failed}, "
        f"late={client.stats.simulated_late}, "
        f"remaining={remaining}, elapsed={elapsed:.2f}s, "
        f"delivered_rate={final_rate:.1f}/s",
        flush=True,
    )
    return 0 if client.stats.failed == 0 and remaining == 0 else 1


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except (KafkaException, RuntimeError) as exc:
        print(f"Generator failed: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
