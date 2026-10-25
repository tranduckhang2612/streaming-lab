from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
import uuid


def format_utc_timestamp(value: datetime) -> str:
    """Return an ISO-8601 UTC timestamp with millisecond precision."""
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True)
class TransactionSettings:
    account_count: int = 20
    merchant_count: int = 50
    late_ratio: float = 0.10
    late_delay_seconds: float = 45.0
    minimum_amount_vnd: int = 10_000
    maximum_amount_vnd: int = 5_000_000

    def __post_init__(self) -> None:
        if self.account_count <= 0:
            raise ValueError("account_count must be greater than zero")
        if self.merchant_count <= 0:
            raise ValueError("merchant_count must be greater than zero")
        if not 0.0 <= self.late_ratio <= 1.0:
            raise ValueError("late_ratio must be between 0 and 1")
        if self.late_delay_seconds < 0:
            raise ValueError("late_delay_seconds cannot be negative")
        if self.minimum_amount_vnd <= 0:
            raise ValueError("minimum_amount_vnd must be greater than zero")
        if self.minimum_amount_vnd > self.maximum_amount_vnd:
            raise ValueError(
                "minimum_amount_vnd cannot exceed maximum_amount_vnd"
            )
        if (
            self.minimum_amount_vnd % 1_000 != 0
            or self.maximum_amount_vnd % 1_000 != 0
        ):
            raise ValueError("VND amount limits must be multiples of 1000")


class TransactionFactory:
    """Create deterministic transactions when the same seed is used."""

    CATEGORIES = (
        "grocery",
        "restaurant",
        "shopping",
        "transport",
        "utilities",
    )

    def __init__(self, settings: TransactionSettings, seed: int = 42) -> None:
        self.settings = settings
        self._random = random.Random(seed)

    def create(
        self,
        sequence: int,
        produced_at: datetime | None = None,
    ) -> dict[str, object]:
        if sequence <= 0:
            raise ValueError("sequence must be greater than zero")

        produced_at = produced_at or datetime.now(timezone.utc)
        if produced_at.tzinfo is None:
            raise ValueError("produced_at must include timezone information")

        simulated_late = self._random.random() < self.settings.late_ratio
        late_delay = (
            self.settings.late_delay_seconds if simulated_late else 0.0
        )
        event_time = produced_at - timedelta(seconds=late_delay)
        account_number = self._random.randint(1, self.settings.account_count)
        merchant_number = self._random.randint(1, self.settings.merchant_count)
        category = self._random.choice(self.CATEGORIES)
        amount_vnd = 1_000 * self._random.randint(
            self.settings.minimum_amount_vnd // 1_000,
            self.settings.maximum_amount_vnd // 1_000,
        )
        transaction_id = uuid.UUID(
            int=self._random.getrandbits(128),
            version=4,
        )

        return {
            "transaction_id": str(transaction_id),
            "account_id": f"account-{account_number:03d}",
            "merchant_id": f"merchant-{merchant_number:03d}",
            "merchant_category": category,
            "amount_vnd": amount_vnd,
            "currency": "VND",
            "event_time": format_utc_timestamp(event_time),
            "produced_at": format_utc_timestamp(produced_at),
            "sequence": sequence,
            "simulated_late": simulated_late,
        }
