"""Load and validate the user-editable config.json file."""

from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import json
from pathlib import Path
from typing import Any, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config.json"


class ConfigError(ValueError):
    """Raised when config.json contains an invalid value."""


@dataclass(frozen=True)
class GeneratorConfig:
    topic: str = "auto"
    partitions: int = 3
    rate: float = 100.0
    duration_seconds: float = 60.0
    late_ratio: float = 0.10
    late_delay_seconds: float = 45.0
    account_count: int = 100
    merchant_count: int = 50
    min_amount_vnd: int = 10_000
    max_amount_vnd: int = 5_000_000
    seed: int = 42

    def __post_init__(self) -> None:
        positive = {
            "partitions": self.partitions,
            "rate": self.rate,
            "duration_seconds": self.duration_seconds,
            "account_count": self.account_count,
            "merchant_count": self.merchant_count,
            "min_amount_vnd": self.min_amount_vnd,
            "max_amount_vnd": self.max_amount_vnd,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ConfigError(f"generator.{name} must be greater than zero")
        if not 0 <= self.late_ratio <= 1:
            raise ConfigError("generator.late_ratio must be between 0 and 1")
        if self.late_delay_seconds < 0:
            raise ConfigError("generator.late_delay_seconds cannot be negative")
        if self.min_amount_vnd > self.max_amount_vnd:
            raise ConfigError(
                "generator.min_amount_vnd cannot exceed max_amount_vnd"
            )
        if self.topic != "auto" and not self.topic.strip():
            raise ConfigError("generator.topic cannot be empty")

    @property
    def resolved_topic(self) -> str:
        if self.topic != "auto":
            return self.topic
        if self.partitions == 3:
            return "raw-events"
        return f"raw-events-p{self.partitions}"

    def with_changes(self, **changes: object) -> "GeneratorConfig":
        clean = {key: value for key, value in changes.items() if value is not None}
        return replace(self, **clean)

    def as_dict(self) -> dict[str, object]:
        values = asdict(self)
        values["resolved_topic"] = self.resolved_topic
        return values


@dataclass(frozen=True)
class ExperimentConfig:
    rates: tuple[float, ...] = (10.0, 100.0, 500.0)
    late_ratios: tuple[float, ...] = (0.0, 0.10, 0.30)
    partitions: tuple[int, ...] = (1, 3, 6)
    topic_prefix: str = "raw-events-p"
    results_dir: str = "results"
    export_logs: bool = False

    def __post_init__(self) -> None:
        if not self.rates or any(value <= 0 for value in self.rates):
            raise ConfigError("experiments.rates must contain positive numbers")
        if not self.late_ratios or any(
            not 0 <= value <= 1 for value in self.late_ratios
        ):
            raise ConfigError(
                "experiments.late_ratios must contain values between 0 and 1"
            )
        if not self.partitions or any(value <= 0 for value in self.partitions):
            raise ConfigError(
                "experiments.partitions must contain positive integers"
            )
        if not self.topic_prefix:
            raise ConfigError("experiments.topic_prefix cannot be empty")

    def with_changes(self, **changes: object) -> "ExperimentConfig":
        clean = {key: value for key, value in changes.items() if value is not None}
        return replace(self, **clean)


@dataclass(frozen=True)
class RuntimeConfig:
    bootstrap_servers: str = "kafka:29092"
    skip_build: bool = False

    def __post_init__(self) -> None:
        if not self.bootstrap_servers:
            raise ConfigError("runtime.bootstrap_servers cannot be empty")


@dataclass(frozen=True)
class AppConfig:
    generator: GeneratorConfig
    experiments: ExperimentConfig
    runtime: RuntimeConfig

    @classmethod
    def load(cls, path: Path = DEFAULT_CONFIG_PATH) -> "AppConfig":
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ConfigError(f"Config file not found: {path}") from exc
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Invalid JSON in {path}: {exc}") from exc

        if not isinstance(raw, Mapping):
            raise ConfigError("The config root must be a JSON object")
        try:
            generator = GeneratorConfig(**_section(raw, "generator"))
            experiment_values = _section(raw, "experiments")
            experiments = ExperimentConfig(
                rates=tuple(experiment_values.get("rates", (10, 100, 500))),
                late_ratios=tuple(
                    experiment_values.get("late_ratios", (0, 0.1, 0.3))
                ),
                partitions=tuple(
                    experiment_values.get("partitions", (1, 3, 6))
                ),
                topic_prefix=experiment_values.get(
                    "topic_prefix", "raw-events-p"
                ),
                results_dir=experiment_values.get("results_dir", "results"),
                export_logs=experiment_values.get("export_logs", False),
            )
            runtime = RuntimeConfig(**_section(raw, "runtime"))
        except TypeError as exc:
            raise ConfigError(f"Unknown or missing config field: {exc}") from exc
        return cls(generator, experiments, runtime)

    def with_generator(self, generator: GeneratorConfig) -> "AppConfig":
        return replace(self, generator=generator)

    def with_experiments(self, experiments: ExperimentConfig) -> "AppConfig":
        return replace(self, experiments=experiments)

    def with_runtime(self, runtime: RuntimeConfig) -> "AppConfig":
        return replace(self, runtime=runtime)


def _section(raw: Mapping[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name, {})
    if not isinstance(value, Mapping):
        raise ConfigError(f"{name} must be a JSON object")
    return dict(value)
