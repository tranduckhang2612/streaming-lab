"""Build and execute the Dockerized transaction generator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
from typing import Sequence

from .config import GeneratorConfig, PROJECT_ROOT, RuntimeConfig


SUMMARY_PATTERN = re.compile(
    r"Finished: attempted=(?P<attempted>\d+), "
    r"delivered=(?P<delivered>\d+), "
    r"failed=(?P<failed>\d+), "
    r"late=(?P<late>\d+), "
    r"remaining=(?P<remaining>\d+), "
    r"elapsed=(?P<elapsed>[\d.]+)s, "
    r"delivered_rate=(?P<delivered_rate>[\d.]+)/s"
)


@dataclass(frozen=True)
class Execution:
    return_code: int
    output: str
    summary: dict[str, str] | None


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def timestamp_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def parse_summary(output: str) -> dict[str, str] | None:
    matches = list(SUMMARY_PATTERN.finditer(output))
    return matches[-1].groupdict() if matches else None


def prepare_environment(
    runtime: RuntimeConfig,
    project_root: Path = PROJECT_ROOT,
) -> None:
    if not runtime.skip_build:
        subprocess.run(
            ["docker", "compose", "build", "event-generator"],
            cwd=project_root,
            check=True,
        )
    subprocess.run(
        ["docker", "compose", "up", "-d", "--wait", "kafka", "postgres"],
        cwd=project_root,
        check=True,
    )
    subprocess.run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "--no-deps",
            "-T",
            "kafka-init",
        ],
        cwd=project_root,
        check=True,
    )


def build_command(
    config: GeneratorConfig,
    runtime: RuntimeConfig,
    client_id: str,
) -> list[str]:
    return [
        "docker",
        "compose",
        "--profile",
        "generator",
        "run",
        "--rm",
        "--no-deps",
        "-T",
        "event-generator",
        "--bootstrap-servers",
        runtime.bootstrap_servers,
        "--topic",
        config.resolved_topic,
        "--rate",
        f"{config.rate:g}",
        "--late-ratio",
        f"{config.late_ratio:g}",
        "--late-delay-seconds",
        f"{config.late_delay_seconds:g}",
        "--duration-seconds",
        f"{config.duration_seconds:g}",
        "--account-count",
        str(config.account_count),
        "--merchant-count",
        str(config.merchant_count),
        "--min-amount-vnd",
        str(config.min_amount_vnd),
        "--max-amount-vnd",
        str(config.max_amount_vnd),
        "--seed",
        str(config.seed),
        "--client-id",
        client_id,
    ]


def execute(
    config: GeneratorConfig,
    runtime: RuntimeConfig,
    client_id: str,
    log_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> Execution:
    command = build_command(config, runtime, client_id)
    return_code, output = _run_and_capture(command, project_root, log_path)
    return Execution(return_code, output, parse_summary(output))


def _run_and_capture(
    command: Sequence[str],
    project_root: Path,
    log_path: Path | None,
) -> tuple[int, str]:
    lines: list[str] = []
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("w", encoding="utf-8") if log_path else None
    try:
        process = subprocess.Popen(
            command,
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="", flush=True)
            if log_file:
                log_file.write(line)
            lines.append(line)
        return process.wait(), "".join(lines)
    finally:
        if log_file:
            log_file.close()
