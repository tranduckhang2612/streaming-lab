"""Build and execute the Dockerized transaction generator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
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
    delivered_events: tuple[dict[str, object], ...] = ()
    manifest_sha256: str | None = None
    manifest_status: str = "unavailable"
    manifest_error: str | None = None


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
    event_manifest: Path | None = None,
) -> list[str]:
    command = [
        "docker",
        "compose",
        "--profile",
        "generator",
        "run",
        "--rm",
        "--no-deps",
        "-T",
    ]
    if event_manifest is not None:
        command.extend([
            "--volume",
            f"{event_manifest.parent.resolve()}:/tmp/streaming-lab-evidence",
        ])
    command.extend([
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
    ])
    if event_manifest is not None:
        command.extend([
            "--event-manifest",
            f"/tmp/streaming-lab-evidence/{event_manifest.name}",
        ])
    return command


def execute(
    config: GeneratorConfig,
    runtime: RuntimeConfig,
    client_id: str,
    log_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> Execution:
    with tempfile.TemporaryDirectory(prefix="streaming-lab-evidence-") as directory:
        manifest_path = Path(directory) / "delivered-events.jsonl"
        # Rootless Docker remaps container UIDs; a writable directory lets the
        # container create the file without relying on host-file ownership.
        Path(directory).chmod(0o777)
        command = build_command(
            config,
            runtime,
            client_id,
            event_manifest=manifest_path,
        )
        return_code, output = _run_and_capture(command, project_root, log_path)
        summary = parse_summary(output)
        events, digest, status, error = _read_manifest(manifest_path, summary)
        if status != "complete" and return_code == 0:
            return_code = 1
        return Execution(
            return_code,
            output,
            summary,
            tuple(events),
            digest,
            status,
            error,
        )


def _read_manifest(
    path: Path,
    summary: dict[str, str] | None,
) -> tuple[list[dict[str, object]], str | None, str, str | None]:
    try:
        raw = path.read_bytes()
        events: list[dict[str, object]] = []
        for line_number, raw_line in enumerate(raw.splitlines(), start=1):
            if not raw_line.strip():
                continue
            value = json.loads(raw_line)
            if not isinstance(value, dict):
                raise ValueError(f"manifest line {line_number} is not an object")
            events.append(value)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        return [], None, "incomplete", str(exc)

    digest = hashlib.sha256(raw).hexdigest()
    if summary is None:
        return events, digest, "incomplete", "generator summary is missing"
    delivered = int(summary["delivered"])
    if len(events) != delivered:
        return (
            events,
            digest,
            "incomplete",
            f"manifest has {len(events)} events but Kafka acknowledged {delivered}",
        )
    return events, digest, "complete", None


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
