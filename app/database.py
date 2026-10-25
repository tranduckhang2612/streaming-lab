"""Persistence for experiment metadata, metrics, and raw logs."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
from typing import Mapping
import uuid

from .config import PROJECT_ROOT


def _read_dotenv(project_root: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    env_path = project_root / ".env"
    if not env_path.exists():
        return values
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def sql_literal(value: object) -> str:
    if value is None or value == "":
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("\x00", "").replace("'", "''")
    return f"'{escaped}'"


class ExperimentStore:
    def __init__(self, project_root: Path = PROJECT_ROOT) -> None:
        self.project_root = project_root
        dotenv = _read_dotenv(project_root)
        self.database = os.getenv(
            "POSTGRES_DB", dotenv.get("POSTGRES_DB", "streaming_db")
        )
        self.user = os.getenv(
            "POSTGRES_USER", dotenv.get("POSTGRES_USER", "streaming_user")
        )
        self.schema_file = (
            project_root / "database/postgres/init/streaming_schema.sql"
        )

    def prepare(self) -> None:
        self._execute(self.schema_file.read_text(encoding="utf-8"))

    def create_run(
        self,
        experiment_type: str,
        topic: str,
        started_at: str,
        configuration: Mapping[str, object],
    ) -> str:
        run_id = str(uuid.uuid4())
        configuration_json = json.dumps(
            configuration,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        self._execute(
            "INSERT INTO streaming.experiment_runs ("
            "run_id, experiment_type, topic, started_at, status, configuration"
            ") VALUES ("
            f"{sql_literal(run_id)}::UUID, "
            f"{sql_literal(experiment_type)}, "
            f"{sql_literal(topic)}, "
            f"{sql_literal(started_at)}::TIMESTAMPTZ, "
            "'running', "
            f"{sql_literal(configuration_json)}::JSONB"
            ");"
        )
        return run_id

    def save_scenario(
        self,
        *,
        run_id: str,
        sequence_number: int,
        result: Mapping[str, object],
        log_text: str,
    ) -> None:
        result_json = json.dumps(
            result,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        partition_counts = result.get("partition_event_counts")
        partition_counts_json = (
            json.dumps(partition_counts, separators=(",", ":"))
            if isinstance(partition_counts, Mapping)
            else partition_counts
        )
        sql = f"""
BEGIN;
WITH inserted AS (
    INSERT INTO streaming.experiment_scenarios (
        run_id, sequence_number, scenario_name, topic, partition_count,
        active_partitions, partition_event_counts,
        partition_balance_cv_percent, target_rate_events_s,
        target_late_ratio_percent, late_delay_seconds, duration_seconds,
        seed, started_at, finished_at, expected_events, attempted, delivered,
        failed, simulated_late, remaining, elapsed_seconds,
        delivered_rate_events_s, rate_achievement_percent,
        delivery_success_percent, actual_late_ratio_percent, status, result_data
    ) VALUES (
        {sql_literal(run_id)}::UUID,
        {sequence_number},
        {sql_literal(result['scenario'])},
        {sql_literal(result['topic'])},
        {sql_literal(result.get('partition_count'))},
        {sql_literal(result.get('active_partitions'))},
        {sql_literal(partition_counts_json)}::JSONB,
        {sql_literal(result.get('partition_balance_cv_percent'))},
        {sql_literal(result.get('target_rate_events_s'))},
        {sql_literal(result.get('target_late_ratio_percent'))},
        {sql_literal(result.get('late_delay_seconds'))},
        {sql_literal(result.get('duration_seconds'))},
        {sql_literal(result.get('seed'))},
        {sql_literal(result['started_at_utc'])}::TIMESTAMPTZ,
        {sql_literal(result['finished_at_utc'])}::TIMESTAMPTZ,
        {sql_literal(result.get('expected_events'))},
        {sql_literal(result.get('attempted'))},
        {sql_literal(result.get('delivered'))},
        {sql_literal(result.get('failed'))},
        {sql_literal(result.get('simulated_late'))},
        {sql_literal(result.get('remaining'))},
        {sql_literal(result.get('elapsed_seconds'))},
        {sql_literal(result.get('delivered_rate_events_s'))},
        {sql_literal(result.get('rate_achievement_percent'))},
        {sql_literal(result.get('delivery_success_percent'))},
        {sql_literal(result.get('actual_late_ratio_percent'))},
        {sql_literal(result['status'])},
        {sql_literal(result_json)}::JSONB
    )
    RETURNING scenario_id
)
INSERT INTO streaming.experiment_logs (scenario_id, log_text)
SELECT scenario_id, {sql_literal(log_text)}
FROM inserted;
COMMIT;
"""
        self._execute(sql)

    def finish_run(self, run_id: str, status: str, finished_at: str) -> None:
        self._execute(
            "UPDATE streaming.experiment_runs SET "
            f"status = {sql_literal(status)}, "
            f"finished_at = {sql_literal(finished_at)}::TIMESTAMPTZ "
            f"WHERE run_id = {sql_literal(run_id)}::UUID;"
        )

    def _execute(self, sql: str) -> str:
        completed = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                self.user,
                "-d",
                self.database,
            ],
            cwd=self.project_root,
            input=sql,
            text=True,
            capture_output=True,
            check=True,
        )
        return completed.stdout.strip()
