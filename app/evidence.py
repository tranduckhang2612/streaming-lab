"""Export one experiment run as a report-ready evidence bundle."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

from .config import PROJECT_ROOT
from .database import ExperimentStore, sql_literal


@dataclass(frozen=True)
class EvidenceBundle:
    run_id: str
    experiment_type: str
    path: Path


def export_evidence_bundle(
    store: ExperimentStore,
    *,
    run_id: str | None = None,
    output_dir: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> EvidenceBundle:
    selected_run_id = run_id or store._execute("""
SELECT r.run_id::TEXT
FROM streaming.experiment_runs r
WHERE EXISTS (
    SELECT 1 FROM streaming.experiment_scenarios s WHERE s.run_id = r.run_id
)
ORDER BY r.started_at DESC
LIMIT 1;
""")
    if not selected_run_id:
        raise RuntimeError("No experiment run with scenario evidence was found.")

    metadata_text = store._execute(f"""
SELECT json_build_object(
    'run_id', r.run_id,
    'experiment_type', r.experiment_type,
    'topic', r.topic,
    'started_at', r.started_at,
    'finished_at', r.finished_at,
    'status', r.status,
    'configuration', r.configuration,
    'scenarios', COALESCE((
        SELECT json_agg(json_build_object(
            'sequence_number', s.sequence_number,
            'scenario_name', s.scenario_name,
            'status', s.status,
            'spark_run_id', s.spark_run_id,
            'spark_query_id', s.spark_query_id,
            'ground_truth_status', s.ground_truth_status,
            'result', s.result_data
        ) ORDER BY s.sequence_number)
        FROM streaming.experiment_scenarios s
        WHERE s.run_id = r.run_id
    ), '[]'::JSON)
)
FROM streaming.experiment_runs r
WHERE r.run_id = {sql_literal(selected_run_id)}::UUID;
""")
    if not metadata_text:
        raise RuntimeError(f"Experiment run {selected_run_id} was not found.")
    metadata = json.loads(metadata_text)
    experiment_type = str(metadata["experiment_type"])

    root = output_dir or project_root / "evidence"
    if not root.is_absolute():
        root = project_root / root
    bundle_path = root / f"{experiment_type}-{selected_run_id[:8]}"
    bundle_path.mkdir(parents=True, exist_ok=True)

    _write_text(
        bundle_path / "run.json",
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True),
    )
    _write_text(
        bundle_path / "scenario-metrics.csv",
        _copy_csv(store, f"""
SELECT m.sequence_number, m.scenario_name, m.experiment_type, m.topic,
       m.partition_count, m.active_partitions, m.watermark_delay_seconds,
       m.target_rate_events_s, m.delivered_rate_events_s,
       m.spark_input_rows, m.avg_processed_rows_per_second,
       m.max_total_consumer_lag, m.max_partition_lag,
       m.max_batch_p95_latency_ms, m.p95_checkpoint_commit_ms,
       m.rows_dropped_by_watermark, m.spark_run_id, m.spark_query_id,
       m.spark_checkpoint_location, m.metrics_bounds_complete,
       m.spark_status, m.scenario_status,
       a.total_expected_count, a.total_actual_count,
       a.total_missing_count, a.total_extra_count,
       a.overall_count_accuracy_percent, a.overall_amount_accuracy_percent,
       a.accuracy_complete
FROM streaming.experiment_scenario_metrics m
LEFT JOIN streaming.experiment_scenario_accuracy a
  ON a.experiment_run_id = m.experiment_run_id
 AND a.scenario_name = m.scenario_name
WHERE m.experiment_run_id = {sql_literal(selected_run_id)}::UUID
ORDER BY m.sequence_number
"""),
    )
    _write_text(
        bundle_path / "window-accuracy.csv",
        _copy_csv(store, f"""
SELECT *
FROM streaming.window_accuracy_results
WHERE experiment_run_id = {sql_literal(selected_run_id)}::UUID
ORDER BY scenario_name, window_start, merchant_category
"""),
    )
    _write_text(
        bundle_path / "checkpoint-recovery.csv",
        _copy_csv(store, f"""
SELECT *
FROM streaming.checkpoint_recovery_results
WHERE experiment_run_id = {sql_literal(selected_run_id)}::UUID
ORDER BY sequence_number
"""),
    )

    dashboard_source = (
        project_root / "docker/grafana/dashboards/experiment_evidence.json"
    )
    shutil.copy2(dashboard_source, bundle_path / "grafana-dashboard.json")
    dashboard_url = (
        "http://localhost:3000/d/streaming-experiment-evidence/"
        "streaming-experiment-evidence"
        f"?var-experiment_run_id={selected_run_id}&var-scenario_name=All"
    )
    _write_text(bundle_path / "dashboard-url.txt", dashboard_url)
    _write_text(
        bundle_path / "README.md",
        _bundle_readme(metadata, dashboard_url),
    )
    _write_checksums(bundle_path)
    return EvidenceBundle(selected_run_id, experiment_type, bundle_path)


def _copy_csv(store: ExperimentStore, query: str) -> str:
    return store._execute(
        f"COPY ({query.strip()}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);"
    )


def _write_text(path: Path, value: str) -> None:
    path.write_text(value.rstrip() + "\n", encoding="utf-8")


def _bundle_readme(metadata: dict[str, object], dashboard_url: str) -> str:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return f"""# Experiment evidence

- Run ID: `{metadata['run_id']}`
- Experiment: `{metadata['experiment_type']}`
- Topic: `{metadata['topic']}`
- Status: `{metadata['status']}`
- Started at: `{metadata['started_at']}`
- Finished at: `{metadata['finished_at']}`
- Bundle generated at: `{generated_at}`
- Dashboard: {dashboard_url}

## Files

- `run.json`: cấu hình và kết quả gốc của run/scenario.
- `scenario-metrics.csv`: throughput, lag, latency, checkpoint và accuracy.
- `window-accuracy.csv`: expected/actual theo window và category.
- `checkpoint-recovery.csv`: bằng chứng stop/restart nếu run là checkpoint.
- `grafana-dashboard.json`: dashboard cùng toàn bộ truy vấn SQL tại thời điểm xuất.
- `grafana-overview.jpg`: ảnh tổng quan dashboard, nếu đã chụp.
- `grafana-checkpoint-recovery.jpg`: ảnh panel recovery, nếu đã chụp.
- `checksums.sha256`: checksum để phát hiện evidence bị thay đổi.
"""


def _write_checksums(bundle_path: Path) -> None:
    rows = []
    for path in sorted(bundle_path.iterdir()):
        if path.name == "checksums.sha256" or not path.is_file():
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        rows.append(f"{digest}  {path.name}")
    _write_text(bundle_path / "checksums.sha256", "\n".join(rows))
