# Experiment evidence

- Run ID: `88464f73-0ea7-4e6a-bfa1-0253b59ed6d0`
- Experiment: `rate`
- Topic: `raw-events`
- Status: `passed`
- Started at: `2026-11-11T21:44:24.617+07:00`
- Finished at: `2026-11-11T21:50:32.511+07:00`
- Bundle generated at: `2026-11-11T14:51:18+00:00`
- Dashboard: http://localhost:3000/d/streaming-experiment-evidence/streaming-experiment-evidence?var-experiment_run_id=88464f73-0ea7-4e6a-bfa1-0253b59ed6d0&var-scenario_name=All

## Files

- `run.json`: cấu hình và kết quả gốc của run/scenario.
- `scenario-metrics.csv`: throughput, lag, latency, checkpoint và accuracy.
- `window-accuracy.csv`: expected/actual theo window và category.
- `checkpoint-recovery.csv`: bằng chứng stop/restart nếu run là checkpoint.
- `grafana-dashboard.json`: dashboard cùng toàn bộ truy vấn SQL tại thời điểm xuất.
- `grafana-overview.jpg`: ảnh tổng quan dashboard, nếu đã chụp.
- `grafana-checkpoint-recovery.jpg`: ảnh panel recovery, nếu đã chụp.
- `checksums.sha256`: checksum để phát hiện evidence bị thay đổi.
