# Experiment evidence

- Run ID: `9247659e-ff0e-4157-b6f3-846931443764`
- Experiment: `watermark`
- Topic: `raw-events`
- Status: `passed`
- Started at: `2026-11-11T21:28:13.58+07:00`
- Finished at: `2026-11-11T21:37:24.458+07:00`
- Bundle generated at: `2026-11-11T14:38:17+00:00`
- Dashboard: http://localhost:3000/d/streaming-experiment-evidence/streaming-experiment-evidence?var-experiment_run_id=9247659e-ff0e-4157-b6f3-846931443764&var-scenario_name=All

## Files

- `run.json`: cấu hình và kết quả gốc của run/scenario.
- `scenario-metrics.csv`: throughput, lag, latency, checkpoint và accuracy.
- `window-accuracy.csv`: expected/actual theo window và category.
- `checkpoint-recovery.csv`: bằng chứng stop/restart nếu run là checkpoint.
- `grafana-dashboard.json`: dashboard cùng toàn bộ truy vấn SQL tại thời điểm xuất.
- `grafana-overview.jpg`: ảnh tổng quan dashboard, nếu đã chụp.
- `grafana-checkpoint-recovery.jpg`: ảnh panel recovery, nếu đã chụp.
- `checksums.sha256`: checksum để phát hiện evidence bị thay đổi.
