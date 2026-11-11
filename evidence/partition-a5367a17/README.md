# Experiment evidence

- Run ID: `a5367a17-b3d1-42ea-a195-b7d0cf7281e6`
- Experiment: `partition`
- Topic: `multiple-topics`
- Status: `passed`
- Started at: `2026-11-11T22:03:37.724+07:00`
- Finished at: `2026-11-11T22:10:28.863+07:00`
- Bundle generated at: `2026-11-11T15:11:10+00:00`
- Dashboard: http://localhost:3000/d/streaming-experiment-evidence/streaming-experiment-evidence?var-experiment_run_id=a5367a17-b3d1-42ea-a195-b7d0cf7281e6&var-scenario_name=All

## Files

- `run.json`: cấu hình và kết quả gốc của run/scenario.
- `scenario-metrics.csv`: throughput, lag, latency, checkpoint và accuracy.
- `window-accuracy.csv`: expected/actual theo window và category.
- `checkpoint-recovery.csv`: bằng chứng stop/restart nếu run là checkpoint.
- `grafana-dashboard.json`: dashboard cùng toàn bộ truy vấn SQL tại thời điểm xuất.
- `grafana-overview.jpg`: ảnh tổng quan dashboard, nếu đã chụp.
- `grafana-checkpoint-recovery.jpg`: ảnh panel recovery, nếu đã chụp.
- `checksums.sha256`: checksum để phát hiện evidence bị thay đổi.
