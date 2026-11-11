# Checkpoint experiment evidence

- Run ID: `511eb6ee-90ac-4481-b6b1-5cfce79459fd`
- Experiment: `checkpoint`
- Scenario: `checkpoint-restart`
- Topic: `raw-events`
- Status: `passed`
- Started at: `2026-11-11 07:55:31 +07`
- Evidence: 50 expected, 50 actual, 0 missing, 0 extra
- Checkpoint recovery: verified

## Files

- `run.json`: cấu hình và kết quả gốc của run/scenario.
- `scenario-metrics.csv`: throughput, lag, latency, checkpoint và accuracy.
- `window-accuracy.csv`: expected/actual theo window và category.
- `checkpoint-recovery.csv`: bằng chứng stop/restart.
- `grafana-dashboard.json`: dashboard cùng truy vấn SQL tại thời điểm xuất.
- `grafana-overview.jpg`: bộ lọc run/scenario và accuracy tổng quan.
- `grafana-checkpoint-recovery.jpg`: panel checkpoint restart verification.
- `dashboard-url.txt`: URL mở lại đúng run.
- `checksums.sha256`: checksum cho toàn bộ evidence.
