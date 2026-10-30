# Bàn giao dữ liệu cho Grafana

Ngày kiểm tra: 30/10/2026 (Asia/Ho_Chi_Minh).

## Kết quả kiểm chứng

Schema và hai migration đã áp dụng lên database đang chạy. Bằng chứng chi tiết:
[verification/2026-10-30.json](verification/2026-10-30.json).

| Kiểm tra | Kết quả |
|---|---|
| Đủ bốn bảng đầu ra Spark | 91 dòng window, 9 run, 20 batch metrics, 50 partition metrics |
| Generator → Kafka → Spark → PostgreSQL | Ba lượt demo phát 300, 600, 200 event; đều thành công, không lỗi delivery |
| Validation | Phát 20 event hợp lệ và 5 event lỗi; chỉ 20 giao dịch vào kết quả |
| Khôi phục checkpoint | Dừng Spark, phát thêm 10 event; restart đọc đúng offset `25 → 35`, giữ `query_id`, đổi `spark_run_id` |
| Watermark | 10 late event bị loại; kết quả nghiệp vụ giữ nguyên |
| Tách topic | Cùng window/category: p1 giữ 30 giao dịch/30.000 VND, p3 giữ 40 giao dịch/80.000 VND |
| Quay lại topic cũ | Query tiếp tục đúng checkpoint, không ghi trùng kết quả |
| Dừng có kiểm soát | Container ghi `stopped` và `finished_at` trước khi JVM thoát |
| Metrics thiếu | Không có trường hợp tính `rows_read`/lag khi thiếu offset cần thiết |

Phiên mặc định đang chạy tại thời điểm kiểm tra:

- Topic: `raw-events`, 3 partition.
- `query_id`: `e11dc096-91c5-4f28-9329-00ff3d59cab1`.
- `spark_run_id`: `6e7b7b1b-381f-4a34-91b8-2409e8e5354d`.
- Có mẫu p95 latency và checkpoint commit trong PostgreSQL; log phiên này không có lỗi.

Database giữ lại một run `failed` dùng để tái hiện lỗi shutdown trước bản sửa.
Sáu run cũ đã xác nhận dừng nhưng Docker không còn giữ thời điểm kết thúc
chính xác; chúng có `status = stopped`, `finished_at = NULL`. Các phiên dừng
bằng bản sửa hiện tại ghi được thời gian kết thúc.

## Kết nối và schema

- Grafana chạy trong Docker: PostgreSQL ở `postgres:5432`.
- Database: `streaming_db`; schema: `streaming`.
- Lấy tài khoản/mật khẩu từ cấu hình `.env` của môi trường triển khai.
- Timestamp trong PostgreSQL là `TIMESTAMPTZ`. Có thể đặt timezone dashboard
  thành `Asia/Ho_Chi_Minh`; không cộng thêm 7 giờ trong SQL.

| Bảng | Cột thời gian | Dữ liệu cho panel | Bộ lọc/liên kết |
|---|---|---|---|
| `pipeline_runs` | `started_at`, `finished_at` | Cấu hình, số partition, trạng thái job | `topic`, `spark_run_id`, `query_id` |
| `pipeline_batch_metrics` | `observed_at` | Input/processed throughput, latency, watermark, state, checkpoint, tổng/max lag | `topic`, `spark_run_id` |
| `kafka_partition_metrics` | `observed_at` | Offset, số record đọc, lag từng partition | `topic`, `partition_id`, `spark_run_id`, `batch_id` |
| `transaction_window_results` | `window_start`, `window_end` | Số giao dịch, doanh thu, thống kê late event đã được chấp nhận | `topic`, `merchant_category` |
| `experiment_runs`, `experiment_scenarios` | `started_at`, `finished_at` | Rate, late ratio, partition và kết quả generator | `run_id`, `topic`, khoảng thời gian |

Các tên bảng trên thuộc schema `streaming`. Khóa kết quả nghiệp vụ là
`(topic, window_start, window_end, merchant_category)`.

## Quy ước khi làm dashboard

- Throughput: `rows/second`; latency và thời gian checkpoint: `milliseconds`.
- `amount_vnd`, `total_amount_vnd`: VND; lag/offset: record.
- `NULL` nghĩa là thiếu số đo. Hiển thị gap/No data; không đổi thành `0`.
- `latency_p95_ms` là p95 của từng micro-batch. Trung bình các giá trị p95
  không phải p95 của toàn khoảng thời gian.
- `simulated_late_count` đếm event late được đưa vào kết quả window.
  `rows_dropped_by_watermark` là số dòng bị loại tại state operator của Spark;
  sau bước tổng hợp cục bộ, nó có thể nhỏ hơn số event Kafka bị loại. Trong
  fixture bàn giao, 10 late event cùng nhóm bị loại nhưng Spark báo 1 state row.
- Join metrics batch và partition bằng cả `spark_run_id` và `batch_id`.
- `query_id` giữ nguyên sau restart cùng checkpoint; `spark_run_id` đổi.
  Khi xem một phiên chạy, lọc theo `spark_run_id`.
- Liên kết thí nghiệm generator với Spark bằng `topic` và khoảng thời gian chạy.
- Loại nhãn `<legacy-unknown>` khỏi các panel theo topic thực tế, nếu database
  được nâng cấp từ phiên bản chưa lưu topic.
- Các topic `handover-20261030-p1` và `handover-20261030-p3` là dữ liệu kiểm thử
  cố định. Dữ liệu generator đang dùng cho demo nằm ở `raw-events`.

## SQL tham khảo

Truy vấn sau dùng cửa sổ 24 giờ; khi đưa vào Grafana, thay điều kiện thời gian
bằng bộ lọc thời gian của dashboard và thêm bộ lọc topic/run theo yêu cầu.

```sql
-- Throughput, latency và checkpoint theo batch.
SELECT observed_at AS time, topic, spark_run_id,
       input_rows_per_second, processed_rows_per_second,
       latency_avg_ms, latency_p95_ms, latency_max_ms,
       checkpoint_commit_ms, total_consumer_lag, max_partition_lag,
       rows_dropped_by_watermark
FROM streaming.pipeline_batch_metrics
WHERE observed_at >= NOW() - INTERVAL '24 hours'
ORDER BY observed_at;

-- Lag từng partition.
SELECT observed_at AS time, topic, partition_id, consumer_lag
FROM streaming.kafka_partition_metrics
WHERE observed_at >= NOW() - INTERVAL '24 hours'
ORDER BY observed_at, topic, partition_id;

-- Doanh thu/số giao dịch theo category và window.
SELECT window_start AS time, topic, merchant_category,
       transaction_count, total_amount_vnd, simulated_late_count
FROM streaming.transaction_window_results
WHERE window_start >= NOW() - INTERVAL '24 hours'
  AND topic <> '<legacy-unknown>'
ORDER BY window_start, topic, merchant_category;

-- Metadata thí nghiệm để tạo bộ lọc và so sánh rate/late ratio/partition.
SELECT r.run_id, r.experiment_type, s.scenario_name, s.topic,
       s.partition_count, s.target_rate_events_s,
       s.target_late_ratio_percent, s.delivered_rate_events_s,
       s.actual_late_ratio_percent, s.started_at, s.finished_at, s.status
FROM streaming.experiment_runs r
JOIN streaming.experiment_scenarios s USING (run_id)
ORDER BY s.started_at DESC;
```

## Chạy lại môi trường

Theo [hướng dẫn nâng cấp và khởi động](BAO_CAO_BAN_GIAO_SPARK.md#41-khởi-động-pipeline-qua-docker-compose).
Nạp schema trước hai migration để tạo đủ bảng trên database cũ.
Checkpoint mặc định hiện tại là `transaction-window-v3/<topic>`.

Để tạo dữ liệu demo mới bằng generator sau khi Spark đang chạy với `raw-events`:

```bash
python3 main.py run --topic raw-events --partitions 3 --rate 20 \
  --duration-seconds 30 --late-ratio 0.1 --skip-build
```

Generator chạy theo thời lượng cấu hình rồi dừng; Spark tiếp tục chờ event mới.
Dashboard live sẽ không có input mới nếu không chạy generator.
Khi clone repo trên máy khác, chạy generator để tạo dữ liệu cho PostgreSQL của
môi trường đó.
