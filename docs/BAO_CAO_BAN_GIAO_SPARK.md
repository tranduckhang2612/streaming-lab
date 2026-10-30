# Báo Cáo Bàn Giao: Spark Structured Streaming

- **Người thực hiện:** Hoang Minh
- **Hạng mục:** Spark Structured Streaming Pipeline
- **Ngày bàn giao:** 2026-10-27
- **Tình trạng:** Hoàn thành
---

## 1. Tổng quan nhiệm vụ

Xây dựng và vận hành pipeline xử lý luồng dữ liệu giao dịch bằng Spark Structured Streaming với các mục tiêu:
1. Đọc stream transaction từ Kafka topic (`raw-events`).
2. Parse dữ liệu JSON tuân theo schema hợp đồng giao dịch (`contracts/transaction-schema.json`).
3. Xử lý theo Event Time (`event_time`), áp dụng Watermark (mặc định 30s) và Tumbling Window (mặc định 1m).
4. Thống kê theo `merchant_category`: số lượng giao dịch, tổng/TB/min/max giá trị giao dịch (`amount_vnd`), số event đến muộn (`simulated_late_count`) và độ trễ trung bình (`average_event_lateness_ms`).
5. Upsert kết quả window vào PostgreSQL theo cơ chế idempotency.
6. Đo lường và ghi nhận runtime metrics (batch metrics, partition lag, end-to-end latency) vào PostgreSQL qua `StreamingQueryListener` và Spark `.observe()`.
7. Kiểm chứng độ tin cậy của checkpoint khi dừng và tái khởi động ứng dụng streaming.

Trước khi tổng hợp, Spark kiểm tra đủ 10 trường bắt buộc và loại chuỗi rỗng
hoặc chỉ có khoảng trắng. JSON lỗi, trường không parse được theo kiểu dữ liệu
đã khai báo, trường thiếu hoặc `null` được gắn
`validation_error = invalid_json_or_missing_field`. Kafka key thiếu hoặc
không khớp `account_id` được gắn `key_mismatch`; currency khác `VND` được gắn
`unsupported_currency`. Các event này bị loại khỏi tổng hợp window, không ghi
vào bảng kết quả. Boolean `false` vẫn là giá trị hợp lệ của `simulated_late`.

---

## 2. Kiến trúc & Các bảng dữ liệu bàn giao

Toàn bộ dữ liệu được ghi nhận vào PostgreSQL (`streaming_db`):

| Tên bảng | Mục đích bàn giao | Khóa chính (PK) |
|---|---|---|
| `streaming.transaction_window_results` | Kết quả thống kê nghiệp vụ theo từng topic, window và category | `(topic, window_start, window_end, merchant_category)` |
| `streaming.pipeline_runs` | Thông tin cấu hình và định danh mỗi lần Spark khởi chạy | `spark_run_id` |
| `streaming.pipeline_batch_metrics` | Throughput, latency (avg, p95, max), watermark, state memory, commit time theo batch | `(spark_run_id, batch_id)` |
| `streaming.kafka_partition_metrics` | Start/End offset, latest offset, số row đọc và consumer lag theo từng partition | `(spark_run_id, batch_id, topic, partition_id)` |

---

## 3. Bằng chứng nghiệm thu kỹ thuật (Evidence)

Số liệu dưới đây được ghi nhận trước khi bổ sung `topic` vào khóa kết quả.
Schema và hướng dẫn chạy hiện tại được cập nhật ở mục 2 và mục 4.

### 3.1. Phục hồi trạng thái qua Checkpoint (Idempotent Restart)

Cùng một đường dẫn checkpoint (`file:///opt/spark/work-dir/checkpoints/transaction-window-v2`), qua nhiều lần dừng và khởi động lại:
- **`query_id` duy nhất và không đổi:** `1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e`.
- **`spark_run_id` tự động cấp mới mỗi phiên chạy:**
  - Phiên gần nhất: `56451447-89a5-43f7-b1a0-380c73142372`
  - Đã ghi nhận đúng `partition_count = 3` và trạng thái `running`.

*Truy vấn đối chứng:*
```sql
SELECT spark_run_id, query_id, checkpoint_location, partition_count, status, started_at
FROM streaming.pipeline_runs
ORDER BY started_at;
```

*Kết quả thực tế:*
```text
             spark_run_id             |               query_id               |                     checkpoint_location                      | partition_count | status  |          started_at
--------------------------------------+--------------------------------------+--------------------------------------------------------------+-----------------+---------+-------------------------------
 6baf12ed-f4d4-4dd0-a1e2-41d6ecf5be1b | 1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e | file:///opt/spark/work-dir/checkpoints/transaction-window-v2 |                 | failed  | 2026-10-25 17:46:14.469034+07
 939e992d-2e5f-44b6-b885-316e7ae6d011 | 1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e | file:///opt/spark/work-dir/checkpoints/transaction-window-v2 |                 | running | 2026-10-25 17:53:15.667024+07
 6599c8d4-6bb2-41fe-ba96-d8ba6b908aa3 | 1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e | file:///opt/spark/work-dir/checkpoints/transaction-window-v2 |                 | running | 2026-10-27 13:46:26.789221+07
 f13cb892-ef81-437c-85fc-4d5426541ac7 | 1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e | file:///opt/spark/work-dir/checkpoints/transaction-window-v2 |                 | running | 2026-10-27 13:54:26.793698+07
 23dec6f1-f61a-4134-a654-eb0299e2f20b | 1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e | file:///opt/spark/work-dir/checkpoints/transaction-window-v2 |                 | running | 2026-10-27 14:54:47.024215+07
 56451447-89a5-43f7-b1a0-380c73142372 | 1a9d05aa-914e-4ed6-9c2c-d19f6c34c99e | file:///opt/spark/work-dir/checkpoints/transaction-window-v2 |               3 | running | 2026-10-27 15:01:32.242242+07
```

---

### 3.2. Tính toàn vẹn dữ liệu: Không mất mát, không lặp lại

**a. Tính liên tục của Kafka Offsets:**
Số lượng record thực đọc (`total_read`) khớp chính xác với hiệu số offset (`max_end - min_start`) trên toàn bộ 3 partition. Điều này chứng minh không có khoảng trống offset bị bỏ qua và không có dải offset nào bị đọc lặp sau các lần restart.

*Truy vấn đối chứng:*
```sql
SELECT topic, partition_id, MIN(start_offset) AS min_start, MAX(end_offset) AS max_end, SUM(rows_read) AS total_read
FROM streaming.kafka_partition_metrics
GROUP BY topic, partition_id;
```

*Kết quả thực tế:*
```text
   topic    | partition_id | min_start | max_end | total_read 
------------+--------------+-----------+---------+------------
 raw-events |            1 |         0 |    1885 |       1885
 raw-events |            0 |         0 |    1032 |       1032
 raw-events |            2 |         0 |    1683 |       1683
```

**b. Không bị trùng bản ghi kết quả window:**
Cơ chế upsert `ON CONFLICT (window_start, window_end, merchant_category) DO UPDATE` đảm bảo cập nhật trạng thái tích lũy thay vì sinh thêm bản ghi dư thừa.

*Truy vấn đối chứng:*
```sql
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT (window_start, window_end, merchant_category)) AS unique_keys
FROM streaming.transaction_window_results;
```

*Kết quả thực tế:*
```text
 total_rows | unique_keys 
------------+-------------
         72 |          72
```

---

### 3.3. Đo lường độ trễ xử lý (Latency Metrics)

Metrics độ trễ xử lý end-to-end được tích hợp trực tiếp qua Spark DataFrame Observation (`transaction-latency`) và trích xuất tự động vào bảng `streaming.pipeline_batch_metrics`:

*Truy vấn đối chứng:*
```sql
SELECT spark_run_id, batch_id, partition_count, latency_avg_ms, latency_p95_ms, latency_max_ms
FROM streaming.pipeline_batch_metrics m
JOIN streaming.pipeline_runs r USING (spark_run_id)
ORDER BY observed_at DESC LIMIT 5;
```

*Kết quả thực tế:*
```text
             spark_run_id             | batch_id | partition_count | latency_avg_ms | latency_p95_ms | latency_max_ms
--------------------------------------+----------+-----------------+----------------+----------------+----------------
 56451447-89a5-43f7-b1a0-380c73142372 |       73 |               3 |       4111.875 |       4812.000 |       4812.000
 56451447-89a5-43f7-b1a0-380c73142372 |       72 |               3 |       2415.160 |       4614.000 |       4815.000
 56451447-89a5-43f7-b1a0-380c73142372 |       71 |               3 |       2413.440 |       4613.000 |       4814.000
 56451447-89a5-43f7-b1a0-380c73142372 |       70 |               3 |       2412.160 |       4612.000 |       4813.000
 56451447-89a5-43f7-b1a0-380c73142372 |       69 |               3 |       2412.400 |       4613.000 |       4813.000
```

---

## 4. Hướng dẫn vận hành cho các thành viên

Kết quả kiểm chứng toàn luồng ngày 30/10/2026 và SQL cho dashboard nằm trong
[tài liệu bàn giao Grafana](GRAFANA_HANDOVER.md).

### 4.1. Khởi động pipeline qua Docker Compose
```bash
# 1. Khởi động các hạ tầng cơ sở
docker compose up -d postgres kafka kafka-init spark-master spark-worker

# 2. Dừng Spark và cập nhật schema cho database hiện có
docker compose --profile streaming stop spark-submit
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < database/postgres/init/streaming_schema.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < database/postgres/migrations/001_window_results_topic.sql
docker compose exec -T postgres sh -c 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < database/postgres/migrations/002_nullable_pipeline_metrics.sql

# 3. Khởi động Spark streaming submit
docker compose --profile streaming up -d spark-submit

# 4. Theo dõi log streaming
docker compose logs -f spark-submit
```

Database mới đã có khóa `(topic, window_start, window_end, merchant_category)`
trong schema khởi tạo. Migration cũng có thể chạy lại an toàn trên schema mới.
Các dòng cũ không có thông tin topic được giữ với nhãn `<legacy-unknown>`;
lọc chúng khỏi thống kê theo topic thực tế. Dữ liệu đã bị ghi đè trước đây
không thể khôi phục chỉ bằng migration.

Metrics không có trong Spark progress được lưu `NULL`; giá trị `0` do Spark
báo cáo vẫn được giữ. `rows_read` chỉ được tính khi có cả start/end offset,
`consumer_lag` chỉ được tính khi có cả end/latest offset và hiệu không âm.
Tổng/max lag chỉ có giá trị khi mọi partition được báo cáo đều có lag;
thời gian checkpoint chỉ được tính khi đủ các thành phần áp dụng. Query báo
`stateOperators: []` không có phần state commit; thiếu mục này là chưa biết.
Không xác định được partition nào thì `partition_count` là `NULL`.
Migration 002 cho phép các metric thiếu được lưu `NULL`, không sửa lại các số
liệu lịch sử đã ghi trước bản sửa này. Dashboard nên hiển thị dữ liệu thiếu
thay vì dùng `COALESCE(..., 0)` để biểu diễn thành số đo bằng 0.

Checkpoint mặc định hiện được tách theo input topic:
`file:///opt/spark/work-dir/checkpoints/transaction-window-v3/<topic>`.
Ví dụ `raw-events-p1` và `raw-events-p3` dùng hai thư mục riêng; khởi động lại
cùng topic sẽ dùng lại checkpoint của topic đó. Không đặt một giá trị
`SPARK_CHECKPOINT_LOCATION` chung khi chuyển qua lại giữa các topic.

Việc thêm `topic` vào khóa nhóm Spark thay đổi schema của state. Checkpoint
trước thay đổi này (bao gồm `transaction-window-v2/<topic>`) không tương thích;
dùng đường dẫn mặc định `v3` hoặc một thư mục mới khi đặt
`SPARK_CHECKPOINT_LOCATION` / `--checkpoint-location`. Spark tạo query mới
và đọc lại từ `earliest` đối với dữ liệu còn được Kafka lưu giữ.

### 4.2. Khởi động sinh giao dịch thử nghiệm
```powershell
docker compose --profile generator run --rm event-generator
```

### 4.3. Bàn giao cho phụ trách Grafana (Tuan Minh)
Tất cả bảng dữ liệu thuộc schema `streaming` đã sẵn sàng để cấu hình datasource và query trên dashboard:
- Dashboard Throughput & Lag: Truy vấn từ `streaming.pipeline_batch_metrics` và `streaming.kafka_partition_metrics`.
- Dashboard Latency & Checkpoint: `latency_avg_ms`, `latency_p95_ms`, `checkpoint_commit_ms` từ `streaming.pipeline_batch_metrics`.
- Dashboard Nghiệp vụ: Doanh thu và số lượng giao dịch theo window từ `streaming.transaction_window_results`.
