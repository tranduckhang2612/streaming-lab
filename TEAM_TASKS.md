# Phân công công việc nhóm

## 1. Luồng dữ liệu chung

Transaction Generator → Kafka → Spark Structured Streaming → PostgreSQL → Grafana

Quy ước đã thống nhất:

- PostgreSQL lưu cả kết quả cửa sổ và metric vận hành.
- Grafana chỉ đọc dữ liệu từ PostgreSQL bằng SQL.
- Toàn bộ câu lệnh tạo schema, table, constraint và index nằm trong
  `database/postgres/init/streaming_schema.sql`.
- Không đặt câu lệnh DDL trong tài liệu phân công này.

## 2. Phân công

| Thành viên | Phần phụ trách | Đầu ra chính |
|---|---|---|
| Duc Khang | Generator và Kafka | Event giao dịch, kịch bản rate/late ratio/partition |
| Hoang Minh | Spark Structured Streaming | Window, watermark, checkpoint, ghi PostgreSQL |
| Tuan Minh | Grafana | Datasource, dashboard và truy vấn SQL |

### Duc Khang — Generator và Kafka

Phần đã thực hiện:

- Sinh transaction có `event_time`, `produced_at` và key `account_id`.
- Cho phép chỉnh tốc độ phát, tỷ lệ đến muộn và độ trễ của event đến muộn.
- Tạo và chạy thử topic với nhiều số partition.
- Lưu kết quả các thí nghiệm generator vào PostgreSQL.

Phần cần bàn giao:
- Transaction schema và tên Kafka topic.
- Cách chạy từng kịch bản trong `main.py`.
- `run_id`, thời gian chạy và cấu hình của từng thí nghiệm.

### Hoang Minh — Spark Structured Streaming

Nhiệm vụ:

1. Đọc transaction từ Kafka topic được cấu hình.
2. Parse JSON theo transaction schema của generator.
3. Dùng `event_time` để xử lý theo event time.
4. Áp dụng watermark và window theo cấu hình.
5. Tính thống kê theo `merchant_category`:
   - số transaction;
   - tổng, trung bình, nhỏ nhất và lớn nhất của `amount_vnd`;
   - số event đến muộn và độ trễ trung bình.
6. Upsert kết quả vào `streaming.transaction_window_results`.
7. Ghi metric từng micro-batch vào PostgreSQL.
8. Dừng và khởi động lại job để kiểm chứng checkpoint.

Các bảng Spark cần ghi:

| Bảng | Nội dung |
|---|---|
| `streaming.transaction_window_results` | Kết quả nghiệp vụ theo window |
| `streaming.pipeline_runs` | Thông tin mỗi lần Spark khởi động |
| `streaming.pipeline_batch_metrics` | Throughput, latency, watermark và checkpoint theo batch |
| `streaming.kafka_partition_metrics` | Offset và consumer lag theo partition |

Kết quả bàn giao:

- Spark job chạy được bằng Docker Compose.
- PostgreSQL nhận được cả kết quả window và runtime metric.
- Cùng checkpoint thì `query_id` không đổi sau restart; `spark_run_id` đổi.
- Có bằng chứng dữ liệu không mất hoặc bị ghi trùng sau restart.

### Tuan Minh — Grafana

Nhiệm vụ:

1. Provision PostgreSQL datasource trong Grafana.
2. Viết SQL và tạo các panel:
   - input/processed throughput;
   - tổng lag và lag lớn nhất;
   - p95 end-to-end latency;
   - watermark và số dòng bị loại bởi watermark;
   - thời gian commit checkpoint;
   - số lượng và tổng giá trị transaction theo window;
   - so sánh rate, late ratio và partition giữa các thí nghiệm.
3. Thêm bộ lọc thời gian, topic, Spark run và experiment run.
4. Provision dashboard để chạy lại Docker Compose không phải cấu hình thủ công.

Kết quả bàn giao:

- Dashboard tự xuất hiện và kết nối đúng PostgreSQL.
- Mỗi panel có truy vấn SQL rõ ràng.
- Có ảnh dashboard phục vụ báo cáo và demo.

## 3. Thống nhất dữ liệu chung 

### Các ID

| ID | Ý nghĩa |
|---|---|
| `experiment_run_id` | Một lần chạy generator hoặc một thí nghiệm |
| `query_id` | Định danh Spark query, giữ nguyên khi dùng lại checkpoint |
| `spark_run_id` | Một phiên chạy Spark, thay đổi mỗi lần khởi động |

Liên kết Spark metric với một thí nghiệm bằng `topic` và khoảng thời gian chạy.

### Công thức và đơn vị

```text
event_lateness_ms = produced_at - event_time
end_to_end_latency_ms = thời điểm Spark xử lý - produced_at
consumer_lag = latest_offset - end_offset
checkpoint_commit_ms = wal_commit_ms
                     + commit_offsets_ms
                     + tổng stateOperators.commitTimeMs
```

- Throughput: `rows/second`.
- Latency và thời gian xử lý: `milliseconds`.
- Consumer lag: `records`.
- Timestamp lưu theo UTC.
- Metric không có trong Spark progress phải ghi `NULL`, không tự thay bằng `0`.

## 4. Thứ tự phối hợp

1. Duc Khang bàn giao transaction schema, topic và kịch bản generator.
2. Hoang Minh hoàn thành Spark job và ghi dữ liệu vào các bảng đã có trong SQL schema.
3. Tuan Minh dùng dữ liệu PostgreSQL để hoàn thiện dashboard.
4. Cả nhóm chạy kiểm thử đầu-cuối và thu bằng chứng cho báo cáo.

## 5. Kịch bản kiểm thử cuối

1. Khởi động Kafka, PostgreSQL, Spark và Grafana.
2. Chạy generator ở tốc độ thấp và kiểm tra toàn bộ luồng.
3. Tăng tốc độ phát để quan sát throughput, lag và backpressure.
4. Thay đổi tỷ lệ event đến muộn và watermark.
5. Dừng Spark, tiếp tục phát event, rồi khởi động lại để kiểm tra checkpoint.
6. Chạy lại với các số partition khác nhau.
7. Chụp dashboard và lưu các truy vấn/kết quả dùng trong báo cáo.

## 6. Tiêu chí hoàn thành

- Pipeline chạy đầu-cuối từ generator đến Grafana.
- Spark xử lý đúng event time, window và watermark.
- PostgreSQL có kết quả window và đầy đủ metric cần cho dashboard.
- Checkpoint khôi phục được sau khi Spark dừng.
- Dashboard thể hiện throughput, lag, p95 latency và checkpoint.
- Có bảng hoặc biểu đồ so sánh rate, late ratio, watermark và partition.
