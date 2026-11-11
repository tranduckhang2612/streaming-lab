# Streaming Lab

Pipeline transaction thời gian thực sử dụng Kafka, Spark Structured Streaming,
PostgreSQL và Grafana.

Hai dashboard được provision tự động:

- [Streaming Pipeline](docker/grafana/dashboards/streaming_pipeline.json): vận hành gần thời gian thực theo topic/Spark run.
- [Streaming Experiment Evidence](docker/grafana/dashboards/experiment_evidence.json): so sánh watermark, partition, throughput, lag, p95 latency, checkpoint và accuracy theo experiment/scenario.

Toàn bộ cấu trúc PostgreSQL và phần nâng cấp database cũ nằm trong
`database/postgres/init/streaming_schema.sql`. File này được mount vào PostgreSQL
khi khởi tạo database mới và được thiết kế để có thể áp dụng lại mà giữ dữ liệu
hiện có.

`POSTGRES_PORT` trong `.env` cấu hình cổng PostgreSQL trên máy host. Ví dụ đặt
`POSTGRES_PORT=15432` thì công cụ trên host kết nối `localhost:15432`; Spark,
Grafana và pgAdmin trong mạng Docker kết nối `postgres:5432`. Sau khi đổi giá
trị trong `.env`, chạy lại `docker compose up -d postgres` để áp dụng cổng mới.

## 1. Chỉnh thông số

Toàn bộ thông số thường dùng nằm trong `config.json`:
Các thông số chính:
- `partitions`: số partition của Kafka topic.
- `rate`: số transaction phát mỗi giây.
- `duration_seconds`: thời gian chạy.
- `late_ratio`: tỷ lệ đến muộn từ `0` đến `1`; `0.1` nghĩa là 10%.
- `late_delay_seconds`: event đến muộn bị lùi bao nhiêu giây.
- `experiments.watermarks`: các mức watermark tính bằng giây; `0` là hợp lệ.
- `seed`: giữ dữ liệu có thể tái lập giữa các lần chạy.
- `topic: "auto"`: dùng `raw-events` khi có 3 partition, nếu không dùng
  `raw-events-pN`.

## 2. Chạy generator

```bash
python3 main.py run
```

## 3. Chạy các thí nghiệm qua toàn pipeline

```bash
python3 main.py experiment partition --partitions 1 3 6 \
  --rate 20 --duration-seconds 10 --skip-build
```

So sánh watermark trong khi giữ nguyên topic, partition, rate, late ratio và
seed của generator:

```bash
python3 main.py experiment watermark --watermarks 10 30 60 \
  --rate 20 --late-ratio 0.3 --late-delay-seconds 45 \
  --duration-seconds 130 --skip-build
```

Mỗi mức watermark dùng một Spark run và checkpoint riêng. Chạy lâu hơn tổng
window và watermark lớn nhất giúp tạo ra các window đầu luồng đã đóng, nhưng
watermark ngừng tiến khi hết input nên các window cuối vẫn có thể mở. Phép kiểm
chứng dùng snapshot aggregate ở batch cuối sau khi Spark đã xử lý hết offset;
trạng thái đóng/mở của window và số dòng watermark loại vẫn được quan sát riêng
qua runtime metrics.

Các lệnh `experiment rate`, `experiment late`, `experiment watermark` và
`experiment partition` tự khởi động hạ tầng Spark và chạy một job cô lập cho
từng scenario. Với thí nghiệm partition, topic là `<experiments.topic_prefix>N`.
Mỗi job có checkpoint riêng và bắt đầu từ Kafka offset hiện tại để không phát
lại lịch sử của scenario trước; runner chờ Spark sẵn sàng trước khi phát, rồi
chờ xử lý tới offset cuối của mọi partition trước khi chuyển kịch bản. Nếu Spark
không khởi động, dùng sai watermark hoặc không xử lý hết trong thời hạn, thí
nghiệm không được báo `passed`.

Job `spark-submit` mặc định đang chạy sẽ được dừng có kiểm soát trong thời gian
thí nghiệm và khởi động lại đúng container ban đầu khi kết thúc hoặc có lỗi.
Job vốn đang dừng được giữ dừng. Các container Spark thí nghiệm được dọn sau
mỗi kịch bản.

### Kiểm chứng khôi phục checkpoint

Lệnh sau tự động chạy đúng chuỗi: phát dữ liệu với Spark đang chạy, chờ Spark
xử lý hết, dừng Spark, tiếp tục phát một backlog mới, rồi khởi động lại bằng
chính checkpoint cũ:

```bash
python3 main.py experiment checkpoint \
  --rate 20 --duration-seconds 10 --skip-build
```

`duration-seconds` áp dụng cho từng pha generator (trước khi dừng và trong lúc
Spark dừng). Kịch bản này cố định `late_ratio=0` để event bị watermark loại
không thể bị hiểu nhầm là mất dữ liệu do checkpoint. Kết quả chỉ được xác minh
khi đồng thời thỏa các điều kiện:

- `query_id` trước/sau restart giữ nguyên;
- `spark_run_id` thay đổi;
- checkpoint path giữ nguyên;
- backlog tạo khi Spark dừng được xử lý tới đủ offset;
- ground truth và Spark actual không có event thiếu hoặc thừa.

Xem bằng chứng mới nhất bằng SQL:

```bash
docker compose exec -T postgres psql -U streaming_user -d streaming_db \
  -f database/postgres/init/verify_checkpoint_recovery.sql
```

Dashboard **Streaming Experiment Evidence** có panel **Checkpoint restart
verification** hiển thị cùng các run ID, query ID, backlog, accuracy và trạng
thái `checkpoint_recovery_verified`.

Trong `config.json`, `runtime.spark_startup_timeout_seconds` mặc định 180 giây;
`runtime.spark_drain_timeout_seconds` mặc định 120 giây. CSV và
`experiment_scenarios` lưu trực tiếp `spark_run_id`, `spark_query_id`, watermark
và khoảng batch đã xử lý; `streaming.experiment_scenario_metrics` tổng hợp đúng
metrics trong khoảng đó cho dashboard. Bản JSON đầy đủ vẫn được giữ trong
`result_data`; bảng so sánh nằm trong
[dashboard Grafana](docker/grafana/dashboards/streaming_pipeline.json).

## 4. Kiểm chứng độ chính xác kết quả cửa sổ

Dữ liệu chuẩn không được tái tạo bằng timestamp mới. Generator ghi manifest JSONL
chỉ sau khi Kafka xác nhận delivery; Spark ghi actual window vào đúng
`experiment_run_id + scenario_name`. Vì vậy mỗi phép so sánh độc lập với job
thường, run cũ và các scenario khác:

- `streaming.experiment_verification_events`: event Kafka đã ACK dùng làm minh chứng.
- `streaming.experiment_window_ground_truth`: aggregate đầy đủ của mọi event đã ACK.
- `streaming.experiment_window_actuals`: aggregate Spark thực tế theo scenario.
- `streaming.window_accuracy_results`: `FULL OUTER JOIN` chi tiết, gồm cả missing và extra window/category.
- `streaming.experiment_scenario_accuracy`: accuracy tổng hợp theo từng scenario; chỉ có giá trị khi `accuracy_complete = true`.

Chạy thí nghiệm rồi đọc kết quả kiểm chứng:

```bash
# Tạo evidence thật cho từng watermark
python3 main.py experiment watermark --watermarks 10 30 60

# Xem run mới nhất có evidence
python3 main.py verify-accuracy

# Hoặc chọn chính xác run/scenario
python3 main.py verify-accuracy \
  --run-id 11111111-1111-4111-8111-111111111111 \
  --scenario watermark-10s
```

Chứng minh bằng SQL trên PostgreSQL:

```bash
docker compose exec -T postgres psql -U streaming_user -d streaming_db \
  -f database/postgres/init/verify_watermark_accuracy.sql
```

Khởi động Grafana và mở `http://localhost:3000` (hoặc `GRAFANA_PORT` trong
`.env`), vào folder **Streaming Pipeline** rồi chọn dashboard
**Streaming Experiment Evidence**. Chọn một `Experiment run`; bộ lọc `Scenario`
mặc định là tất cả scenario của run đó. Panel accuracy chỉ hiển thị điểm khi
`accuracy_complete = true`, tránh biến dữ liệu thiếu thành 0% hoặc 100% giả.

## 5. Xuất minh chứng cho báo cáo

Xuất run mới nhất có scenario:

```bash
python3 main.py export-evidence
```

Hoặc chọn chính xác run đã dùng để chụp dashboard:

```bash
python3 main.py export-evidence \
  --run-id 511eb6ee-90ac-4481-b6b1-5cfce79459fd
```

Bundle được tạo trong `evidence/<experiment-type>-<run-id-8-ký-tự>/`, gồm:

- metadata/configuration và result gốc của run;
- CSV scenario metrics, window accuracy và checkpoint recovery;
- bản dashboard JSON chứa chính xác các truy vấn SQL đã dùng;
- URL dashboard đã khóa theo `experiment_run_id`;
- checksum SHA-256 cho toàn bộ file;
- ảnh dashboard nếu đã chụp và đặt vào bundle.

Khi chụp ảnh, luôn để lộ bộ lọc **Experiment run** và **Scenario**. Với
checkpoint, ảnh hợp lệ phải thể hiện `checkpoint_recovery_verified=true`,
`no_loss_no_duplicates=true`, `total_missing_count=0` và
`total_extra_count=0`.

## 6. Cấu trúc source code

```text
streaming-lab/
├── main.py                 # entry point duy nhất
├── config.json             # nơi chỉnh toàn bộ thông số
├── requirements.txt
├── app/
│   ├── cli.py              # đọc command line
│   ├── config.py           # đọc và kiểm tra config.json
│   ├── generator.py        # chạy transaction generator
│   ├── experiment.py       # runner chung cho mọi thí nghiệm
│   ├── accuracy.py         # tính ground truth & kiểm chứng accuracy
│   ├── evidence.py         # xuất CSV/JSON/query bundle cho báo cáo
│   ├── kafka.py            # quản lý topic, partition và offset
│   ├── spark.py            # điều phối Spark cô lập cho các thí nghiệm
│   └── database.py         # lưu PostgreSQL
├── producer/
│   ├── events.py           # sinh transaction
│   └── generator.py        # gửi transaction vào Kafka
├── spark-apps/
│   └── transaction_stream.py
├── database/
│   └── postgres/init/
│       ├── streaming_schema.sql
│       ├── verify_checkpoint_recovery.sql
│       └── verify_watermark_accuracy.sql
├── tests/
└── docker-compose.yml
```

## 7. Dependency và test

Cài dependency Python:

```bash
python3 -m pip install -r requirements.txt
```

Sau khi thay đổi dependency, build lại generator:

```bash
docker compose build event-generator
```

