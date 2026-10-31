# Streaming Lab

Pipeline transaction thời gian thực sử dụng Kafka, Spark Structured Streaming,
PostgreSQL và Grafana.

Tài liệu cho người làm dashboard: [Bàn giao Grafana](docs/GRAFANA_HANDOVER.md).

Toàn bộ cấu trúc PostgreSQL và phần nâng cấp database cũ nằm trong
`database/postgres/init/streaming_schema.sql`. Chỉ cần áp dụng file này theo
[hướng dẫn vận hành](docs/BAO_CAO_BAN_GIAO_SPARK.md#41-khởi-động-pipeline-qua-docker-compose);
có thể chạy lại và giữ dữ liệu hiện có.

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
- `seed`: giữ dữ liệu có thể tái lập giữa các lần chạy.
- `topic: "auto"`: dùng `raw-events` khi có 3 partition, nếu không dùng
  `raw-events-pN`.

## 2. Chạy generator
python3 main.py run

## 3. So sánh số partition qua toàn pipeline

```bash
python3 main.py experiment partition --partitions 1 3 6 \
  --rate 20 --duration-seconds 10 --skip-build
```

Runner tự khởi động hạ tầng Spark và chạy lần lượt một job cho mỗi topic
`<experiments.topic_prefix>N`. Mỗi job có checkpoint riêng theo experiment;
runner chờ Spark đọc hết dữ liệu cũ trước khi phát, rồi chờ xử lý tới offset
cuối của mọi partition trước khi chuyển kịch bản. Nếu Spark không khởi động
hoặc không xử lý hết trong thời hạn, thí nghiệm không được báo `passed`.

Job `spark-submit` mặc định đang chạy sẽ được dừng có kiểm soát trong thời gian
thí nghiệm và khởi động lại đúng container ban đầu khi kết thúc hoặc có lỗi.
Job vốn đang dừng được giữ dừng. Các container Spark thí nghiệm được dọn sau
mỗi kịch bản.

Trong `config.json`, `runtime.spark_startup_timeout_seconds` mặc định 180 giây;
`runtime.spark_drain_timeout_seconds` mặc định 120 giây. CSV và
`experiment_scenarios.result_data` lưu `spark_run_id`, `spark_query_id`, offset
đã xử lý và trạng thái Spark để đối chiếu với dashboard. Hướng dẫn SQL nằm trong
[bàn giao Grafana](docs/GRAFANA_HANDOVER.md#thí-nghiệm-partition).

## 4. Cấu trúc source code

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
│   ├── kafka.py            # quản lý topic, partition và offset
│   ├── spark.py            # điều phối Spark cho thí nghiệm partition
│   └── database.py         # lưu PostgreSQL
├── producer/
│   ├── events.py           # sinh transaction
│   └── generator.py        # gửi transaction vào Kafka
├── spark-apps/
│   └── transaction_stream.py
├── database/
├── tests/
└── docker-compose.yml
```

## 5. Dependency và test

Cài dependency Python:

```bash
python3 -m pip install -r requirements.txt
```

Sau khi thay đổi dependency, build lại generator:

```bash
docker compose build event-generator
```

