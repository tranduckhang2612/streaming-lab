# Streaming Lab

## 1. Chỉnh thông số

Toàn bộ thông số thường dùng nằm trong config.json:
Các thông số chính:
- partitions: số partition của Kafka topic.
- rate: số transaction phát mỗi giây.
- duration_seconds: thời gian chạy.
- late_ratio: tỷ lệ đến muộn từ 0 đến 1; 0.1 nghĩa là 10%.
- late_delay_seconds: event đến muộn bị lùi bao nhiêu giây.
- experiments.watermarks: các mức watermark tính bằng giây; 0 là hợp lệ.
- seed: giữ dữ liệu có thể tái lập giữa các lần chạy.
- topic: "auto": dùng raw-events khi có 3 partition, nếu không dùng
  raw-events-pN.

## 2. Chạy và thu thập minh chứng

### 2.1. Khởi động hệ thống

~~~bash
docker compose --profile streaming up -d --build
docker compose ps
~~~

Mở Grafana tại http://localhost:3000 bằng tài khoản
admin/admin_password.

### 2.2. Pipeline end-to-end

~~~bash
python3 main.py run \
  --rate 100 \
  --duration-seconds 60 \
  --late-ratio 0.1 \
  --late-delay-seconds 45 \
  --seed 42 \
  --skip-build
~~~

Minh chứng: terminal có delivered, failed, late, delivered_rate;
Grafana và PostgreSQL có kết quả window.

### 2.3. Throughput, lag và p95 latency

~~~bash
python3 main.py experiment rate \
  --rates 10 100 500 \
  --duration-seconds 60 \
  --skip-build
~~~

Chụp các panel throughput, consumer lag, p95 latency và checkpoint time.

### 2.4. Checkpoint recovery

~~~bash
python3 main.py experiment checkpoint \
  --rate 20 \
  --duration-seconds 10 \
  --skip-build
~~~

~~~bash
docker compose exec -T postgres psql \
  -U streaming_user -d streaming_db \
  -f database/postgres/init/verify_checkpoint_recovery.sql
~~~

Kết quả hợp lệ khi checkpoint_recovery_verified=true và không có
missing/extra.

### 2.5. So sánh watermark

~~~bash
python3 main.py experiment watermark \
  --watermarks 10 30 60 \
  --rate 20 \
  --late-ratio 0.3 \
  --late-delay-seconds 45 \
  --duration-seconds 130 \
  --skip-build
~~~

Minh chứng: accuracy, late dropped, latency và state memory.

### 2.6. So sánh partition

~~~bash
python3 main.py experiment partition \
  --partitions 1 3 6 \
  --rate 500 \
  --duration-seconds 60 \
  --skip-build
~~~

Minh chứng: throughput, latency, partition balance và tài nguyên.

### 2.7. Xuất evidence

~~~bash
python3 main.py export-evidence --run-id <RUN_ID>
~~~

Kết quả nằm trong evidence/, gồm CSV metric, accuracy, cấu hình, dashboard
và checksum.

## 6. Cấu trúc source code

~~~text
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
~~~
