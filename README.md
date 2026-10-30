# Streaming Lab

Pipeline transaction thời gian thực sử dụng Kafka, Spark Structured Streaming,
PostgreSQL và Grafana.

Tài liệu cho người làm dashboard: [Bàn giao Grafana](docs/GRAFANA_HANDOVER.md).

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

## 3. Cấu trúc source code

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

## 4. Dependency và test

Cài dependency Python:

```bash
python3 -m pip install -r requirements.txt
```

Sau khi thay đổi dependency, build lại generator:

```bash
docker compose build event-generator
```

