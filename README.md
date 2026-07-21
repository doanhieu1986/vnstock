# VNStock → Bronze Ingestion

Phase đầu tiên của lakehouse: kéo dữ liệu từ **vnstock** và đổ xuống tầng
**Bronze** trên **MinIO** theo chuẩn medallion. Đây là nền để các phase sau
(Silver/Gold bằng Spark hoặc PyIceberg, query bằng Trino, RAG + Chainlit)
gắn vào.

## Kiến trúc phase này

```
vnstock (v4 Unified UI)  ──►  Ingestion  ──►  MinIO (bucket: lakehouse)
   Market / Listing           + metadata        bronze/
   [source: KBS]              + idempotency        ohlcv/ingest_date=…/symbol=…/part-*.parquet
                              + manifest           symbols/ingest_date=…/part-*.parquet
                                                   _manifests/ohlcv/<batch>.json
```

Tầng Bronze giữ **dữ liệu thô**, chỉ thêm cột metadata kỹ thuật
(`_ingested_at`, `_source`, `_batch_id`, `_symbol`). Mọi chuẩn hoá để dành
cho Silver.

## Yêu cầu

- macOS (Apple Silicon OK), Docker Desktop hoặc OrbStack
- Python 3.10+

## Chạy nhanh

```bash
# 1. Bật toàn bộ service: MinIO (data lake) + metastore-db/hive-metastore/trino
#    (query engine). Lần đầu sẽ mất chút thời gian để hive-metastore init schema.
cp .env.example .env
docker compose up -d
#   MinIO console: http://localhost:9001  (minioadmin / minioadmin)
#   Trino Web UI : http://localhost:8080

# 2. Cài thư viện Python (nên dùng venv)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Nạp danh sách mã niêm yết -> Bronze
python scripts/run_bronze_ingest.py symbols

# 4. Nạp OHLCV cho rổ VN30, giới hạn 3 mã để test nhanh
python scripts/run_bronze_ingest.py ohlcv --vn30 --limit 3 \
    --start 2025-01-01 --end 2025-06-30

# 5. Nạp các mã tự chọn
python scripts/run_bronze_ingest.py ohlcv --symbols VNM,FPT,ACB \
    --start 2024-01-01 --end 2024-12-31

# 6. Đăng ký bảng Trino trỏ vào Bronze + đồng bộ partition vừa nạp
#    Chạy lại bước này sau mỗi lần ingest để nạp partition mới.
python scripts/register_trino_tables.py

# 7. Query thử qua Trino
python -c "
import trino
conn = trino.dbapi.connect(host='localhost', port=8080, user='vnstock', catalog='hive', schema='bronze')
cur = conn.cursor()
cur.execute('SELECT symbol, count(*) FROM hive.bronze.ohlcv GROUP BY symbol')
print(cur.fetchall())
"
```

Sau khi ingest, mở console MinIO (cổng 9001) → bucket `lakehouse` → thư mục
`bronze/` để xem cây partition và các file Parquet, hoặc query trực tiếp qua
Trino như bước 7.

## Query engine (Trino)

Trino đọc trực tiếp Bronze qua Hive connector (chưa có Iceberg nên là bảng
external trỏ vào thư mục Parquet có sẵn, không phải bảng managed). Stack:

```
MinIO (bronze/*.parquet) ◄── hive-metastore ◄── trino
                              (biết bảng trỏ    (query engine,
                               vào đâu)          đọc qua Hive connector)
                              JDBC ▲
                                   │
                              metastore-db (MySQL)
```

- Cấu hình catalog Trino: `trino/catalog/hive.properties`.
- Conf riêng cho `hive-metastore` (JDBC tới MySQL, S3A tới MinIO — image gốc
  không có env var tiện để chỉnh 2 thứ này): `hive-metastore/conf/`.
- Script đăng ký bảng: `scripts/register_trino_tables.py` — idempotent, an
  toàn chạy lại nhiều lần (dùng `CREATE ... IF NOT EXISTS` +
  `sync_partition_metadata`).

## Kiểm thử (không cần MinIO/vnstock)

```bash
pip install pytest
python -m pytest tests/ -v
```

Test dùng `LocalFileSystem` + OHLCV giả để kiểm tra 3 tính chất cốt lõi của
Bronze: metadata, partition, và idempotency (ghi lại cùng ngày/mã không nhân
đôi dữ liệu).

## Ghi chú quan trọng về vnstock

- Code dùng **API v4 Unified UI** (`from vnstock import Market, Listing`).
  Cách gọi cũ qua class `Vnstock` đã deprecated, EOL **31-08-2026**.
- Nguồn mặc định là **KBS** (ổn định nhất trên v4). **TCBS đã bị gỡ** khỏi thư
  viện. Đổi nguồn qua biến `VNSTOCK_SOURCE` trong `.env` (ví dụ `VCI`).
- Bản free giới hạn **20 req/phút** (Guest) hoặc **60 req/phút** (Community,
  cần API key free tại https://vnstocks.com/login). Đặt `VNSTOCK_API_KEY` trong
  `.env` và tăng/giảm `INGEST_SLEEP_SECONDS` cho phù hợp.
- Toàn bộ phần phụ thuộc vnstock nằm gọn trong `src/fetchers/vnstock_fetcher.py`.
  Nếu API đổi, chỉ cần sửa file này.

## Cấu trúc thư mục

```
config/settings.py              # đọc env, cấu hình tập trung
src/storage/object_store.py     # s3fs trỏ MinIO
src/fetchers/vnstock_fetcher.py # adapter vnstock v4 (cô lập tại đây)
src/bronze/writer.py            # ghi Parquet + metadata + idempotency + manifest
src/pipelines/ingest_ohlcv.py   # pipeline OHLCV
src/pipelines/ingest_symbols.py # pipeline danh sách mã
scripts/run_bronze_ingest.py    # CLI ingest
scripts/register_trino_tables.py # đăng ký bảng Trino + sync partition
trino/catalog/hive.properties   # cấu hình Trino Hive connector
hive-metastore/conf/            # JDBC (MySQL) + S3A (MinIO) cho Hive Metastore
tests/test_bronze_writer.py     # test tầng Bronze
```

## Bước tiếp theo (chưa làm ở phase này)

- Bọc `run_bronze_ingest.py` thành Airflow DAG (schedule hằng ngày sau phiên).
- Silver: chuẩn hoá schema, ép kiểu, khử trùng lặp → ghi bảng Iceberg (đổi
  catalog Trino từ Hive external table sang Iceberg connector).
- RAG (FastEmbed + Qdrant), Metric (Cube.js), Reports (Metabase), Chainlit.
