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
# 1. Bật toàn bộ service: MinIO + metastore-db/hive-metastore/trino (query engine)
#    + airflow-db/airflow-webserver/airflow-scheduler (orchestration).
#    Lần đầu sẽ lâu hơn: build image Airflow tuỳ biến + hive-metastore init schema.
cp .env.example .env
docker compose up -d
#   MinIO console : http://localhost:9001  (minioadmin / minioadmin)
#   Trino Web UI  : http://localhost:8080
#   Airflow UI    : http://localhost:8081  (admin / admin)

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

## Data processing (Spark → Iceberg Silver)

Bronze ghi theo `ingest_date` (ngày CHẠY batch), không phải ngày giao dịch —
vì Airflow DAG ingest cửa sổ trailing 5 ngày mỗi lần chạy, cùng 1 giao dịch
(symbol, time) có thể nằm ở nhiều `ingest_date` khác nhau. Silver dùng Spark
để đọc toàn bộ Bronze, khử trùng lặp, ép kiểu, ghi thành bảng **Iceberg**
thật (không phải external table như Bronze) — Iceberg tự đăng ký
schema/snapshot vào `hive-metastore` (dùng lại metastore đã có cho Trino),
nên **không cần script `register_trino_tables.py`** cho Silver, Trino tự
thấy bảng qua catalog Iceberg mới.

```bash
# Chạy job Silver (Spark local[*] mode, tính lại toàn bộ mỗi lần — dữ liệu
# còn nhỏ nên full recompute đơn giản và đúng hơn merge/upsert)
docker compose up -d spark
docker exec lakehouse-spark /opt/spark/bin/spark-submit /opt/spark-jobs/silver_ohlcv.py
docker exec lakehouse-spark /opt/spark/bin/spark-submit /opt/spark-jobs/silver_symbols.py

# Query qua Trino (catalog "iceberg", khác catalog "hive" của Bronze)
python -c "
import trino
conn = trino.dbapi.connect(host='localhost', port=8080, user='vnstock', catalog='iceberg', schema='silver')
cur = conn.cursor()
cur.execute('SELECT symbol, time, close FROM iceberg.silver.ohlcv ORDER BY time DESC LIMIT 5')
print(cur.fetchall())
"
```

- Cấu hình Spark (Iceberg catalog trỏ `hive-metastore`, S3A trỏ MinIO):
  `spark/conf/spark-defaults.conf`. Image build từ `spark/Dockerfile`
  (`apache/spark` + jar Iceberg/hadoop-aws — cần bản Java 17 vì Iceberg
  runtime jar mới yêu cầu, base image Java 11 sẽ lỗi
  `UnsupportedClassVersionError`).
- Job: `spark/jobs/silver_ohlcv.py` (dedup theo `symbol, time`, giữ bản
  `_ingested_at` mới nhất), `spark/jobs/silver_symbols.py` (dedup theo
  `symbol`). Container `spark` không chạy thường trực — gọi qua
  `docker exec`/`docker compose run` khi cần.
- **Lưu ý Parquet timestamp**: Bronze ghi cột `time` bằng microsecond
  (`datetime64[us]`) thay vì nanosecond mặc định của pandas — Spark 3.5
  không đọc được Parquet `TIMESTAMP(NANOS)` (`_write_parquet` trong
  `src/bronze/writer.py` tự ép kiểu trước khi ghi). Nếu có Parquet Bronze cũ
  ghi từ trước khi sửa, cần rewrite lại (đổi kiểu cột `time` sang
  `timestamp('us')`) thì Spark mới đọc được.

## Orchestration (Airflow)

DAG `bronze_ingest_daily` (`airflow/dags/bronze_ingest_dag.py`) gọi lại đúng
CLI/job ở trên qua `BashOperator`, không viết lại logic ingest/xử lý:

```
[ingest_symbols]     ┬─► [register_trino_tables]   (sync Bronze/Hive)
                     └─► [spark_silver_symbols]     (Bronze -> Silver/Iceberg)

[ingest_ohlcv_vn30]  ┬─► [register_trino_tables]
                     └─► [spark_silver_ohlcv]
```

`register_trino_tables` (Bronze/Hive) và `spark_silver_*` (Silver/Iceberg) là
2 catalog độc lập, không phụ thuộc nhau — chạy song song sau khi ingest xong.

**Task `spark_silver_*` gọi `docker exec lakehouse-spark spark-submit ...`**
— container Airflow không tự chạy Spark, nó exec lệnh vào container `spark`
riêng. Cần mount Docker socket (`/var/run/docker.sock`) vào
`airflow-scheduler` + cài Docker CLI trong `airflow/Dockerfile` (chỉ
scheduler cần, vì LocalExecutor chạy task ngay trong container đó).
**Đây là pattern chỉ dùng cho dev/local** — nó cho container Airflow quyền
điều khiển toàn bộ Docker host (tương đương root), chấp nhận được vì chỉ
chạy trên máy cá nhân. Ở production, không exec thẳng vào container khác:
dùng `SparkSubmitOperator` (Spark cluster thật qua network,
`spark://host:7077`) hoặc `KubernetesPodOperator`/managed service (EMR,
Dataproc, Databricks) — Airflow chỉ cần địa chỉ mạng + credentials, không
cần shell access vào compute engine.

- Lịch: `30 15 * * 1-5` giờ **Asia/Ho_Chi_Minh** (15:30, thứ 2-6 — sau giờ
  đóng cửa phiên ~15:00).
- Phạm vi: rổ **VN30**, cửa sổ **trailing 5 ngày** mỗi lần chạy (start = ngày
  chạy − 5, end = ngày chạy) — an toàn nếu DAG lỡ chạy trễ/miss 1 hôm, không lo
  trùng dữ liệu vì `BronzeWriter` ghi đè theo partition.
- DAG mặc định **paused** khi mới bật lần đầu — vào UI
  (`http://localhost:8081`, admin/admin) bật (unpause) DAG `bronze_ingest_daily`
  để nó tự chạy theo lịch, hoặc bấm Trigger để chạy thử ngay.
- Container Airflow build từ `airflow/Dockerfile` (extend image chính thức +
  cài `requirements.txt`), mount thẳng project vào `/opt/airflow/vnstock` nên
  không cần đóng gói lại khi sửa code/DAG — chỉ cần sửa file, Airflow tự nạp
  lại DAG (không cần rebuild trừ khi đổi `requirements.txt`).
- **Rate limit vnstock**: mặc định Guest (20 req/phút) khá sát giới hạn khi
  chạy tự động không giám sát — DAG dùng `INGEST_SLEEP_SECONDS=6` (cao hơn
  mặc định `.env` là 3s) để có biên an toàn. Nếu vẫn gặp lỗi rate limit, nên
  đăng ký API key Community (60 req/phút, miễn phí tại
  https://vnstocks.com/login) và đặt `VNSTOCK_API_KEY` trong `.env`.
- **Resume khi bị rate limit giữa chừng** (vd. backfill dài ngày qua
  `run_bronze_ingest.py ohlcv`): thư viện vnstock tự `sys.exit()` khi chạm
  rate limit thay vì raise lỗi bình thường, nên cả lệnh dừng ngay lập tức.
  Mặc định `ohlcv` sẽ **tự bỏ qua các mã đã có dữ liệu Bronze trong ngày
  UTC hiện tại** trước khi gọi vnstock — cứ đợi hết cửa sổ rate limit rồi
  chạy lại **y nguyên câu lệnh cũ**, các mã đã xong sẽ được skip, chỉ nạp
  tiếp mã còn thiếu. Dùng `--force` nếu muốn nạp lại toàn bộ bất kể đã có
  dữ liệu hay chưa.

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
- `Market().equity().ohlcv()` có tham số `count` (mặc định 100) luôn được
  truyền xuống thành `count_back`, và provider (vd. KBS) luôn `.tail(count_back)`
  **sau khi** đã lọc theo `start`/`end` — nếu không set `count` đủ lớn, khoảng
  ngày rộng vẫn bị cắt còn đúng 100 dòng cuối bất kể `start`/`end` yêu cầu bao
  nhiêu. `vnstock_fetcher.py` đã tự tính `count` từ khoảng `start`-`end` để
  tránh bị cắt (xem `_estimate_count`).

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
trino/catalog/hive.properties   # cấu hình Trino Hive connector (Bronze)
trino/catalog/iceberg.properties # cấu hình Trino Iceberg connector (Silver)
hive-metastore/conf/            # JDBC (MySQL) + S3A (MinIO) cho Hive Metastore
spark/Dockerfile                 # image Spark tuỳ biến (+ jar Iceberg/hadoop-aws)
spark/conf/spark-defaults.conf   # Iceberg catalog + S3A config cho Spark
spark/jobs/silver_ohlcv.py       # Bronze -> Silver: OHLCV (dedup + Iceberg)
spark/jobs/silver_symbols.py     # Bronze -> Silver: danh sách mã
airflow/Dockerfile               # image Airflow tuỳ biến (+ requirements.txt, Docker CLI)
airflow/dags/bronze_ingest_dag.py # DAG ingest -> sync Trino + Silver hằng ngày
tests/test_bronze_writer.py     # test tầng Bronze
```

## Bước tiếp theo (chưa làm ở phase này)

- RAG (FastEmbed + Qdrant), Metric (Cube.js), Reports (Metabase), Chainlit.
