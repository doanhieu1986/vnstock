"""Đăng ký bảng Trino (Hive connector) trỏ vào tầng Bronze trên MinIO.

BronzeWriter ghi thẳng Parquet xuống thư mục phân vùng
(bronze/ohlcv/ingest_date=.../symbol=..., bronze/symbols/ingest_date=...),
không qua Hive `ADD PARTITION`. Script này:
  1. Tạo schema `hive.bronze` + bảng external `ohlcv`/`symbols` (idempotent).
  2. Đồng bộ partition có sẵn trên MinIO vào Hive Metastore.

Chạy lại an toàn sau mỗi lần ingest mới (để nạp partition mới xuất hiện).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import trino

from config.settings import settings

CREATE_SCHEMA = f"""
CREATE SCHEMA IF NOT EXISTS hive.bronze
WITH (location = 's3a://{settings.bucket}/{settings.bronze_prefix}/')
"""

CREATE_OHLCV = f"""
CREATE TABLE IF NOT EXISTS hive.bronze.ohlcv (
    "time" timestamp(6),
    open double,
    high double,
    low double,
    close double,
    volume bigint,
    _ingested_at varchar,
    _source varchar,
    _batch_id varchar,
    _symbol varchar,
    ingest_date varchar,
    symbol varchar
) WITH (
    external_location = 's3a://{settings.bucket}/{settings.bronze_prefix}/ohlcv/',
    format = 'PARQUET',
    partitioned_by = ARRAY['ingest_date', 'symbol']
)
"""

CREATE_SYMBOLS = f"""
CREATE TABLE IF NOT EXISTS hive.bronze.symbols (
    symbol varchar,
    organ_name varchar,
    _ingested_at varchar,
    _source varchar,
    _batch_id varchar,
    ingest_date varchar
) WITH (
    external_location = 's3a://{settings.bucket}/{settings.bronze_prefix}/symbols/',
    format = 'PARQUET',
    partitioned_by = ARRAY['ingest_date']
)
"""

SYNC_PARTITIONS = "CALL hive.system.sync_partition_metadata(?, ?, 'ADD')"


def run(cur, sql: str, label: str) -> None:
    print(f"==> {label}")
    cur.execute(sql)
    cur.fetchall()


def main() -> None:
    conn = trino.dbapi.connect(
        host=settings.trino_host,
        port=settings.trino_port,
        user="vnstock",
        catalog=settings.trino_catalog,
        schema=settings.trino_schema,
    )
    cur = conn.cursor()

    run(cur, CREATE_SCHEMA, "Tạo schema hive.bronze")
    run(cur, CREATE_OHLCV, "Tạo bảng hive.bronze.ohlcv")
    run(cur, CREATE_SYMBOLS, "Tạo bảng hive.bronze.symbols")

    for table in ("ohlcv", "symbols"):
        cur.execute(SYNC_PARTITIONS, ("bronze", table))
        cur.fetchall()
        print(f"==> Đồng bộ partition cho hive.bronze.{table}")

    print("\nXong. Thử: SELECT symbol, count(*) FROM hive.bronze.ohlcv GROUP BY symbol;")


if __name__ == "__main__":
    main()
