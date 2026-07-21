"""Test tầng Bronze mà KHÔNG cần MinIO/vnstock thật.

Thay S3 bằng LocalFileSystem của fsspec, dùng OHLCV giả giống output vnstock.
Kiểm tra 3 điều cốt lõi của Bronze: metadata, partition, idempotency.
"""
import fsspec
import pandas as pd
import pyarrow.parquet as pq

from src.bronze.writer import BronzeWriter, new_batch_id


def _fake_ohlcv(n=5):
    """DataFrame giống output market.equity(...).ohlcv() của vnstock."""
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "time": dates,
            "open": range(100, 100 + n),
            "high": range(101, 101 + n),
            "low": range(99, 99 + n),
            "close": range(100, 100 + n),
            "volume": [1000 * i for i in range(1, n + 1)],
        }
    )


def _read_partition(fs, base, symbol, ingest_date):
    d = f"{base}/ohlcv/ingest_date={ingest_date}/symbol={symbol}"
    files = [p for p in fs.ls(d) if p.endswith(".parquet")]
    return pd.concat([pq.read_table(fs.open(f)).to_pandas() for f in files])


def test_metadata_and_write(tmp_path):
    fs = fsspec.filesystem("file")
    base = str(tmp_path / "bronze")
    writer = BronzeWriter(fs, base)
    batch = new_batch_id()

    rows = writer.write_ohlcv(_fake_ohlcv(5), "VNM", batch, "KBS", ingest_date="2024-06-01")
    assert rows == 5

    df = _read_partition(fs, base, "VNM", "2024-06-01")
    # cột gốc được giữ nguyên
    for col in ["time", "open", "high", "low", "close", "volume"]:
        assert col in df.columns
    # cột metadata được thêm
    for col in ["_ingested_at", "_source", "_batch_id", "_symbol"]:
        assert col in df.columns
    assert set(df["_symbol"].unique()) == {"VNM"}
    assert set(df["_source"].unique()) == {"KBS"}


def test_idempotency_overwrites(tmp_path):
    """Ghi lại cùng (symbol, ingest_date) không được nhân đôi dữ liệu."""
    fs = fsspec.filesystem("file")
    base = str(tmp_path / "bronze")
    writer = BronzeWriter(fs, base)

    writer.write_ohlcv(_fake_ohlcv(5), "FPT", new_batch_id(), "KBS", ingest_date="2024-06-01")
    writer.write_ohlcv(_fake_ohlcv(5), "FPT", new_batch_id(), "KBS", ingest_date="2024-06-01")

    df = _read_partition(fs, base, "FPT", "2024-06-01")
    assert len(df) == 5  # vẫn 5 dòng, không phải 10


def test_manifest(tmp_path):
    fs = fsspec.filesystem("file")
    base = str(tmp_path / "bronze")
    writer = BronzeWriter(fs, base)
    batch = new_batch_id()

    results = [
        {"symbol": "VNM", "rows": 5, "status": "ok"},
        {"symbol": "ZZZ", "status": "error", "error": "not found"},
    ]
    path = writer.write_manifest("ohlcv", batch, results, {"start": "2024-01-01"})
    assert fs.exists(path)

    import json

    with fs.open(path) as f:
        m = json.load(f)
    assert m["summary"]["ok"] == 1
    assert m["summary"]["error"] == 1
    assert m["summary"]["rows"] == 5
