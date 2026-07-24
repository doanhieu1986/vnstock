"""Tầng Bronze — ghi dữ liệu THÔ xuống Data Lake theo chuẩn medallion.

Nguyên tắc Bronze áp dụng ở đây:
  1. Raw preservation  — giữ nguyên schema gốc từ nguồn, KHÔNG transform
                         business logic. Chỉ thêm cột metadata kỹ thuật.
  2. Partition rõ ràng — bronze/<dataset>/ingest_date=<d>/symbol=<s>/
  3. Idempotency       — ghi lại cùng (dataset, ingest_date, symbol) sẽ
                         GHI ĐÈ sạch partition đó, không nhân đôi dữ liệu.
  4. Audit/manifest    — mỗi lần chạy (batch) sinh 1 manifest JSON ghi lại
                         mã nào, bao nhiêu dòng, ok/lỗi — mầm mống audit trail.

Writer nhận `fs` (fsspec filesystem) qua tham số nên hoàn toàn tách khỏi
MinIO: chạy thật -> s3fs; chạy test -> LocalFileSystem.
"""
from __future__ import annotations

import io
import json
import uuid
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def new_batch_id() -> str:
    """ID lô nạp: theo thời gian + hậu tố ngẫu nhiên, dễ truy vết."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{uuid.uuid4().hex[:8]}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class BronzeWriter:
    def __init__(self, fs, base_uri: str):
        """
        fs       : fsspec filesystem (s3fs.S3FileSystem hoặc LocalFileSystem)
        base_uri : gốc tầng Bronze, ví dụ 's3://lakehouse/bronze' hoặc '/tmp/bronze'
        """
        self.fs = fs
        self.base_uri = base_uri.rstrip("/")

    # ------------------------------------------------------------------ #
    # Ghi OHLCV cho MỘT mã
    # ------------------------------------------------------------------ #
    def write_ohlcv(
        self,
        df: pd.DataFrame,
        symbol: str,
        batch_id: str,
        source: str,
        ingest_date: str | None = None,
    ) -> int:
        """Ghi OHLCV thô của 1 mã xuống Bronze. Trả về số dòng đã ghi."""
        ingest_date = ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

        enriched = self._add_metadata(
            df, source=source, batch_id=batch_id, symbol=symbol
        )

        partition_dir = (
            f"{self.base_uri}/ohlcv/ingest_date={ingest_date}/symbol={symbol}"
        )
        self._overwrite_partition(partition_dir)

        path = f"{partition_dir}/part-{batch_id}.parquet"
        self._write_parquet(enriched, path)
        return len(enriched)

    def has_ohlcv_partition(self, symbol: str, ingest_date: str) -> bool:
        """Batch trước đã ghi Bronze cho (ingest_date, symbol) này chưa.

        Dùng để resume khi 1 batch bị ngắt giữa chừng (vd. rate limit
        vnstock): bỏ qua mã đã có, chỉ nạp lại mã còn thiếu.
        """
        partition_dir = f"{self.base_uri}/ohlcv/ingest_date={ingest_date}/symbol={symbol}"
        return self.fs.exists(partition_dir)

    # ------------------------------------------------------------------ #
    # Ghi danh sách mã (bảng tham chiếu)
    # ------------------------------------------------------------------ #
    def write_symbols(
        self,
        df: pd.DataFrame,
        batch_id: str,
        source: str,
        ingest_date: str | None = None,
    ) -> int:
        ingest_date = ingest_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        enriched = self._add_metadata(df, source=source, batch_id=batch_id)

        partition_dir = f"{self.base_uri}/symbols/ingest_date={ingest_date}"
        self._overwrite_partition(partition_dir)

        path = f"{partition_dir}/part-{batch_id}.parquet"
        self._write_parquet(enriched, path)
        return len(enriched)

    # ------------------------------------------------------------------ #
    # Manifest cho mỗi batch — phục vụ audit & giám sát
    # ------------------------------------------------------------------ #
    def write_manifest(
        self,
        dataset: str,
        batch_id: str,
        results: list[dict],
        params: dict,
    ) -> str:
        manifest = {
            "dataset": dataset,
            "batch_id": batch_id,
            "created_at": _utc_now_iso(),
            "params": params,
            "summary": {
                "total": len(results),
                "ok": sum(1 for r in results if r.get("status") == "ok"),
                "error": sum(1 for r in results if r.get("status") == "error"),
                "rows": sum(r.get("rows", 0) for r in results),
            },
            "items": results,
        }
        path = f"{self.base_uri}/_manifests/{dataset}/{batch_id}.json"
        parent = path.rsplit("/", 1)[0]
        self.fs.makedirs(parent, exist_ok=True)
        with self.fs.open(path, "w") as f:
            f.write(json.dumps(manifest, ensure_ascii=False, indent=2))
        return path

    # ------------------------------------------------------------------ #
    # Nội bộ
    # ------------------------------------------------------------------ #
    @staticmethod
    def _add_metadata(
        df: pd.DataFrame,
        source: str,
        batch_id: str,
        symbol: str | None = None,
    ) -> pd.DataFrame:
        """Thêm cột metadata kỹ thuật, giữ nguyên toàn bộ cột gốc."""
        out = df.copy()
        out["_ingested_at"] = _utc_now_iso()
        out["_source"] = source
        out["_batch_id"] = batch_id
        if symbol is not None:
            out["_symbol"] = symbol
        return out

    def _overwrite_partition(self, partition_dir: str) -> None:
        """Idempotency: xoá sạch partition cũ trước khi ghi mới."""
        if self.fs.exists(partition_dir):
            self.fs.rm(partition_dir, recursive=True)
        self.fs.makedirs(partition_dir, exist_ok=True)

    def _write_parquet(self, df: pd.DataFrame, path: str) -> None:
        # Ep cot datetime ve microsecond: pandas mac dinh datetime64[ns] -> Parquet
        # TIMESTAMP(NANOS), Spark khong doc duoc kieu nay (chi ho tro MICROS/MILLIS).
        datetime_cols = df.select_dtypes(include=["datetime64[ns]"]).columns
        if len(datetime_cols) > 0:
            df = df.copy()
            for col in datetime_cols:
                df[col] = df[col].astype("datetime64[us]")

        table = pa.Table.from_pandas(df, preserve_index=False)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        with self.fs.open(path, "wb") as f:
            f.write(buf.read())
