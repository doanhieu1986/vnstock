"""Pipeline: OHLCV nhiều mã -> Bronze.

Lặp qua từng mã, gọi vnstock, ghi Bronze, giãn cách để tránh rate limit,
cuối cùng ghi manifest tổng kết batch.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from config.settings import settings
from src.bronze.writer import BronzeWriter, new_batch_id
from src.fetchers.vnstock_fetcher import VNStockFetcher
from src.storage.object_store import get_s3fs


def run_ohlcv_ingest(
    symbols: list[str],
    start: str,
    end: str,
    interval: str = "1D",
    sleep_seconds: float | None = None,
    skip_existing: bool = True,
) -> dict:
    source = settings.vnstock_source
    sleep_seconds = (
        settings.ingest_sleep_seconds if sleep_seconds is None else sleep_seconds
    )

    fetcher = VNStockFetcher(source=source, api_key=settings.vnstock_api_key or None)
    writer = BronzeWriter(get_s3fs(), settings.bronze_uri)
    batch_id = new_batch_id()
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    results: list[dict] = []
    rate_limited = False
    for i, sym in enumerate(symbols, 1):
        if skip_existing and writer.has_ohlcv_partition(sym, ingest_date):
            results.append({"symbol": sym, "status": "skipped"})
            print(f"[{i}/{len(symbols)}] {sym}: da co du lieu hom nay -> bo qua")
            continue

        try:
            df = fetcher.ohlcv(sym, start=start, end=end, interval=interval)
            rows = writer.write_ohlcv(
                df, symbol=sym, batch_id=batch_id, source=source, ingest_date=ingest_date
            )
            results.append({"symbol": sym, "rows": rows, "status": "ok"})
            print(f"[{i}/{len(symbols)}] {sym}: {rows} dong -> Bronze")
        except SystemExit as exc:
            # vnstock (qua vnai) tu goi sys.exit() khi cham rate limit, khong phai
            # Exception thuong nen phai bat rieng. Dung batch tai day, khong thu
            # tiep cac ma con lai trong lan chay nay.
            results.append({"symbol": sym, "status": "error", "error": str(exc)})
            print(f"[{i}/{len(symbols)}] {sym}: LOI rate limit - {exc}")
            rate_limited = True
            break
        except Exception as exc:  # noqa: BLE001 — Bronze ghi lỗi vào manifest, không dừng batch
            results.append({"symbol": sym, "status": "error", "error": str(exc)})
            print(f"[{i}/{len(symbols)}] {sym}: LOI - {exc}")

        if i < len(symbols):
            time.sleep(sleep_seconds)

    params = {
        "start": start, "end": end, "interval": interval,
        "source": source, "sleep_seconds": sleep_seconds,
    }
    manifest_path = writer.write_manifest("ohlcv", batch_id, results, params)

    ok = sum(1 for r in results if r["status"] == "ok")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    print(
        f"\n==> Batch {batch_id}: {ok}/{len(symbols)} ma OK, {skipped} bo qua (da co). "
        f"Manifest: {manifest_path}"
    )
    if rate_limited:
        print(
            "==> Bi rate limit giua chung. Chay lai dung lenh nay (cung ngay UTC) "
            "se tu dong bo qua cac ma da xong, chi nap tiep ma con thieu."
        )
    return {
        "batch_id": batch_id,
        "results": results,
        "manifest": manifest_path,
        "rate_limited": rate_limited,
    }
