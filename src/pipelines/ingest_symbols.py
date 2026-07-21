"""Pipeline: danh sách mã niêm yết -> Bronze (bảng tham chiếu)."""
from __future__ import annotations

from config.settings import settings
from src.bronze.writer import BronzeWriter, new_batch_id
from src.fetchers.vnstock_fetcher import VNStockFetcher
from src.storage.object_store import get_s3fs


def run_symbols_ingest() -> dict:
    source = settings.vnstock_source
    fetcher = VNStockFetcher(source=source, api_key=settings.vnstock_api_key or None)
    writer = BronzeWriter(get_s3fs(), settings.bronze_uri)
    batch_id = new_batch_id()

    df = fetcher.list_symbols()
    rows = writer.write_symbols(df, batch_id=batch_id, source=source)
    print(f"==> Ghi {rows} ma vao Bronze. Batch {batch_id}")

    results = [{"symbol": "__all__", "rows": rows, "status": "ok"}]
    writer.write_manifest("symbols", batch_id, results, {"source": source})
    return {"batch_id": batch_id, "rows": rows}
