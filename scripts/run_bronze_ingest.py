#!/usr/bin/env python3
"""CLI chạy ingestion VNStock -> Bronze.

Ví dụ:
    # Nạp danh sách mã
    python scripts/run_bronze_ingest.py symbols

    # Nạp OHLCV cho VN30 (danh sách rút gọn sẵn), 1 năm gần nhất
    python scripts/run_bronze_ingest.py ohlcv --vn30 --start 2025-01-01 --end 2025-12-31

    # Nạp OHLCV cho các mã chỉ định
    python scripts/run_bronze_ingest.py ohlcv --symbols VNM,FPT,ACB --start 2024-01-01 --end 2024-12-31
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Cho phép chạy trực tiếp từ thư mục gốc dự án
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.pipelines.ingest_ohlcv import run_ohlcv_ingest  # noqa: E402
from src.pipelines.ingest_symbols import run_symbols_ingest  # noqa: E402

# VN30 (rút gọn để test nhanh, không cần gọi listing). Chỉnh tuỳ ý.
VN30 = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG",
    "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB",
    "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="VNStock -> Bronze ingestion")
    sub = parser.add_subparsers(dest="dataset", required=True)

    sub.add_parser("symbols", help="Nạp danh sách mã niêm yết")

    p_ohlcv = sub.add_parser("ohlcv", help="Nạp OHLCV lịch sử")
    g = p_ohlcv.add_mutually_exclusive_group(required=True)
    g.add_argument("--symbols", help="Danh sách mã, ngăn cách bằng dấu phẩy")
    g.add_argument("--vn30", action="store_true", help="Dùng rổ VN30 sẵn có")
    p_ohlcv.add_argument("--start", required=True, help="YYYY-MM-DD")
    p_ohlcv.add_argument("--end", required=True, help="YYYY-MM-DD")
    p_ohlcv.add_argument("--interval", default="1D", help="1D, 1H, ... (mặc định 1D)")
    p_ohlcv.add_argument("--limit", type=int, default=None, help="Giới hạn số mã (test)")
    p_ohlcv.add_argument("--sleep", type=float, default=None, help="Giãn cách (giây)")

    args = parser.parse_args()

    if args.dataset == "symbols":
        run_symbols_ingest()
        return

    symbols = VN30 if args.vn30 else [s.strip().upper() for s in args.symbols.split(",")]
    if args.limit:
        symbols = symbols[: args.limit]

    run_ohlcv_ingest(
        symbols=symbols,
        start=args.start,
        end=args.end,
        interval=args.interval,
        sleep_seconds=args.sleep,
    )


if __name__ == "__main__":
    main()
