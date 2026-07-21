"""Lớp bọc (adapter) quanh vnstock v4 — Unified UI.

Toàn bộ phần phụ thuộc vào API vnstock được cô lập TẠI ĐÂY. Nếu vnstock
đổi interface (thư viện này đổi khá thường xuyên), chỉ cần sửa file này,
không đụng tới tầng Bronze/pipeline.

Tham chiếu API v4 (28-04-2026):
    from vnstock import Market, Listing
    Market().equity('VNM').ohlcv(start='2024-01-01', end='2024-05-01', interval='1D')
    Listing(source='KBS').all_symbols()
"""
from __future__ import annotations

import pandas as pd


class VNStockFetcher:
    def __init__(self, source: str = "KBS", api_key: str | None = None):
        self.source = source

        # Đăng ký API key (tùy chọn) để nâng rate limit từ 20 -> 60 req/phút
        if api_key:
            from vnstock import register_user

            register_user(api_key=api_key)

        # Import trễ để lỗi thiếu thư viện chỉ nổ khi thực sự gọi
        from vnstock import Listing, Market

        self._Listing = Listing
        self._market = Market()

    def list_symbols(self) -> pd.DataFrame:
        """Danh sách toàn bộ mã niêm yết (dùng để lặp ingestion)."""
        listing = self._Listing(source=self.source)
        df = listing.all_symbols()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)

    def ohlcv(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str = "1D",
    ) -> pd.DataFrame:
        """OHLCV lịch sử của một mã. Trả về DataFrame THÔ, chưa transform.

        Tầng Bronze cố ý giữ nguyên schema gốc; mọi chuẩn hoá để dành cho Silver.
        """
        df = self._market.equity(symbol).ohlcv(
            start=start, end=end, interval=interval
        )
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        return df
