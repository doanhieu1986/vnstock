"""Cấu hình tập trung, đọc từ biến môi trường (.env).

Tách config ra một chỗ để: (1) không hard-code secret trong code,
(2) sau này dễ khai báo ranh giới local-only vs. ra-API cho phase RAG.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()  # nạp .env nếu có


def _get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Settings:
    # MinIO / object store
    minio_endpoint: str = _get("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = _get("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = _get("MINIO_SECRET_KEY", "minioadmin")
    bucket: str = _get("LAKEHOUSE_BUCKET", "lakehouse")
    bronze_prefix: str = _get("BRONZE_PREFIX", "bronze")

    # vnstock
    vnstock_source: str = _get("VNSTOCK_SOURCE", "KBS")
    vnstock_api_key: str = _get("VNSTOCK_API_KEY", "")
    ingest_sleep_seconds: float = float(_get("INGEST_SLEEP_SECONDS", "3") or 3)

    # ranh giới data
    allow_external_api: bool = _get("ALLOW_EXTERNAL_API", "false").lower() == "true"

    @property
    def bronze_uri(self) -> str:
        """URI gốc của tầng Bronze, ví dụ: s3://lakehouse/bronze"""
        return f"s3://{self.bucket}/{self.bronze_prefix}"


settings = Settings()
