"""Kết nối tới object store (MinIO) qua giao thức S3.

Trả về một fsspec filesystem để tầng Bronze ghi Parquet lên đó.
BronzeWriter nhận filesystem này qua tham số, nên khi test có thể thay
bằng LocalFileSystem mà không cần MinIO thật.
"""
from __future__ import annotations

import s3fs

from config.settings import settings


def get_s3fs() -> s3fs.S3FileSystem:
    """Tạo S3FileSystem trỏ vào MinIO (path-style addressing)."""
    return s3fs.S3FileSystem(
        key=settings.minio_access_key,
        secret=settings.minio_secret_key,
        client_kwargs={"endpoint_url": settings.minio_endpoint},
        # MinIO dùng path-style: http://host:9000/bucket/key
        config_kwargs={"s3": {"addressing_style": "path"}},
    )
