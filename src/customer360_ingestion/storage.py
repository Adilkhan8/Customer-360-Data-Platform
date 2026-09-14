"""S3 key construction and raw-file transfer."""

from pathlib import Path
from typing import Protocol


class S3Client(Protocol):
    """Minimal S3 client contract required by the uploader."""

    def upload_fileobj(self, fileobj: object, bucket: str, key: str) -> None:
        ...


def build_s3_key(prefix: str, source_date: str, filename: str) -> str:
    """Build the partitioned key used by the S3 raw zone."""
    clean_prefix = prefix.strip("/")
    parts = [part for part in (clean_prefix, source_date, filename) if part]
    return "/".join(parts)


def upload_file(path: Path, bucket: str, key: str, client: S3Client) -> None:
    """Upload one local file to S3."""
    with path.open("rb") as source:
        client.upload_fileobj(source, bucket, key)