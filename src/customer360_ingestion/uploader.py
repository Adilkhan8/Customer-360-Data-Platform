"""Orchestrate validation and upload of a dated raw CSV batch."""

from pathlib import Path
from typing import List, Optional

from .models import EXPECTED_FILES, UploadResult
from .storage import S3Client, build_s3_key, upload_file
from .validation import count_csv_rows, find_source_files


def upload_raw_files(
    source_dir: Path,
    bucket: str,
    prefix: str = "raw",
    s3_client: Optional[S3Client] = None,
    dry_run: bool = False,
) -> List[UploadResult]:
    """Validate and upload all expected files from one dated source directory."""
    source_dir = source_dir.resolve()
    source_date = source_dir.name
    files = find_source_files(source_dir)
    results = []

    if not dry_run and s3_client is None:
        import boto3

        s3_client = boto3.client("s3")

    for path in files:
        row_count = count_csv_rows(path)
        key = build_s3_key(prefix, source_date, path.name)
        if not dry_run:
            upload_file(path, bucket, key, s3_client)
        results.append(UploadResult(path, key, row_count))

    return results


__all__ = [
    "EXPECTED_FILES",
    "UploadResult",
    "build_s3_key",
    "count_csv_rows",
    "find_source_files",
    "upload_raw_files",
]