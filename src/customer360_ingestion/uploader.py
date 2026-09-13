"""Validate and upload daily Customer 360 CSV files to S3."""

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence


EXPECTED_FILES = (
    "customers.csv",
    "orders.csv",
    "website_visits.csv",
    "support_tickets.csv",
)


@dataclass(frozen=True)
class UploadResult:
    """Metadata for one uploaded source file."""

    local_path: Path
    s3_key: str
    row_count: int


def find_source_files(source_dir: Path) -> List[Path]:
    """Return the documented source files in a dated local directory."""
    missing = [name for name in EXPECTED_FILES if not (source_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing expected source file(s): " + ", ".join(sorted(missing))
        )
    return [source_dir / name for name in EXPECTED_FILES]


def count_csv_rows(path: Path) -> int:
    """Validate that a CSV has headers and return its data-row count."""
    with path.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames or any(not name for name in reader.fieldnames):
            raise ValueError("CSV must contain a non-empty header: {}".format(path))
        return sum(1 for _ in reader)


def build_s3_key(prefix: str, source_date: str, filename: str) -> str:
    """Build the partitioned key used by the S3 raw zone."""
    clean_prefix = prefix.strip("/")
    parts = [part for part in (clean_prefix, source_date, filename) if part]
    return "/".join(parts)


def upload_raw_files(
    source_dir: Path,
    bucket: str,
    prefix: str = "raw",
    s3_client: Optional[object] = None,
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
            with path.open("rb") as source:
                s3_client.upload_fileobj(source, bucket, key)
        results.append(UploadResult(path, key, row_count))

    return results