"""Command-line entry point for raw CSV ingestion."""

import argparse
from pathlib import Path

from .uploader import upload_raw_files


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload a dated Customer 360 CSV batch to an S3 raw bucket."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/raw/2026-09-13"),
        help="Dated directory containing the four source CSV files.",
    )
    parser.add_argument("--bucket", required=True, help="Target S3 bucket name.")
    parser.add_argument(
        "--prefix", default="raw", help="S3 key prefix for the raw zone."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate files and print target keys without uploading.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    results = upload_raw_files(
        source_dir=args.source_dir,
        bucket=args.bucket,
        prefix=args.prefix,
        dry_run=args.dry_run,
    )
    action = "Would upload" if args.dry_run else "Uploaded"
    for result in results:
        print(
            "{} {} rows from {} to s3://{}/{}".format(
                action, result.row_count, result.local_path, args.bucket, result.s3_key
            )
        )