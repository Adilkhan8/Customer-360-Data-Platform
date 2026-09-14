"""Shared data structures and source-batch configuration."""

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple


EXPECTED_FILES: Tuple[str, ...] = (
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