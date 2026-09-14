"""Validation and discovery for a dated raw CSV batch."""

import csv
from pathlib import Path
from typing import List

from .models import EXPECTED_FILES


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