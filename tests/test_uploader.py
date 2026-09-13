import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from customer360_ingestion.uploader import (
    EXPECTED_FILES,
    build_s3_key,
    upload_raw_files,
)


class UploaderTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.source_dir = Path(self.temp_dir.name) / "2026-09-13"
        self.source_dir.mkdir()
        for filename in EXPECTED_FILES:
            with (self.source_dir / filename).open("w", newline="") as output:
                writer = csv.writer(output)
                writer.writerow(["customer_id"])
                writer.writerow(["CUST001"])

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_build_s3_key(self):
        self.assertEqual(
            build_s3_key("/raw/", "2026-09-13", "customers.csv"),
            "raw/2026-09-13/customers.csv",
        )

    def test_dry_run_validates_without_s3_client(self):
        results = upload_raw_files(
            self.source_dir, "customer360", dry_run=True
        )
        self.assertEqual(len(results), 4)
        self.assertEqual(results[0].s3_key, "raw/2026-09-13/customers.csv")
        self.assertEqual(results[0].row_count, 1)

    def test_uploads_each_file_to_expected_key(self):
        client = Mock()
        upload_raw_files(self.source_dir, "customer360", s3_client=client)
        self.assertEqual(client.upload_fileobj.call_count, 4)
        keys = [call.args[2] for call in client.upload_fileobj.call_args_list]
        self.assertEqual(keys, [
            "raw/2026-09-13/customers.csv",
            "raw/2026-09-13/orders.csv",
            "raw/2026-09-13/website_visits.csv",
            "raw/2026-09-13/support_tickets.csv",
        ])


if __name__ == "__main__":
    unittest.main()