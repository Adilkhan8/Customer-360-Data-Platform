import csv
import tempfile
import unittest
from pathlib import Path

from customer360_ingestion.transformation import transform_raw_batch


class TransformationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.raw_dir = self.root / "raw" / "2026-09-13"
        self.raw_dir.mkdir(parents=True)
        self.curated_dir = self.root / "curated"

        self._write_csv(
            self.raw_dir / "customers.csv",
            [
                ["customer_id", "first_name", "last_name", "email", "phone", "city", "state", "signup_date", "status"],
                ["CUST001", "Aisha", "Khan", "aisha.khan@example.com", "555-0101", "Austin", "TX", "2025-01-15", "active"],
                ["CUST001", "Aisha", "Khan", "aisha.khan@example.com", "555-0101", "Austin", "TX", "2025-01-15", "active"],
                ["CUST002", "Daniel", "Lee", "daniel.lee@example.com", "555-0102", "Seattle", "WA", "2025-03-22", "ACTIVE"],
            ],
        )

        self._write_csv(
            self.raw_dir / "orders.csv",
            [
                ["order_id", "customer_id", "order_date", "product_id", "product_name", "quantity", "unit_price", "order_status"],
                ["ORD1001", "CUST001", "2026-09-01", "PROD101", "Wireless Headphones", "1", "79.99", "completed"],
                ["ORD1002", "CUST001", "2026-09-08", "PROD205", "USB-C Charging Cable", "2", "14.50", "COMPLETED"],
            ],
        )

        self._write_csv(
            self.raw_dir / "website_visits.csv",
            [
                ["visit_id", "customer_id", "visit_timestamp", "page_url", "device", "traffic_source"],
                ["VIS5001", "CUST001", "2026-09-01T09:14:00Z", "/products/wireless-headphones", "mobile", "organic"],
                ["VIS5002", "CUST001", "2026-09-07T18:42:00Z", "/checkout", "DESKTOP", "email"],
            ],
        )

        self._write_csv(
            self.raw_dir / "support_tickets.csv",
            [
                ["ticket_id", "customer_id", "created_at", "category", "priority", "status", "resolved_at"],
                ["TKT9001", "CUST001", "2026-09-02T11:30:00Z", "delivery", "normal", "resolved", "2026-09-03T15:10:00Z"],
                ["TKT9002", "CUST002", "2026-09-10T13:45:00Z", "payment", "high", "OPEN", ""],
            ],
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def _write_csv(self, path: Path, rows):
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerows(rows)

    def test_transform_raw_batch_standardizes_and_deduplicates(self):
        outputs = transform_raw_batch(self.raw_dir, self.curated_dir)

        self.assertIn("customers", outputs)
        self.assertIn("orders", outputs)
        self.assertIn("website_visits", outputs)
        self.assertIn("support_tickets", outputs)

        customers = outputs["customers"]
        self.assertEqual(customers.count(), 2)
        customer_row = customers.filter("customer_id = 'CUST001'").first()
        self.assertEqual(customer_row["status"], "active")
        self.assertEqual(str(customer_row["signup_date"]), "2025-01-15")

        orders = outputs["orders"]
        self.assertEqual(orders.count(), 2)
        order_row = orders.filter("order_id = 'ORD1002'").first()
        self.assertEqual(str(order_row["order_status"]), "completed")
        self.assertEqual(float(order_row["unit_price"]), 14.5)

        visits = outputs["website_visits"]
        self.assertEqual(visits.count(), 2)
        self.assertEqual(visits.filter("device = 'desktop'").count(), 1)

        tickets = outputs["support_tickets"]
        self.assertEqual(tickets.count(), 2)
        self.assertEqual(tickets.filter("customer_id = 'CUST002'").first()["status"], "open")


if __name__ == "__main__":
    unittest.main()
