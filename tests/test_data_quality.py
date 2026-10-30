"""Small unit tests for pure Python parsing and synthetic file checks."""

import tempfile
import unittest
from pathlib import Path

from src.pipeline import file_sha256, parse_date, parse_decimal, read_csv


class TestHelpers(unittest.TestCase):
    def test_parse_date(self):
        self.assertEqual(str(parse_date("2026-10-29")), "2026-10-29")
        self.assertIsNone(parse_date(""))

    def test_parse_decimal(self):
        self.assertEqual(str(parse_decimal("12.50")), "12.50")
        self.assertIsNone(parse_decimal(""))
        self.assertIsNone(parse_decimal("not-a-number"))

    def test_file_hash_is_repeatable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            path.write_text("a,b\n1,2\n", encoding="utf-8")
            self.assertEqual(file_sha256(path), file_sha256(path))

    def test_transaction_csv_has_required_columns(self):
        path = Path(__file__).resolve().parents[1] / "data" / "transactions.csv"
        rows = read_csv(path, "transactions")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["transaction_id"], "T1001")


if __name__ == "__main__":
    unittest.main()
