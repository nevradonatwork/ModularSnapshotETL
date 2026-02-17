import os

import pandas as pd
import pytest

from src.ingestion import load_raw, load_staging, _parse_price, derive_snapshot_month, archive_file
from tests.conftest import COLUMNS, SAMPLE_ROWS, write_csv


class TestParsePrice:
    def test_standard_price(self):
        s = pd.Series(["$150.00"])
        assert _parse_price(s).iloc[0] == 150.00

    def test_price_with_comma(self):
        s = pd.Series(["$1,200.00"])
        assert _parse_price(s).iloc[0] == 1200.00

    def test_invalid_price_returns_nan(self):
        s = pd.Series(["not_a_price"])
        assert pd.isna(_parse_price(s).iloc[0])

    def test_empty_string_returns_nan(self):
        s = pd.Series([""])
        assert pd.isna(_parse_price(s).iloc[0])


class TestLoadRaw:
    def test_loads_csv_into_raw(self, db_conn, sample_csv):
        count = load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        assert count == 3
        rows = db_conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0]
        assert rows == 3

    def test_loads_gzip_csv(self, db_conn, sample_csv_gz):
        count = load_raw(db_conn, sample_csv_gz, "new-york", "2025-01-01", 0)
        assert count == 3

    def test_raises_on_missing_file(self, db_conn):
        with pytest.raises(FileNotFoundError):
            load_raw(db_conn, "/nonexistent/path.csv", "test", "2025-01-01", 0)

    def test_raises_on_missing_columns(self, db_conn, tmp_path):
        path = write_csv([[1, "Test"]], ["id", "name"], str(tmp_path / "bad.csv"))
        with pytest.raises(Exception, match="Missing required columns"):
            load_raw(db_conn, path, "test", "2025-01-01", 0)

    def test_city_and_snapshot_stored(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        row = db_conn.execute(
            "SELECT city, snapshot_month FROM raw_listings LIMIT 1"
        ).fetchone()
        assert row[0] == "new-york"
        assert row[1] == "2025-01-01"


class TestLoadStaging:
    def test_loads_staging_from_raw(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        count, excluded, geo_flagged = load_staging(db_conn, "new-york", "2025-01-01")
        assert count == 3
        assert excluded == 0

    def test_staging_has_price_amount(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        load_staging(db_conn, "new-york", "2025-01-01")
        row = db_conn.execute(
            "SELECT price_amount FROM stg_listings WHERE id = 1"
        ).fetchone()
        assert row[0] == 150.0

    def test_deduplicates(self, db_conn, sample_csv):
        # Load twice to create duplicates in raw
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        raw_count = db_conn.execute("SELECT COUNT(*) FROM raw_listings").fetchone()[0]
        assert raw_count == 6  # doubled

        count, _, _ = load_staging(db_conn, "new-york", "2025-01-01")
        assert count == 3  # deduplicated

    def test_excludes_zero_price(self, db_conn, tmp_path):
        rows = SAMPLE_ROWS + [
            [4, "Free Place", "Harlem", "$0.00", "Private room", 1, 0, "2025-01-15",
             104, "Dave", "Harlem", "Manhattan",
             "Apartment", 1, 1, 1, "WiFi", 40.81, -73.95,
             10, 20, 30, 100, 0, 3.0, "f", "t", "t", "t", "f",
             "2021-01-01", "NY", "Hi", "within an hour", "100%", "90%", 1, 1, 1],
        ]
        path = write_csv(rows, COLUMNS, str(tmp_path / "listings.csv"))
        load_raw(db_conn, path, "new-york", "2025-01-01", 0)
        count, excluded, _ = load_staging(db_conn, "new-york", "2025-01-01")
        assert count == 3
        assert excluded == 1  # the $0.00 row

    def test_excludes_null_price(self, db_conn, tmp_path):
        rows = SAMPLE_ROWS + [
            [5, "No Price", "Harlem", None, "Private room", 1, 0, "2025-01-15",
             105, "Eve", "Harlem", "Manhattan",
             "Apartment", 1, 1, 1, "WiFi", 40.81, -73.95,
             10, 20, 30, 100, 0, 3.0, "f", "t", "t", "t", "f",
             "2021-01-01", "NY", "Hi", "within an hour", "100%", "90%", 1, 1, 1],
        ]
        path = write_csv(rows, COLUMNS, str(tmp_path / "listings.csv"))
        load_raw(db_conn, path, "new-york", "2025-01-01", 0)
        count, excluded, _ = load_staging(db_conn, "new-york", "2025-01-01")
        assert count == 3
        assert excluded == 1  # the null price row

    def test_geo_flag_stored_in_staging(self, db_conn, sample_csv):
        """All sample rows are within new-york bounding box — flag should be 0."""
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        count, _, geo_flagged = load_staging(db_conn, "new-york", "2025-01-01")
        assert count == 3
        assert geo_flagged == 0
        flagged = db_conn.execute(
            "SELECT COUNT(*) FROM stg_listings WHERE geo_out_of_city_flag = 1"
        ).fetchone()[0]
        assert flagged == 0

    def test_geo_flag_out_of_bounds(self, db_conn, tmp_path):
        """A row with lat/long far from new-york should be flagged."""
        rows = SAMPLE_ROWS + [
            [6, "Far Away", "Harlem", "$100.00", "Private room", 1, 0, "2025-01-15",
             106, "Zach", "Harlem", "Manhattan",
             "Apartment", 1, 1, 1, "WiFi", 34.05, -118.25,  # LA coordinates
             10, 20, 30, 100, 1.0, 4.0, "f", "t", "t", "t", "f",
             "2021-01-01", "NY", "Hi", "within an hour", "100%", "90%", 1, 1, 1],
        ]
        path = write_csv(rows, COLUMNS, str(tmp_path / "listings.csv"))
        load_raw(db_conn, path, "new-york", "2025-01-01", 0)
        count, _, geo_flagged = load_staging(db_conn, "new-york", "2025-01-01")
        assert count == 4
        assert geo_flagged == 1
        flagged = db_conn.execute(
            "SELECT COUNT(*) FROM stg_listings WHERE geo_out_of_city_flag = 1"
        ).fetchone()[0]
        assert flagged == 1


class TestArchiveFile:
    def test_archive_moves_file(self, tmp_path):
        src = tmp_path / "listings.csv"
        src.write_text("data")
        archived = archive_file(str(src), "new-york", "2025-01-01")
        assert not src.exists()
        assert os.path.exists(archived)
        assert "archive" in archived
        assert "new-york_2025-01-01" in archived

    def test_archive_creates_directory(self, tmp_path):
        src = tmp_path / "listings.csv.gz"
        src.write_text("data")
        archived = archive_file(str(src), "chicago", "2025-06-01")
        assert os.path.isdir(str(tmp_path / "archive"))
        assert archived.endswith(".csv.gz")


class TestDeriveSnapshotMonth:
    def test_derives_month(self, sample_csv):
        result = derive_snapshot_month(sample_csv)
        assert result == "2025-01-01"
