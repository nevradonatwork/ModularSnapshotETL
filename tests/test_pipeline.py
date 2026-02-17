import os
import sqlite3

import pandas as pd
import pytest

from src.schema import create_all
from src.pipeline import run
from tests.conftest import COLUMNS, SAMPLE_ROWS, write_csv


class TestPipeline:
    def test_run_populates_all_layers(self, db_conn, sample_csv):
        row_counts = run(db_conn, sample_csv, "new-york")

        assert row_counts["raw_listings"] == 3
        assert row_counts["stg_listings"] == 3
        assert row_counts["fct_listing_monthly_snapshot"] == 3
        assert row_counts["fct_neighbourhood_monthly_avg_price"] == 5
        assert row_counts["fct_neighbourhood_monthly_top10_price_delta"] > 0
        assert row_counts["fct_data_compliance_monthly"] == 1
        assert row_counts["dim_neighbourhood_reference_csv"] == 0
        assert row_counts["dim_neighbourhood_reference_geojson"] == 0

    def test_run_raises_on_missing_file(self, db_conn, tmp_path):
        with pytest.raises(FileNotFoundError):
            run(db_conn, str(tmp_path / "nope.csv"), "test")

    def test_etl_run_logged(self, db_conn, sample_csv):
        run(db_conn, sample_csv, "new-york")

        row = db_conn.execute(
            "SELECT status, city, source_file_name FROM etl_run_log ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        assert row[0] == "SUCCESS"
        assert row[1] == "new-york"
        assert row[2] is not None

    def test_failed_run_logged(self, db_conn, tmp_path):
        with pytest.raises(FileNotFoundError):
            run(db_conn, str(tmp_path / "nope.csv"), "test")

        row = db_conn.execute(
            "SELECT status, city FROM etl_run_log ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        assert row[0] == "FAILED"
        assert row[1] == "test"

    def test_two_cities(self, db_conn, tmp_path):
        for city in ["new-york", "chicago"]:
            path = write_csv(SAMPLE_ROWS, COLUMNS, str(tmp_path / f"{city}.csv"))
            run(db_conn, path, city)

        cities = db_conn.execute(
            "SELECT DISTINCT city_name FROM dim_city ORDER BY city_name"
        ).fetchall()
        assert [c[0] for c in cities] == ["chicago", "new-york"]

    def test_reporting_views_populated(self, db_conn, sample_csv):
        run(db_conn, sample_csv, "new-york")

        avg_rows = db_conn.execute(
            "SELECT COUNT(*) FROM vw_rep_monthly_neighbourhood_avg_price"
        ).fetchone()[0]
        assert avg_rows > 0

        over_rows = db_conn.execute(
            "SELECT COUNT(*) FROM vw_rep_monthly_top10_overpriced"
        ).fetchone()[0]
        assert over_rows > 0

    def test_archived_file_path_logged(self, db_conn, sample_csv):
        run(db_conn, sample_csv, "new-york")

        row = db_conn.execute(
            "SELECT archived_file_path FROM etl_run_log ORDER BY run_id DESC LIMIT 1"
        ).fetchone()
        assert row[0] is not None
        assert "archive" in row[0]

    def test_source_file_archived_after_run(self, db_conn, sample_csv):
        original_path = sample_csv
        run(db_conn, original_path, "new-york")
        assert not os.path.exists(original_path)

    def test_geo_out_of_city_count_in_row_counts(self, db_conn, sample_csv):
        row_counts = run(db_conn, sample_csv, "new-york")
        assert "geo_out_of_city_count" in row_counts
        assert row_counts["geo_out_of_city_count"] == 0
