import pandas as pd
import pytest

from src.dimensions import (
    load_dim_date,
    load_dim_city,
    load_dim_neighbourhoods,
    load_dim_hosts,
    load_dim_listings,
    load_neighbourhood_reference_files,
    backfill_staging_neighbourhoods_from_dim_geo,
)
from src.ingestion import load_raw, load_staging


class TestDimDate:
    def test_creates_date_row(self, db_conn):
        month_key = load_dim_date(db_conn, "2025-01-01")
        assert month_key == 202501

        row = db_conn.execute(
            "SELECT year, month, month_start_date FROM dim_date WHERE month_key = ?",
            (month_key,),
        ).fetchone()
        assert row == (2025, 1, "2025-01-01")

    def test_idempotent(self, db_conn):
        k1 = load_dim_date(db_conn, "2025-01-01")
        k2 = load_dim_date(db_conn, "2025-01-01")
        assert k1 == k2

        count = db_conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
        assert count == 1


class TestDimCity:
    def test_creates_city(self, db_conn):
        key = load_dim_city(db_conn, "new-york")
        assert key > 0

        row = db_conn.execute(
            "SELECT city_name, currency_code FROM dim_city WHERE city_key = ?",
            (key,),
        ).fetchone()
        assert row[0] == "new-york"
        assert row[1] == "USD"

    def test_idempotent(self, db_conn):
        k1 = load_dim_city(db_conn, "new-york")
        k2 = load_dim_city(db_conn, "new-york")
        assert k1 == k2


class TestDimNeighbourhoods:
    def test_loads_neighbourhoods(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        load_staging(db_conn, "new-york", "2025-01-01")
        city_key = load_dim_city(db_conn, "new-york")

        stg_df = pd.read_sql_query(
            "SELECT * FROM stg_listings WHERE city = 'new-york'", db_conn
        )
        mapping = load_dim_neighbourhoods(db_conn, city_key, stg_df)

        assert "Williamsburg" in mapping
        assert "Harlem" in mapping
        assert len(mapping) == 2


class TestDimHosts:
    def test_loads_hosts(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        load_staging(db_conn, "new-york", "2025-01-01")

        stg_df = pd.read_sql_query(
            "SELECT * FROM stg_listings WHERE city = 'new-york'", db_conn
        )
        mapping = load_dim_hosts(db_conn, stg_df, "2025-01-01")

        assert len(mapping) == 3
        # All should be current
        current = db_conn.execute(
            "SELECT COUNT(*) FROM dim_host WHERE is_current = 1"
        ).fetchone()[0]
        assert current == 3


class TestNeighbourhoodReferenceFiles:
    def test_loads_csv_and_geojson_into_dim(self, db_conn, tmp_path):
        city_key = load_dim_city(db_conn, "new-orleans")

        (tmp_path / "neighbourhoods.csv").write_text(
            "neighbourhood_group,neighbourhood\nHistoric,Bywater\nHistoric,French Quarter\n",
            encoding="utf-8",
        )
        (tmp_path / "neighbourhoods.geojson").write_text(
            '{"type":"FeatureCollection","features":['
            '{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-90.01,29.95],[-89.99,29.95],[-89.99,29.97],[-90.01,29.97],[-90.01,29.95]]]},"properties":{"neighbourhood":"Bywater","neighbourhood_group":"Historic"}}'
            ']}',
            encoding="utf-8",
        )

        csv_count, geo_count = load_neighbourhood_reference_files(db_conn, city_key, str(tmp_path))
        assert csv_count == 2
        assert geo_count == 1

        row = db_conn.execute(
            """SELECT neighbourhood_group_cleansed, geometry_type, geometry_json
               FROM dim_neighbourhood
               WHERE city_key = ? AND neighbourhood_cleansed = 'Bywater'""",
            (city_key,),
        ).fetchone()
        assert row[0] == "Historic"
        assert row[1] == "Polygon"
        assert row[2] is not None

    def test_backfills_missing_staging_neighbourhood_from_geo(self, db_conn, sample_csv, tmp_path):
        load_raw(db_conn, sample_csv, "new-orleans", "2025-01-01", 0)
        load_staging(db_conn, "new-orleans", "2025-01-01")

        db_conn.execute("UPDATE stg_listings SET neighbourhood_cleansed = NULL, neighbourhood = NULL, neighbourhood_group_cleansed = NULL WHERE id = 1")
        db_conn.execute("UPDATE stg_listings SET latitude = 29.96, longitude = -90.0 WHERE id = 1")
        db_conn.commit()

        city_key = load_dim_city(db_conn, "new-orleans")
        (tmp_path / "neighbourhoods.csv").write_text(
            "neighbourhood_group,neighbourhood\nHistoric,Bywater\n",
            encoding="utf-8",
        )
        (tmp_path / "neighbourhoods.geojson").write_text(
            '{"type":"FeatureCollection","features":['
            '{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[-90.01,29.95],[-89.99,29.95],[-89.99,29.97],[-90.01,29.97],[-90.01,29.95]]]},"properties":{"neighbourhood":"Bywater","neighbourhood_group":"Historic"}}'
            ']}',
            encoding="utf-8",
        )

        load_neighbourhood_reference_files(db_conn, city_key, str(tmp_path))
        updated = backfill_staging_neighbourhoods_from_dim_geo(
            db_conn, "new-orleans", "2025-01-01", city_key
        )
        assert updated == 1

        row = db_conn.execute(
            "SELECT neighbourhood_cleansed, neighbourhood_group_cleansed FROM stg_listings WHERE id = 1"
        ).fetchone()
        assert row == ("Bywater", "Historic")


class TestDimListings:
    def test_loads_listings(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        load_staging(db_conn, "new-york", "2025-01-01")

        stg_df = pd.read_sql_query(
            "SELECT * FROM stg_listings WHERE city = 'new-york'", db_conn
        )
        mapping = load_dim_listings(db_conn, stg_df, "2025-01-01")

        assert len(mapping) == 3
        current = db_conn.execute(
            "SELECT COUNT(*) FROM dim_listing WHERE is_current = 1"
        ).fetchone()[0]
        assert current == 3

    def test_scd2_creates_new_version(self, db_conn, sample_csv):
        load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
        load_staging(db_conn, "new-york", "2025-01-01")

        stg_df = pd.read_sql_query(
            "SELECT * FROM stg_listings WHERE city = 'new-york'", db_conn
        )
        load_dim_listings(db_conn, stg_df, "2025-01-01")

        # Modify a listing attribute and reload
        db_conn.execute(
            "UPDATE stg_listings SET room_type = 'Hotel room' WHERE id = 1"
        )
        db_conn.commit()
        stg_df2 = pd.read_sql_query(
            "SELECT * FROM stg_listings WHERE city = 'new-york'", db_conn
        )
        load_dim_listings(db_conn, stg_df2, "2025-02-01")

        # Should have 4 total rows (3 original + 1 new version for id=1)
        total = db_conn.execute(
            "SELECT COUNT(*) FROM dim_listing"
        ).fetchone()[0]
        assert total == 4

        # Only 3 current
        current = db_conn.execute(
            "SELECT COUNT(*) FROM dim_listing WHERE is_current = 1"
        ).fetchone()[0]
        assert current == 3
