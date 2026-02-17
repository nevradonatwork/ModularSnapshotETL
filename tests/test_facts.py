import pandas as pd
import pytest

from src.ingestion import load_raw, load_staging
from src.dimensions import (
    load_dim_date,
    load_dim_city,
    load_dim_neighbourhoods,
    load_dim_hosts,
    load_dim_listings,
)
from src.facts import (
    load_fct_listing_monthly_snapshot,
    load_fct_neighbourhood_monthly_avg_price,
    load_fct_neighbourhood_monthly_top10_price_delta,
    load_fct_data_compliance_monthly,
)


def _load_all_dims(db_conn, sample_csv):
    """Helper: load raw, staging, and all dimensions. Returns keys and maps."""
    load_raw(db_conn, sample_csv, "new-york", "2025-01-01", 0)
    load_staging(db_conn, "new-york", "2025-01-01")

    month_key = load_dim_date(db_conn, "2025-01-01")
    city_key = load_dim_city(db_conn, "new-york")

    stg_df = pd.read_sql_query(
        "SELECT * FROM stg_listings WHERE city = 'new-york'", db_conn
    )
    neighbourhood_map = load_dim_neighbourhoods(db_conn, city_key, stg_df)
    host_map = load_dim_hosts(db_conn, stg_df, "2025-01-01")
    listing_map = load_dim_listings(db_conn, stg_df, "2025-01-01")

    return month_key, city_key, neighbourhood_map, listing_map, host_map


class TestFctListingMonthlySnapshot:
    def test_loads_facts(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        count = load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        assert count == 3

    def test_fact_prices_match_staging(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )

        prices = db_conn.execute(
            "SELECT price_amount FROM fct_listing_monthly_snapshot ORDER BY price_amount"
        ).fetchall()
        assert [p[0] for p in prices] == [40.0, 75.5, 150.0]


class TestFctNeighbourhoodMonthlyAvgPrice:
    def test_computes_averages(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        count = load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)
        # 3 per-room-type rows + 2 ALL rows = 5
        assert count == 5

    def test_williamsburg_avg(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)

        # Williamsburg ALL: (150 + 40) / 2 = 95
        wburg_key = n_map["Williamsburg"]
        row = db_conn.execute(
            """SELECT avg_price, listing_count FROM fct_neighbourhood_monthly_avg_price
               WHERE neighbourhood_key = ? AND room_type = 'ALL'""",
            (wburg_key,),
        ).fetchone()
        assert row[0] == 95.0
        assert row[1] == 2


class TestFctTop10PriceDelta:
    def test_produces_over_and_under(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)
        count = load_fct_neighbourhood_monthly_top10_price_delta(
            db_conn, city_key, month_key, n=10
        )
        assert count > 0

        types = db_conn.execute(
            "SELECT DISTINCT delta_type FROM fct_neighbourhood_monthly_top10_price_delta"
        ).fetchall()
        assert set(t[0] for t in types) == {"OVERPRICED", "UNDERPRICED"}


class TestFctDataComplianceMonthly:
    def test_compliance_counts_all_valid_rows(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        count = load_fct_data_compliance_monthly(db_conn, city_key, month_key)
        assert count == 1

        row = db_conn.execute(
            """SELECT rows_count, compliance_data_count, missing_price_count,
                      missing_neighbourhood_cleansed_count, missing_room_type_count
               FROM fct_data_compliance_monthly
               WHERE month_key = ? AND city_key = ?""",
            (month_key, city_key),
        ).fetchone()
        assert row == (3, 3, 0, 0, 0)

    def test_compliance_counts_missing_fields(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)

        db_conn.execute("UPDATE stg_listings SET room_type = NULL WHERE id = 1")
        db_conn.execute("UPDATE stg_listings SET neighbourhood_cleansed = NULL WHERE id = 2")
        db_conn.execute("UPDATE stg_listings SET price_amount = NULL WHERE id = 3")
        db_conn.commit()

        load_fct_data_compliance_monthly(db_conn, city_key, month_key)

        row = db_conn.execute(
            """SELECT rows_count, compliance_data_count, missing_price_count,
                      missing_neighbourhood_cleansed_count, missing_room_type_count
               FROM fct_data_compliance_monthly
               WHERE month_key = ? AND city_key = ?""",
            (month_key, city_key),
        ).fetchone()
        assert row == (3, 0, 1, 1, 1)




class TestPresentationViews:
    def test_avg_price_view(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)

        rows = db_conn.execute(
            "SELECT * FROM vw_rep_monthly_neighbourhood_avg_price"
        ).fetchall()
        # 3 per-room-type rows + 2 ALL rows = 5
        assert len(rows) == 5

    def test_overpriced_view(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)
        load_fct_neighbourhood_monthly_top10_price_delta(
            db_conn, city_key, month_key
        )

        rows = db_conn.execute(
            "SELECT * FROM vw_rep_monthly_top10_overpriced"
        ).fetchall()
        assert len(rows) > 0

    def test_underpriced_view(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_listing_monthly_snapshot(
            db_conn, city_key, month_key, n_map, l_map, h_map
        )
        load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)
        load_fct_neighbourhood_monthly_top10_price_delta(
            db_conn, city_key, month_key
        )

        rows = db_conn.execute(
            "SELECT * FROM vw_rep_monthly_top10_underpriced"
        ).fetchall()
        assert len(rows) > 0


    def test_data_compliance_view(self, db_conn, sample_csv):
        month_key, city_key, n_map, l_map, h_map = _load_all_dims(db_conn, sample_csv)
        load_fct_data_compliance_monthly(db_conn, city_key, month_key)

        row = db_conn.execute(
            """SELECT city_name, rows_count, compliance_data_count
               FROM vw_rep_monthly_data_compliance"""
        ).fetchone()
        assert row == ("new-york", 3, 3)
