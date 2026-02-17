"""Tests for the reconciliation layer."""
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
from src.reconciliation import (
    reconcile_avg_price,
    reconcile_top10_price_delta,
    reconcile_reporting_views,
)


def _load_full_pipeline(db_conn, sample_csv):
    """Helper: load raw through facts. Returns (month_key, city_key)."""
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

    load_fct_listing_monthly_snapshot(
        db_conn, city_key, month_key, neighbourhood_map, listing_map, host_map
    )
    load_fct_neighbourhood_monthly_avg_price(db_conn, city_key, month_key)
    load_fct_neighbourhood_monthly_top10_price_delta(db_conn, city_key, month_key)
    load_fct_data_compliance_monthly(db_conn, city_key, month_key)

    return month_key, city_key


class TestReconcileAvgPrice:
    def test_all_match(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        count = reconcile_avg_price(db_conn, city_key, month_key)
        assert count > 0

        mismatches = db_conn.execute(
            """SELECT COUNT(*) FROM rec_avg_price_comparison
               WHERE match_status != 'MATCH'"""
        ).fetchone()[0]
        assert mismatches == 0

    def test_detects_mismatch_when_fct_tampered(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)

        # Tamper with a fact table avg_price
        db_conn.execute(
            """UPDATE fct_neighbourhood_monthly_avg_price
               SET avg_price = avg_price + 999
               WHERE room_type = 'ALL'
               LIMIT 1"""
        )
        db_conn.commit()

        reconcile_avg_price(db_conn, city_key, month_key)

        mismatches = db_conn.execute(
            """SELECT COUNT(*) FROM rec_avg_price_comparison
               WHERE match_status = 'MISMATCH'"""
        ).fetchone()[0]
        assert mismatches >= 1

    def test_records_correct_price_values(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_avg_price(db_conn, city_key, month_key)

        rows = db_conn.execute(
            """SELECT room_type, stg_avg_price, fct_avg_price, price_diff
               FROM rec_avg_price_comparison
               WHERE match_status = 'MATCH'"""
        ).fetchall()
        for row in rows:
            assert row[3] == 0.0 or row[3] is None  # price_diff should be 0 for matches

    def test_idempotent(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_avg_price(db_conn, city_key, month_key)
        count1 = db_conn.execute("SELECT COUNT(*) FROM rec_avg_price_comparison").fetchone()[0]

        reconcile_avg_price(db_conn, city_key, month_key)
        count2 = db_conn.execute("SELECT COUNT(*) FROM rec_avg_price_comparison").fetchone()[0]
        assert count1 == count2


class TestReconcileTop10PriceDelta:
    def test_all_match(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_avg_price(db_conn, city_key, month_key)
        count = reconcile_top10_price_delta(db_conn, city_key, month_key)
        assert count > 0

        mismatches = db_conn.execute(
            """SELECT COUNT(*) FROM rec_top10_price_delta_comparison
               WHERE match_status != 'MATCH'"""
        ).fetchone()[0]
        assert mismatches == 0

    def test_has_both_over_and_underpriced(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_avg_price(db_conn, city_key, month_key)
        reconcile_top10_price_delta(db_conn, city_key, month_key)

        types = db_conn.execute(
            "SELECT DISTINCT delta_type FROM rec_top10_price_delta_comparison"
        ).fetchall()
        assert set(t[0] for t in types) == {"OVERPRICED", "UNDERPRICED"}

    def test_idempotent(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_avg_price(db_conn, city_key, month_key)
        reconcile_top10_price_delta(db_conn, city_key, month_key)
        count1 = db_conn.execute("SELECT COUNT(*) FROM rec_top10_price_delta_comparison").fetchone()[0]

        reconcile_top10_price_delta(db_conn, city_key, month_key)
        count2 = db_conn.execute("SELECT COUNT(*) FROM rec_top10_price_delta_comparison").fetchone()[0]
        assert count1 == count2


class TestReconcileReportingViews:
    def test_all_views_match(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        count = reconcile_reporting_views(db_conn, city_key, month_key)
        assert count == 4  # 4 views compared

        mismatches = db_conn.execute(
            """SELECT COUNT(*) FROM rec_reporting_view_comparison
               WHERE match_status != 'MATCH'"""
        ).fetchone()[0]
        assert mismatches == 0

    def test_view_names_recorded(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_reporting_views(db_conn, city_key, month_key)

        views = db_conn.execute(
            "SELECT view_name FROM rec_reporting_view_comparison ORDER BY view_name"
        ).fetchall()
        expected = [
            "vw_rep_monthly_data_compliance",
            "vw_rep_monthly_neighbourhood_avg_price",
            "vw_rep_monthly_top10_overpriced",
            "vw_rep_monthly_top10_underpriced",
        ]
        assert [v[0] for v in views] == expected

    def test_idempotent(self, db_conn, sample_csv):
        month_key, city_key = _load_full_pipeline(db_conn, sample_csv)
        reconcile_reporting_views(db_conn, city_key, month_key)
        count1 = db_conn.execute("SELECT COUNT(*) FROM rec_reporting_view_comparison").fetchone()[0]

        reconcile_reporting_views(db_conn, city_key, month_key)
        count2 = db_conn.execute("SELECT COUNT(*) FROM rec_reporting_view_comparison").fetchone()[0]
        assert count1 == count2
