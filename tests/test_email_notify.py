"""Tests for the email notification module — reconciliation summary."""
import json
from unittest.mock import patch

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
from src.email_notify import (
    _get_reconciliation_summary,
    _build_email_body,
)
from src.etl_logging import start_run, finish_run


def _load_full_pipeline(db_conn, sample_csv):
    """Load raw through facts + reconciliation. Returns (month_key, city_key)."""
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

    reconcile_avg_price(db_conn, city_key, month_key)
    reconcile_top10_price_delta(db_conn, city_key, month_key)
    reconcile_reporting_views(db_conn, city_key, month_key)

    return month_key, city_key


class TestGetReconciliationSummary:
    def test_all_match_after_clean_pipeline(self, db_conn, sample_csv):
        _load_full_pipeline(db_conn, sample_csv)
        summary = _get_reconciliation_summary(db_conn)

        assert summary["avg_price"]["total"] > 0
        assert summary["avg_price"]["mismatch"] == 0
        assert summary["top10_delta"]["total"] > 0
        assert summary["top10_delta"]["mismatch"] == 0
        assert summary["views"]["total"] == 4
        assert summary["views"]["mismatch"] == 0
        assert summary["mismatches"] == []

    def test_detects_mismatch_after_tampering(self, db_conn, sample_csv):
        _load_full_pipeline(db_conn, sample_csv)

        # Tamper with a fact avg price to force mismatch on next reconcile
        db_conn.execute(
            "UPDATE fct_neighbourhood_monthly_avg_price SET avg_price = avg_price + 999"
        )
        db_conn.commit()

        # Re-reconcile
        month_key, city_key = 202501, db_conn.execute(
            "SELECT city_key FROM dim_city WHERE city_name = 'new-york'"
        ).fetchone()[0]
        reconcile_avg_price(db_conn, city_key, month_key)

        summary = _get_reconciliation_summary(db_conn)
        assert summary["avg_price"]["mismatch"] > 0
        assert len(summary["mismatches"]) > 0

    def test_empty_when_no_rec_data(self, db_conn):
        summary = _get_reconciliation_summary(db_conn)
        assert summary["avg_price"]["total"] == 0
        assert summary["top10_delta"]["total"] == 0
        assert summary["views"]["total"] == 0
        assert summary["mismatches"] == []


class TestBuildEmailBodyReconciliation:
    def _make_run_info(self):
        return {
            "run_id": 1,
            "status": "SUCCESS",
            "start_time": "2025-01-15T10:00:00",
            "end_time": "2025-01-15T10:01:00",
            "row_counts": json.dumps({"raw_listings": 3}),
            "error_message": None,
        }

    def test_includes_reconciliation_section(self):
        rec = {
            "avg_price": {"total": 5, "match": 5, "mismatch": 0, "stg_only": 0, "fct_only": 0},
            "top10_delta": {"total": 10, "match": 10, "mismatch": 0, "stg_only": 0, "fct_only": 0},
            "views": {"total": 4, "match": 4, "mismatch": 0},
            "mismatches": [],
        }
        body = _build_email_body(self._make_run_info(), [], ["new-york"], [], rec)

        assert "Reconciliation (ALL PASSED):" in body
        assert "5/5 match" in body
        assert "10/10 match" in body
        assert "4/4 match" in body

    def test_shows_issues_detected(self):
        rec = {
            "avg_price": {"total": 5, "match": 3, "mismatch": 2, "stg_only": 0, "fct_only": 0},
            "top10_delta": {"total": 10, "match": 10, "mismatch": 0, "stg_only": 0, "fct_only": 0},
            "views": {"total": 4, "match": 4, "mismatch": 0},
            "mismatches": ["Avg price: new-york/Harlem (ALL) — stg=75.5, fct=100.0, diff=-24.5"],
        }
        body = _build_email_body(self._make_run_info(), [], ["new-york"], [], rec)

        assert "ISSUES DETECTED" in body
        assert "3/5 match" in body
        assert "2 mismatch" in body
        assert "Mismatch Details:" in body
        assert "new-york/Harlem" in body

    def test_no_rec_section_when_none(self):
        body = _build_email_body(self._make_run_info(), [], ["new-york"], [])
        assert "Reconciliation" not in body
