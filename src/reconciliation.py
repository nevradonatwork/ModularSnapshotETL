"""
Reconciliation layer for the BreezeBnb data platform.

Compares independently-calculated results from staging data against
the fact tables and reporting views to ensure data integrity.

Tables populated:
    rec_avg_price_comparison          – staging avg price vs fct_neighbourhood_monthly_avg_price
    rec_top10_price_delta_comparison  – staging top-10 deltas vs fct_neighbourhood_monthly_top10_price_delta
    rec_reporting_view_comparison     – fact table row counts vs reporting view row counts
"""
import logging
import sqlite3
from datetime import datetime, timezone

import pandas as pd

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _resolve_city_snapshot(conn, city_key, month_key):
    """Return (city_name, snapshot_month) from dimension keys."""
    city_name = conn.execute(
        "SELECT city_name FROM dim_city WHERE city_key = ?", (city_key,)
    ).fetchone()[0]
    year = month_key // 100
    month = month_key % 100
    return city_name, f"{year}-{month:02d}-01"


def _safe_float(val):
    return None if pd.isna(val) else float(val)


def _safe_int(val):
    return None if pd.isna(val) else int(val)


# ------------------------------------------------------------------
# Average Price Reconciliation
# ------------------------------------------------------------------

def reconcile_avg_price(
    conn: sqlite3.Connection,
    city_key: int,
    month_key: int,
) -> int:
    """Compare avg price per neighbourhood/room_type calculated from staging
    against fct_neighbourhood_monthly_avg_price.

    Returns rows inserted into rec_avg_price_comparison.
    """
    now = datetime.now(timezone.utc).isoformat()
    city_name, snapshot_month = _resolve_city_snapshot(conn, city_key, month_key)

    stg = _query_staging_avg_prices(conn, city_name, snapshot_month)
    fct = _query_fact_avg_prices(conn, month_key, city_key)

    conn.execute(
        "DELETE FROM rec_avg_price_comparison WHERE month_key = ? AND city_key = ?",
        (month_key, city_key),
    )

    merged = _outer_merge(stg, fct, ["neighbourhood_key", "room_type"])
    total = 0

    for _, row in merged.iterrows():
        nb_key = row["neighbourhood_key"]
        if pd.isna(nb_key):
            continue

        stg_avg = _safe_float(row.get("stg_avg_price"))
        fct_avg = _safe_float(row.get("fct_avg_price"))
        stg_cnt = _safe_int(row.get("stg_listing_count"))
        fct_cnt = _safe_int(row.get("fct_listing_count"))

        status = _merge_status(row, lambda: stg_avg == fct_avg and stg_cnt == fct_cnt)
        price_diff = round(stg_avg - fct_avg, 2) if stg_avg is not None and fct_avg is not None else None
        price_diff_pct = round((price_diff / fct_avg) * 100, 2) if price_diff is not None and fct_avg else None
        count_diff = (stg_cnt - fct_cnt) if stg_cnt is not None and fct_cnt is not None else None

        conn.execute(
            """INSERT INTO rec_avg_price_comparison
               (month_key, city_key, neighbourhood_key, room_type,
                stg_avg_price, stg_listing_count, fct_avg_price, fct_listing_count,
                price_diff, price_diff_pct, count_diff, match_status, checked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (month_key, city_key, int(nb_key), row["room_type"],
             stg_avg, stg_cnt, fct_avg, fct_cnt,
             price_diff, price_diff_pct, count_diff, status, now),
        )
        total += 1

    conn.commit()
    logger.info("Reconciled %d avg price rows (month_key=%d, city_key=%d)", total, month_key, city_key)
    return total


def _query_staging_avg_prices(conn, city_name, snapshot_month):
    """Calculate avg prices directly from staging, by room type and ALL."""
    by_room = pd.read_sql_query(
        """SELECT dn.neighbourhood_key, dl.room_type,
                  ROUND(AVG(s.price_amount), 2) AS avg_price, COUNT(*) AS listing_count
           FROM stg_listings s
           JOIN dim_city dc ON s.city = dc.city_name
           JOIN dim_neighbourhood dn
               ON dc.city_key = dn.city_key
               AND s.neighbourhood_cleansed = dn.neighbourhood_cleansed
           JOIN dim_listing dl ON s.id = dl.listing_id AND dl.is_current = 1
           WHERE s.city = ? AND s.snapshot_month = ?
                 AND s.geo_out_of_city_flag = 0
                 AND s.price_amount IS NOT NULL
                 AND dl.room_type IS NOT NULL
           GROUP BY dn.neighbourhood_key, dl.room_type""",
        conn,
        params=(city_name, snapshot_month),
    )

    all_rooms = pd.read_sql_query(
        """SELECT dn.neighbourhood_key, 'ALL' AS room_type,
                  ROUND(AVG(s.price_amount), 2) AS avg_price, COUNT(*) AS listing_count
           FROM stg_listings s
           JOIN dim_city dc ON s.city = dc.city_name
           JOIN dim_neighbourhood dn
               ON dc.city_key = dn.city_key
               AND s.neighbourhood_cleansed = dn.neighbourhood_cleansed
           WHERE s.city = ? AND s.snapshot_month = ?
                 AND s.geo_out_of_city_flag = 0
                 AND s.price_amount IS NOT NULL
           GROUP BY dn.neighbourhood_key""",
        conn,
        params=(city_name, snapshot_month),
    )

    return pd.concat([by_room, all_rooms], ignore_index=True)


def _query_fact_avg_prices(conn, month_key, city_key):
    """Read existing fact avg prices."""
    return pd.read_sql_query(
        """SELECT neighbourhood_key, room_type, avg_price, listing_count
           FROM fct_neighbourhood_monthly_avg_price
           WHERE month_key = ? AND city_key = ?""",
        conn,
        params=(month_key, city_key),
    )


# ------------------------------------------------------------------
# Top-10 Price Delta Reconciliation
# ------------------------------------------------------------------

def reconcile_top10_price_delta(
    conn: sqlite3.Connection,
    city_key: int,
    month_key: int,
    n: int = 10,
) -> int:
    """Compare top-N overpriced/underpriced listings calculated from staging
    against fct_neighbourhood_monthly_top10_price_delta.

    Returns rows inserted into rec_top10_price_delta_comparison.
    """
    now = datetime.now(timezone.utc).isoformat()

    stg_avgs = pd.read_sql_query(
        """SELECT neighbourhood_key, room_type, stg_avg_price AS avg_price
           FROM rec_avg_price_comparison
           WHERE month_key = ? AND city_key = ? AND stg_avg_price IS NOT NULL""",
        conn, params=(month_key, city_key),
    )
    if stg_avgs.empty:
        return 0

    facts = pd.read_sql_query(
        """SELECT f.neighbourhood_key, f.listing_key, f.price_amount, dl.room_type
           FROM fct_listing_monthly_snapshot f
           JOIN dim_listing dl ON f.listing_key = dl.listing_key
           WHERE f.month_key = ? AND f.city_key = ? AND f.price_amount IS NOT NULL""",
        conn, params=(month_key, city_key),
    )
    if facts.empty:
        return 0

    fct_top10 = pd.read_sql_query(
        """SELECT neighbourhood_key, listing_key, room_type, delta_type,
                  price_amount, neighbourhood_avg_price, price_delta,
                  price_delta_pct, rank_in_neighbourhood
           FROM fct_neighbourhood_monthly_top10_price_delta
           WHERE month_key = ? AND city_key = ?""",
        conn, params=(month_key, city_key),
    )

    conn.execute(
        "DELETE FROM rec_top10_price_delta_comparison WHERE month_key = ? AND city_key = ?",
        (month_key, city_key),
    )

    total = 0
    for rt in stg_avgs["room_type"].unique():
        total += _reconcile_room_type_deltas(
            conn, facts, stg_avgs, fct_top10, rt, n,
            month_key, city_key, now,
        )

    conn.commit()
    logger.info(
        "Reconciled %d top-10 price delta rows (month_key=%d, city_key=%d)",
        total, month_key, city_key,
    )
    return total


def _reconcile_room_type_deltas(conn, facts, stg_avgs, fct_top10, rt, n,
                                month_key, city_key, now):
    """Reconcile top-N deltas for a single room_type. Returns rows inserted."""
    avgs_rt = stg_avgs[stg_avgs["room_type"] == rt][["neighbourhood_key", "avg_price"]]
    facts_rt = facts.copy() if rt == "ALL" else facts[facts["room_type"] == rt].copy()
    if facts_rt.empty:
        return 0

    merged = facts_rt.merge(avgs_rt, on="neighbourhood_key")
    merged["price_delta"] = (merged["price_amount"] - merged["avg_price"]).round(2)
    merged["price_delta_pct"] = (
        (merged["price_delta"] / merged["avg_price"]) * 100
    ).round(2)

    total = 0
    for nb_key in merged["neighbourhood_key"].unique():
        nb_data = merged[merged["neighbourhood_key"] == nb_key]
        for delta_type, ascending in [("OVERPRICED", False), ("UNDERPRICED", True)]:
            top_n = nb_data.sort_values("price_delta", ascending=ascending).head(n).copy()
            top_n["rank"] = range(1, len(top_n) + 1)

            for _, stg_row in top_n.iterrows():
                total += _insert_delta_comparison(
                    conn, stg_row, fct_top10, nb_key, rt, delta_type,
                    month_key, city_key, now,
                )
    return total


def _insert_delta_comparison(conn, stg_row, fct_top10, nb_key, rt, delta_type,
                             month_key, city_key, now):
    """Insert one rec_top10_price_delta_comparison row. Returns 1."""
    listing_key = int(stg_row["listing_key"])
    stg_delta = float(stg_row["price_delta"])
    stg_rank = int(stg_row["rank"])

    fct_match = fct_top10[
        (fct_top10["neighbourhood_key"] == nb_key)
        & (fct_top10["listing_key"] == listing_key)
        & (fct_top10["room_type"] == rt)
        & (fct_top10["delta_type"] == delta_type)
    ]

    if fct_match.empty:
        status = "STG_ONLY"
        fct_price = fct_avg = fct_delta = fct_delta_pct = fct_rank = None
    else:
        fct_r = fct_match.iloc[0]
        fct_price = float(fct_r["price_amount"])
        fct_avg = float(fct_r["neighbourhood_avg_price"])
        fct_delta = float(fct_r["price_delta"])
        fct_delta_pct = float(fct_r["price_delta_pct"])
        fct_rank = int(fct_r["rank_in_neighbourhood"])
        status = "MATCH" if stg_delta == fct_delta and stg_rank == fct_rank else "MISMATCH"

    price_delta_diff = round(stg_delta - fct_delta, 2) if fct_delta is not None else None
    rank_diff = stg_rank - fct_rank if fct_rank is not None else None

    conn.execute(
        """INSERT INTO rec_top10_price_delta_comparison
           (month_key, city_key, neighbourhood_key, listing_key,
            room_type, delta_type,
            stg_price_amount, stg_neighbourhood_avg_price,
            stg_price_delta, stg_price_delta_pct, stg_rank,
            fct_price_amount, fct_neighbourhood_avg_price,
            fct_price_delta, fct_price_delta_pct, fct_rank,
            price_delta_diff, rank_diff, match_status, checked_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (month_key, city_key, int(nb_key), listing_key,
         rt, delta_type,
         float(stg_row["price_amount"]), float(stg_row["avg_price"]),
         stg_delta, float(stg_row["price_delta_pct"]), stg_rank,
         fct_price, fct_avg, fct_delta, fct_delta_pct, fct_rank,
         price_delta_diff, rank_diff, status, now),
    )
    return 1


# ------------------------------------------------------------------
# Reporting View Reconciliation
# ------------------------------------------------------------------

_VIEW_COMPARISONS = [
    (
        "vw_rep_monthly_neighbourhood_avg_price",
        "SELECT COUNT(*) FROM fct_neighbourhood_monthly_avg_price WHERE month_key = ? AND city_key = ?",
        "SELECT COUNT(*) FROM vw_rep_monthly_neighbourhood_avg_price WHERE year = ? AND month = ?",
    ),
    (
        "vw_rep_monthly_top10_overpriced",
        "SELECT COUNT(*) FROM fct_neighbourhood_monthly_top10_price_delta WHERE month_key = ? AND city_key = ? AND delta_type = 'OVERPRICED'",
        "SELECT COUNT(*) FROM vw_rep_monthly_top10_overpriced WHERE year = ? AND month = ?",
    ),
    (
        "vw_rep_monthly_top10_underpriced",
        "SELECT COUNT(*) FROM fct_neighbourhood_monthly_top10_price_delta WHERE month_key = ? AND city_key = ? AND delta_type = 'UNDERPRICED'",
        "SELECT COUNT(*) FROM vw_rep_monthly_top10_underpriced WHERE year = ? AND month = ?",
    ),
    (
        "vw_rep_monthly_data_compliance",
        "SELECT COUNT(*) FROM fct_data_compliance_monthly WHERE month_key = ? AND city_key = ?",
        "SELECT COUNT(*) FROM vw_rep_monthly_data_compliance WHERE year = ? AND month = ?",
    ),
]


def reconcile_reporting_views(
    conn: sqlite3.Connection,
    city_key: int,
    month_key: int,
) -> int:
    """Compare row counts between fact tables and their corresponding reporting views.

    Returns rows inserted into rec_reporting_view_comparison.
    """
    now = datetime.now(timezone.utc).isoformat()
    year = month_key // 100
    month = month_key % 100

    conn.execute(
        "DELETE FROM rec_reporting_view_comparison WHERE month_key = ? AND city_key = ?",
        (month_key, city_key),
    )

    total = 0
    for view_name, fct_sql, view_sql in _VIEW_COMPARISONS:
        fct_count = conn.execute(fct_sql, (month_key, city_key)).fetchone()[0]
        view_count = conn.execute(view_sql, (year, month)).fetchone()[0]
        count_diff = fct_count - view_count
        status = "MATCH" if count_diff == 0 else "MISMATCH"

        conn.execute(
            """INSERT INTO rec_reporting_view_comparison
               (month_key, city_key, view_name, fct_row_count, view_row_count,
                count_diff, match_status, checked_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (month_key, city_key, view_name, fct_count, view_count, count_diff, status, now),
        )
        total += 1

    conn.commit()
    logger.info(
        "Reconciled %d reporting view comparisons (month_key=%d, city_key=%d)",
        total, month_key, city_key,
    )
    return total


# ------------------------------------------------------------------
# Internal merge helpers
# ------------------------------------------------------------------

def _outer_merge(stg_df, fct_df, join_keys):
    """Full outer join with standard column renaming."""
    stg_cols = {"avg_price": "stg_avg_price", "listing_count": "stg_listing_count"}
    fct_cols = {"avg_price": "fct_avg_price", "listing_count": "fct_listing_count"}

    if not stg_df.empty:
        stg_df = stg_df.rename(columns=stg_cols)
    else:
        stg_df = pd.DataFrame(columns=join_keys + list(stg_cols.values()))

    if not fct_df.empty:
        fct_df = fct_df.rename(columns=fct_cols)
    else:
        fct_df = pd.DataFrame(columns=join_keys + list(fct_cols.values()))

    return stg_df.merge(fct_df, on=join_keys, how="outer", indicator=True)


def _merge_status(row, match_fn):
    """Determine MATCH/MISMATCH/STG_ONLY/FCT_ONLY from a merge indicator row."""
    if row["_merge"] == "left_only":
        return "STG_ONLY"
    if row["_merge"] == "right_only":
        return "FCT_ONLY"
    return "MATCH" if match_fn() else "MISMATCH"
