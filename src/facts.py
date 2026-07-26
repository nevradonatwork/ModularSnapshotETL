"""
Fact table loaders for the ModularSnapshotETL data platform.

Loads the base fact (fct_listing_monthly_snapshot) and the aggregated
facts (fct_neighbourhood_monthly_avg_price, fct_neighbourhood_monthly_top10_price_delta).
"""
import logging

import pandas as pd

from src import db

logger = logging.getLogger(__name__)


def load_fct_listing_monthly_snapshot(
    conn,
    city_key: int,
    month_key: int,
    neighbourhood_map: dict,
    listing_map: dict,
    host_map: dict,
) -> int:
    """Load the base fact table from stg_listings.

    Returns the number of rows inserted.
    """
    city_row = conn.execute(
        "SELECT city_name FROM dim_city WHERE city_key = ?", (city_key,)
    ).fetchone()
    city_name = city_row[0] if city_row else None

    # Derive snapshot_month from month_key
    year = month_key // 100
    month = month_key % 100
    snapshot_month = f"{year}-{month:02d}-01"

    stg = db.read_sql(
        conn,
        """SELECT * FROM stg_listings
           WHERE city = ? AND snapshot_month = ? AND geo_out_of_city_flag = 0""",
        (city_name, snapshot_month),
    )
    if stg.empty:
        return 0

    # Delete existing facts for this month/city
    conn.execute(
        "DELETE FROM fct_listing_monthly_snapshot WHERE month_key = ? AND city_key = ?",
        (month_key, city_key),
    )

    records = []
    for _, row in stg.iterrows():
        lid = int(row["id"]) if pd.notna(row.get("id")) else None
        hid = int(row["host_id"]) if pd.notna(row.get("host_id")) else None
        nc = row.get("neighbourhood_cleansed")

        listing_key = listing_map.get(lid)
        host_key = host_map.get(hid)
        neighbourhood_key = neighbourhood_map.get(nc)

        if not all([listing_key, host_key, neighbourhood_key]):
            continue

        records.append({
            "month_key": month_key,
            "city_key": city_key,
            "neighbourhood_key": neighbourhood_key,
            "listing_key": listing_key,
            "host_key": host_key,
            "price_amount": _safe(row.get("price_amount")),
            "availability_30": _safe_int(row.get("availability_30")),
            "availability_60": _safe_int(row.get("availability_60")),
            "availability_90": _safe_int(row.get("availability_90")),
            "availability_365": _safe_int(row.get("availability_365")),
            "number_of_reviews": _safe_int(row.get("number_of_reviews")),
            "reviews_per_month": _safe(row.get("reviews_per_month")),
            "review_scores_rating": _safe(row.get("review_scores_rating")),
        })

    # One batched upsert instead of one execute() per row -- against a
    # remote Postgres, each round trip pays full network latency, so this
    # matters a lot for cities with thousands of listings.
    insert_df = pd.DataFrame.from_records(records)
    db.bulk_upsert(
        conn, "fct_listing_monthly_snapshot", insert_df,
        conflict_cols=["month_key", "city_key", "listing_key"],
    )
    conn.commit()  # ensures the DELETE above lands even if insert_df is empty
    rows_inserted = len(insert_df)

    logger.info(
        "Loaded %d rows into fct_listing_monthly_snapshot (month_key=%d, city_key=%d)",
        rows_inserted, month_key, city_key,
    )
    return rows_inserted


def load_fct_neighbourhood_monthly_avg_price(
    conn,
    city_key: int,
    month_key: int,
) -> int:
    """Aggregate neighbourhood average prices from the base fact table.

    Calculates averages per room type AND an 'ALL' row combining all types.
    Returns rows inserted.
    """
    conn.execute(
        "DELETE FROM fct_neighbourhood_monthly_avg_price WHERE month_key = ? AND city_key = ?",
        (month_key, city_key),
    )

    total = 0

    # Per room type averages
    cur = conn.execute(
        """INSERT INTO fct_neighbourhood_monthly_avg_price
           (month_key, city_key, neighbourhood_key, room_type, avg_price, listing_count)
           SELECT
               f.month_key,
               f.city_key,
               f.neighbourhood_key,
               dl.room_type,
               ROUND(CAST(AVG(f.price_amount) AS NUMERIC), 2),
               COUNT(*)
           FROM fct_listing_monthly_snapshot f
           JOIN dim_listing dl ON f.listing_key = dl.listing_key
           WHERE f.month_key = ? AND f.city_key = ? AND f.price_amount IS NOT NULL
                 AND dl.room_type IS NOT NULL
           GROUP BY f.month_key, f.city_key, f.neighbourhood_key, dl.room_type""",
        (month_key, city_key),
    )
    total += cur.rowcount

    # ALL room types combined (neighbourhood-level average)
    cur = conn.execute(
        """INSERT INTO fct_neighbourhood_monthly_avg_price
           (month_key, city_key, neighbourhood_key, room_type, avg_price, listing_count)
           SELECT
               month_key,
               city_key,
               neighbourhood_key,
               'ALL',
               ROUND(CAST(AVG(price_amount) AS NUMERIC), 2),
               COUNT(*)
           FROM fct_listing_monthly_snapshot
           WHERE month_key = ? AND city_key = ? AND price_amount IS NOT NULL
           GROUP BY month_key, city_key, neighbourhood_key""",
        (month_key, city_key),
    )
    total += cur.rowcount

    conn.commit()
    logger.info(
        "Loaded %d rows into fct_neighbourhood_monthly_avg_price", total
    )
    return total


def load_fct_neighbourhood_monthly_top10_price_delta(
    conn,
    city_key: int,
    month_key: int,
    n: int = 10,
) -> int:
    """Identify top N overpriced and underpriced listings per neighbourhood.

    Compares each listing against its own room type average AND against the
    combined 'ALL' average. Returns total rows inserted.
    """
    conn.execute(
        """DELETE FROM fct_neighbourhood_monthly_top10_price_delta
           WHERE month_key = ? AND city_key = ?""",
        (month_key, city_key),
    )

    # Get neighbourhood averages (both per room_type and ALL)
    avgs = db.read_sql(
        conn,
        """SELECT neighbourhood_key, room_type, avg_price
           FROM fct_neighbourhood_monthly_avg_price
           WHERE month_key = ? AND city_key = ?""",
        (month_key, city_key),
    )
    if avgs.empty:
        return 0

    # Get all listings for this month/city with their room_type
    facts = db.read_sql(
        conn,
        """SELECT f.neighbourhood_key, f.listing_key, f.price_amount,
                  dl.room_type
           FROM fct_listing_monthly_snapshot f
           JOIN dim_listing dl ON f.listing_key = dl.listing_key
           WHERE f.month_key = ? AND f.city_key = ? AND f.price_amount IS NOT NULL""",
        (month_key, city_key),
    )
    if facts.empty:
        return 0

    total = 0

    # Process for each room_type group (per room type + ALL)
    for rt in avgs["room_type"].unique():
        avgs_rt = avgs[avgs["room_type"] == rt][["neighbourhood_key", "avg_price"]]

        if rt == "ALL":
            # Compare all listings against the ALL average
            facts_rt = facts.copy()
        else:
            # Compare only listings of this room type against this room type average
            facts_rt = facts[facts["room_type"] == rt].copy()

        if facts_rt.empty:
            continue

        merged = facts_rt.merge(avgs_rt, on="neighbourhood_key")
        merged["price_delta"] = (merged["price_amount"] - merged["avg_price"]).round(2)
        merged["price_delta_pct"] = (
            (merged["price_delta"] / merged["avg_price"]) * 100
        ).round(2)

        for nb_key in merged["neighbourhood_key"].unique():
            nb_data = merged[merged["neighbourhood_key"] == nb_key]

            for delta_type, ascending in [("OVERPRICED", False), ("UNDERPRICED", True)]:
                sorted_df = nb_data.sort_values("price_delta", ascending=ascending)
                top_n = sorted_df.head(n).copy()
                top_n["rank_in_neighbourhood"] = range(1, len(top_n) + 1)

                for _, row in top_n.iterrows():
                    conn.execute(
                        """INSERT INTO fct_neighbourhood_monthly_top10_price_delta
                           (month_key, city_key, neighbourhood_key, listing_key,
                            room_type, price_amount, neighbourhood_avg_price,
                            price_delta, price_delta_pct,
                            rank_in_neighbourhood, delta_type)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            month_key,
                            city_key,
                            int(row["neighbourhood_key"]),
                            int(row["listing_key"]),
                            rt,
                            float(row["price_amount"]),
                            float(row["avg_price"]),
                            float(row["price_delta"]),
                            float(row["price_delta_pct"]),
                            int(row["rank_in_neighbourhood"]),
                            delta_type,
                        ),
                    )
                    total += 1

    conn.commit()
    logger.info(
        "Loaded %d rows into fct_neighbourhood_monthly_top10_price_delta", total
    )
    return total


def load_fct_data_compliance_monthly(
    conn,
    city_key: int,
    month_key: int,
) -> int:
    """Load monthly data compliance metrics from stg_listings.

    Compliance requires non-null/non-empty price_amount, neighbourhood_cleansed, and room_type.
    Returns rows inserted (0 or 1).
    """
    city_row = conn.execute(
        "SELECT city_name FROM dim_city WHERE city_key = ?", (city_key,)
    ).fetchone()
    city_name = city_row[0] if city_row else None

    year = month_key // 100
    month = month_key % 100
    snapshot_month = f"{year}-{month:02d}-01"

    stg = db.read_sql(
        conn,
        """SELECT price_amount, neighbourhood_cleansed, room_type
           FROM stg_listings
           WHERE city = ? AND snapshot_month = ? AND geo_out_of_city_flag = 0""",
        (city_name, snapshot_month),
    )

    conn.execute(
        "DELETE FROM fct_data_compliance_monthly WHERE month_key = ? AND city_key = ?",
        (month_key, city_key),
    )

    if stg.empty:
        conn.commit()
        return 0

    price_missing = stg["price_amount"].isna()
    neighbourhood_missing = stg["neighbourhood_cleansed"].isna() | (
        stg["neighbourhood_cleansed"].astype(str).str.strip() == ""
    )
    room_type_missing = stg["room_type"].isna() | (
        stg["room_type"].astype(str).str.strip() == ""
    )

    compliant = ~(price_missing | neighbourhood_missing | room_type_missing)

    conn.execute(
        """INSERT INTO fct_data_compliance_monthly
           (month_key, city_key, rows_count, compliance_data_count,
            missing_price_count, missing_neighbourhood_cleansed_count, missing_room_type_count)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            month_key,
            city_key,
            int(len(stg)),
            int(compliant.sum()),
            int(price_missing.sum()),
            int(neighbourhood_missing.sum()),
            int(room_type_missing.sum()),
        ),
    )
    conn.commit()
    return 1


def _safe(v):
    if pd.isna(v):
        return None
    return float(v)


def _safe_int(v):
    if pd.isna(v):
        return None
    return int(v)
