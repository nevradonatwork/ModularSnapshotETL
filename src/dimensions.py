"""
Dimension table loaders for the ModularSnapshotETL data platform.

Handles upsert logic for dim_date, dim_city, dim_neighbourhood,
dim_host (SCD2), and dim_listing (SCD2).
"""
import json
import logging
import os

import pandas as pd

from src import db

logger = logging.getLogger(__name__)


def load_dim_date(conn, snapshot_month: str) -> int:
    """Ensure a dim_date row exists for the given snapshot_month.

    Args:
        snapshot_month: Date string like '2025-12-01'.

    Returns:
        The month_key (integer YYYYMM).
    """
    dt = pd.Timestamp(snapshot_month)
    month_key = dt.year * 100 + dt.month
    date_key = dt.year * 10000 + dt.month * 100 + dt.day

    existing = conn.execute(
        "SELECT date_key FROM dim_date WHERE month_key = ?", (month_key,)
    ).fetchone()

    if not existing:
        conn.execute(
            """INSERT INTO dim_date (date_key, month_key, month_start_date, year, month)
               VALUES (?, ?, ?, ?, ?)""",
            (date_key, month_key, snapshot_month, dt.year, dt.month),
        )
        conn.commit()
        logger.info("Inserted dim_date: month_key=%d", month_key)

    return month_key


def load_dim_city(conn, city_name: str) -> int:
    """Ensure a dim_city row exists for the given city. Returns city_key."""
    row = conn.execute(
        "SELECT city_key FROM dim_city WHERE city_name = ?", (city_name,)
    ).fetchone()

    if row:
        return row[0]

    city_key = conn.execute(
        "INSERT INTO dim_city (city_name) VALUES (?) RETURNING city_key", (city_name,)
    ).fetchone()[0]
    conn.commit()
    logger.info("Inserted dim_city: %s (city_key=%d)", city_name, city_key)
    return city_key


def load_dim_neighbourhoods(
    conn,
    city_key: int,
    stg_df: pd.DataFrame,
) -> dict:
    """Upsert dim_neighbourhood from staging data.

    Returns:
        dict mapping neighbourhood_cleansed -> neighbourhood_key
    """
    neighbourhoods = (
        stg_df[["neighbourhood", "neighbourhood_cleansed", "neighbourhood_group_cleansed"]]
        .drop_duplicates(subset=["neighbourhood_cleansed"])
    )

    mapping = {}
    for _, row in neighbourhoods.iterrows():
        nc = row["neighbourhood_cleansed"]
        if nc is None:
            continue

        existing = conn.execute(
            """SELECT neighbourhood_key, neighbourhood_group_cleansed FROM dim_neighbourhood
               WHERE city_key = ? AND neighbourhood_cleansed = ?""",
            (city_key, nc),
        ).fetchone()

        if existing:
            mapping[nc] = existing[0]
            if (existing[1] is None or str(existing[1]).strip() == "") and row.get("neighbourhood_group_cleansed"):
                conn.execute(
                    """UPDATE dim_neighbourhood
                       SET neighbourhood_group_cleansed = COALESCE(neighbourhood_group_cleansed, ?)
                       WHERE neighbourhood_key = ?""",
                    (row.get("neighbourhood_group_cleansed"), existing[0]),
                )
        else:
            new_key = conn.execute(
                """INSERT INTO dim_neighbourhood
                   (city_key, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed)
                   VALUES (?, ?, ?, ?) RETURNING neighbourhood_key""",
                (
                    city_key,
                    row.get("neighbourhood"),
                    nc,
                    row.get("neighbourhood_group_cleansed"),
                ),
            ).fetchone()[0]
            mapping[nc] = new_key

    conn.commit()
    logger.info("Loaded %d neighbourhoods for city_key=%d", len(mapping), city_key)
    return mapping



def load_neighbourhood_reference_files(
    conn,
    city_key: int,
    city_dir: str,
) -> tuple[int, int]:
    """Load neighbourhood CSV and GeoJSON files into dim_neighbourhood.

    Returns tuple: (csv_rows_upserted, geojson_rows_enriched)
    """
    csv_count = _load_neighbourhoods_from_csv(conn, city_key, os.path.join(city_dir, "neighbourhoods.csv"))
    geo_count = _enrich_neighbourhoods_from_geojson(
        conn, city_key, os.path.join(city_dir, "neighbourhoods.geojson")
    )
    return csv_count, geo_count


def backfill_staging_neighbourhoods_from_dim_geo(
    conn,
    city: str,
    snapshot_month: str,
    city_key: int,
) -> int:
    """Fill missing staging neighbourhoods using dim_neighbourhood geometries."""
    rows = conn.execute(
        """SELECT stg_id, latitude, longitude FROM stg_listings
           WHERE city = ? AND snapshot_month = ?
             AND (neighbourhood_cleansed IS NULL OR TRIM(neighbourhood_cleansed) = '')""",
        (city, snapshot_month),
    ).fetchall()
    if not rows:
        return 0

    geometries = conn.execute(
        """SELECT neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, geometry_json
           FROM dim_neighbourhood
           WHERE city_key = ? AND geometry_json IS NOT NULL""",
        (city_key,),
    ).fetchall()
    if not geometries:
        return 0

    updated = 0
    for stg_id, lat, lon in rows:
        if lat is None or lon is None:
            continue
        matched = None
        for neigh, neigh_clean, neigh_group, geom_json in geometries:
            if not geom_json:
                continue
            if _point_in_geojson(lon, lat, geom_json):
                matched = (neigh, neigh_clean, neigh_group)
                break

        if matched:
            conn.execute(
                """UPDATE stg_listings
                   SET neighbourhood = COALESCE(neighbourhood, ?),
                       neighbourhood_cleansed = ?,
                       neighbourhood_group_cleansed = COALESCE(neighbourhood_group_cleansed, ?)
                   WHERE stg_id = ?""",
                (matched[0], matched[1], matched[2], stg_id),
            )
            updated += 1

    conn.commit()
    return updated


def _load_neighbourhoods_from_csv(conn, city_key: int, csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0

    df = pd.read_csv(csv_path)
    rename_map = {
        "neighborhood": "neighbourhood",
        "neighborhood_group": "neighbourhood_group",
        "neighborhood_group_cleansed": "neighbourhood_group",
    }
    df = df.rename(columns=rename_map)
    if "neighbourhood" not in df.columns:
        return 0

    if "neighbourhood_group" not in df.columns:
        df["neighbourhood_group"] = None

    count = 0
    for _, row in df[["neighbourhood", "neighbourhood_group"]].drop_duplicates().iterrows():
        nc = _clean_text(row.get("neighbourhood"))
        if not nc:
            continue
        ng = _clean_text(row.get("neighbourhood_group"))
        existing = conn.execute(
            """SELECT neighbourhood_key, neighbourhood_group_cleansed FROM dim_neighbourhood
               WHERE city_key = ? AND neighbourhood_cleansed = ?""",
            (city_key, nc),
        ).fetchone()
        if existing:
            if (existing[1] is None or str(existing[1]).strip() == "") and ng:
                conn.execute(
                    "UPDATE dim_neighbourhood SET neighbourhood_group_cleansed = ? WHERE neighbourhood_key = ?",
                    (ng, existing[0]),
                )
        else:
            conn.execute(
                """INSERT INTO dim_neighbourhood
                   (city_key, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed)
                   VALUES (?, ?, ?, ?)""",
                (city_key, nc, nc, ng),
            )
        count += 1

    conn.commit()
    return count


def _enrich_neighbourhoods_from_geojson(conn, city_key: int, geojson_path: str) -> int:
    if not os.path.exists(geojson_path):
        return 0

    with open(geojson_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    features = payload.get("features", [])
    updated = 0
    for feat in features:
        props = feat.get("properties", {}) or {}
        geometry = feat.get("geometry", {}) or {}
        neighbourhood = _clean_text(props.get("neighbourhood") or props.get("name"))
        if not neighbourhood:
            continue
        neighbourhood_group = _clean_text(props.get("neighbourhood_group") or props.get("neighbourhood_group_cleansed"))
        geometry_json = json.dumps(geometry)
        geometry_type = geometry.get("type")

        existing = conn.execute(
            """SELECT neighbourhood_key FROM dim_neighbourhood
               WHERE city_key = ? AND neighbourhood_cleansed = ?""",
            (city_key, neighbourhood),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE dim_neighbourhood
                   SET geometry_type = ?,
                       geometry_json = ?,
                       neighbourhood_group_cleansed = COALESCE(neighbourhood_group_cleansed, ?)
                   WHERE neighbourhood_key = ?""",
                (geometry_type, geometry_json, neighbourhood_group, existing[0]),
            )
        else:
            conn.execute(
                """INSERT INTO dim_neighbourhood
                   (city_key, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, geometry_type, geometry_json)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (city_key, neighbourhood, neighbourhood, neighbourhood_group, geometry_type, geometry_json),
            )
        updated += 1

    conn.commit()
    return updated


def _point_in_geojson(lon: float, lat: float, geometry_json: str) -> bool:
    try:
        geometry = json.loads(geometry_json)
    except Exception:
        return False

    gtype = geometry.get("type")
    coords = geometry.get("coordinates")
    if not coords:
        return False

    if gtype == "Polygon":
        return _point_in_polygon(lon, lat, coords)
    if gtype == "MultiPolygon":
        return any(_point_in_polygon(lon, lat, polygon) for polygon in coords)
    return False


def _point_in_polygon(lon: float, lat: float, polygon: list) -> bool:
    if not polygon:
        return False

    outer_ring = polygon[0]
    if not _point_in_ring(lon, lat, outer_ring):
        return False

    for hole in polygon[1:]:
        if _point_in_ring(lon, lat, hole):
            return False
    return True


def _point_in_ring(lon: float, lat: float, ring: list) -> bool:
    inside = False
    n = len(ring)
    if n < 3:
        return False

    j = n - 1
    for i in range(n):
        xi, yi = ring[i]
        xj, yj = ring[j]
        intersects = ((yi > lat) != (yj > lat)) and (
            lon < (xj - xi) * (lat - yi) / ((yj - yi) or 1e-12) + xi
        )
        if intersects:
            inside = not inside
        j = i
    return inside


def _clean_text(value):
    if value is None:
        return None
    val = str(value).strip()
    return val or None

def load_dim_hosts(
    conn,
    stg_df: pd.DataFrame,
    snapshot_month: str,
) -> dict:
    """Upsert dim_host with SCD2 logic.

    Returns:
        dict mapping host_id -> host_key (current version)
    """
    host_cols = [
        "host_id", "host_name", "host_since", "host_location", "host_about",
        "host_response_time", "host_response_rate", "host_acceptance_rate",
        "host_is_superhost", "host_listings_count", "host_total_listings_count",
        "host_has_profile_pic", "host_identity_verified",
    ]
    available_cols = [c for c in host_cols if c in stg_df.columns]
    if "host_id" not in available_cols:
        return {}

    hosts = stg_df[available_cols].drop_duplicates(subset=["host_id"])
    # SCD2 tracking attributes
    track_attrs = [c for c in available_cols if c != "host_id"]

    if db.is_postgres(conn):
        # Batched: a handful of round trips total instead of ~2 per host,
        # which matters a lot once a city has thousands of unique hosts.
        mapping = _load_dim_hosts_batched(conn, hosts, available_cols, track_attrs, snapshot_month)
    else:
        # SQLite is a local file — row-by-row has no network latency to pay.
        mapping = _load_dim_hosts_row_by_row(conn, hosts, track_attrs, snapshot_month)

    conn.commit()
    logger.info("Loaded %d hosts", len(mapping))
    return mapping


def _load_dim_hosts_row_by_row(conn, hosts, track_attrs, snapshot_month):
    mapping = {}
    for _, row in hosts.iterrows():
        hid = row["host_id"]
        if pd.isna(hid):
            continue
        hid = int(hid)

        existing = conn.execute(
            """SELECT host_key, {} FROM dim_host
               WHERE host_id = ? AND is_current = 1""".format(
                ", ".join(track_attrs)
            ),
            (hid,),
        ).fetchone()

        if existing:
            host_key = existing[0]
            old_vals = list(existing[1:])
            new_vals = [_safe_val(row.get(c)) for c in track_attrs]

            if old_vals == new_vals:
                mapping[hid] = host_key
                continue

            # Close old record
            conn.execute(
                """UPDATE dim_host SET valid_to = ?, is_current = 0
                   WHERE host_key = ?""",
                (snapshot_month, host_key),
            )

        # Insert new current record
        available_cols = ["host_id"] + track_attrs
        placeholders = ", ".join(["?"] * (len(available_cols) + 1))
        col_names = ", ".join(available_cols + ["valid_from"])
        new_key = conn.execute(
            f"INSERT INTO dim_host ({col_names}) VALUES ({placeholders}) RETURNING host_key",
            [_safe_val(row.get(c)) for c in available_cols] + [snapshot_month],
        ).fetchone()[0]
        mapping[hid] = new_key

    return mapping


def _load_dim_hosts_batched(conn, hosts, available_cols, track_attrs, snapshot_month):
    mapping = {}
    valid_ids = sorted({int(h) for h in hosts["host_id"].dropna().tolist()})
    if not valid_ids:
        return mapping

    placeholders = ", ".join(["?"] * len(valid_ids))
    existing = db.read_sql(
        conn,
        f"""SELECT host_key, host_id, {", ".join(track_attrs)} FROM dim_host
            WHERE is_current = 1 AND host_id IN ({placeholders})""",
        tuple(valid_ids),
    )
    existing_by_id = {
        int(row["host_id"]): (int(row["host_key"]), tuple(_safe_val(row[c]) for c in track_attrs))
        for _, row in existing.iterrows()
    }

    to_close = []
    to_insert_rows = []
    for _, row in hosts.iterrows():
        hid = row["host_id"]
        if pd.isna(hid):
            continue
        hid = int(hid)
        new_vals = tuple(_safe_val(row.get(c)) for c in track_attrs)

        if hid in existing_by_id:
            host_key, old_vals = existing_by_id[hid]
            if old_vals == new_vals:
                mapping[hid] = host_key
                continue
            to_close.append(host_key)

        to_insert_rows.append(row)

    if to_close:
        close_placeholders = ", ".join(["?"] * len(to_close))
        conn.execute(
            f"UPDATE dim_host SET valid_to = ?, is_current = 0 WHERE host_key IN ({close_placeholders})",
            [snapshot_month] + to_close,
        )

    if to_insert_rows:
        insert_df = pd.DataFrame(to_insert_rows)[["host_id"] + track_attrs].copy()
        insert_df["valid_from"] = snapshot_month
        new_rows = db.bulk_insert_returning(
            conn, "dim_host", insert_df, returning_cols=["host_id", "host_key"]
        )
        for hid_val, host_key in new_rows:
            mapping[int(hid_val)] = host_key

    return mapping


def load_dim_listings(
    conn,
    stg_df: pd.DataFrame,
    snapshot_month: str,
) -> dict:
    """Upsert dim_listing with SCD2 logic.

    Returns:
        dict mapping listing_id -> listing_key (current version)
    """
    listing_cols = [
        "id", "property_type", "room_type", "accommodates",
        "bedrooms", "beds", "amenities", "latitude", "longitude",
    ]
    available_cols = [c for c in listing_cols if c in stg_df.columns]
    track_attrs = [c for c in available_cols if c != "id"]

    listings = stg_df[available_cols].drop_duplicates(subset=["id"])

    if db.is_postgres(conn):
        mapping = _load_dim_listings_batched(conn, listings, track_attrs, snapshot_month)
    else:
        mapping = _load_dim_listings_row_by_row(conn, listings, track_attrs, snapshot_month)

    conn.commit()
    logger.info("Loaded %d listings into dim_listing", len(mapping))
    return mapping


def _load_dim_listings_row_by_row(conn, listings, track_attrs, snapshot_month):
    mapping = {}
    for _, row in listings.iterrows():
        lid = int(row["id"])

        existing = conn.execute(
            """SELECT listing_key, {} FROM dim_listing
               WHERE listing_id = ? AND is_current = 1""".format(
                ", ".join(track_attrs)
            ),
            (lid,),
        ).fetchone()

        if existing:
            listing_key = existing[0]
            old_vals = list(existing[1:])
            new_vals = [_safe_val(row.get(c)) for c in track_attrs]

            if old_vals == new_vals:
                mapping[lid] = listing_key
                continue

            conn.execute(
                """UPDATE dim_listing SET valid_to = ?, is_current = 0
                   WHERE listing_key = ?""",
                (snapshot_month, listing_key),
            )

        db_cols = ["listing_id"] + track_attrs + ["valid_from"]
        placeholders = ", ".join(["?"] * len(db_cols))
        col_str = ", ".join(db_cols)
        vals = [lid] + [_safe_val(row.get(c)) for c in track_attrs] + [snapshot_month]
        new_key = conn.execute(
            f"INSERT INTO dim_listing ({col_str}) VALUES ({placeholders}) RETURNING listing_key",
            vals,
        ).fetchone()[0]
        mapping[lid] = new_key

    return mapping


def _load_dim_listings_batched(conn, listings, track_attrs, snapshot_month):
    mapping = {}
    valid_ids = sorted({int(i) for i in listings["id"].dropna().tolist()})
    if not valid_ids:
        return mapping

    placeholders = ", ".join(["?"] * len(valid_ids))
    existing = db.read_sql(
        conn,
        f"""SELECT listing_key, listing_id, {", ".join(track_attrs)} FROM dim_listing
            WHERE is_current = 1 AND listing_id IN ({placeholders})""",
        tuple(valid_ids),
    )
    existing_by_id = {
        int(row["listing_id"]): (int(row["listing_key"]), tuple(_safe_val(row[c]) for c in track_attrs))
        for _, row in existing.iterrows()
    }

    to_close = []
    to_insert_rows = []
    for _, row in listings.iterrows():
        lid = int(row["id"])
        new_vals = tuple(_safe_val(row.get(c)) for c in track_attrs)

        if lid in existing_by_id:
            listing_key, old_vals = existing_by_id[lid]
            if old_vals == new_vals:
                mapping[lid] = listing_key
                continue
            to_close.append(listing_key)

        to_insert_rows.append(row)

    if to_close:
        close_placeholders = ", ".join(["?"] * len(to_close))
        conn.execute(
            f"UPDATE dim_listing SET valid_to = ?, is_current = 0 WHERE listing_key IN ({close_placeholders})",
            [snapshot_month] + to_close,
        )

    if to_insert_rows:
        insert_df = pd.DataFrame(to_insert_rows)[["id"] + track_attrs].rename(columns={"id": "listing_id"}).copy()
        insert_df["valid_from"] = snapshot_month
        new_rows = db.bulk_insert_returning(
            conn, "dim_listing", insert_df, returning_cols=["listing_id", "listing_key"]
        )
        for lid_val, listing_key in new_rows:
            mapping[int(lid_val)] = listing_key

    return mapping


def _safe_val(v):
    """Convert pandas NA / NaN to None for SQLite."""
    if pd.isna(v):
        return None
    return v
