"""
Data ingestion for the ModularSnapshotETL pipeline.

Handles loading source CSV files into the raw layer and transforming
them into the staging layer with cleansing and deduplication.
"""
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from src import db
from src.validation import validate_file, validate_schema, geo_flag_out_of_city

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Public API
# ------------------------------------------------------------------

def load_raw(
    conn,
    file_path: str,
    city: str,
    snapshot_month: str,
    run_id: int,
) -> int:
    """Load a CSV file into raw_listings. Returns row count."""
    validate_file(file_path)
    df = pd.read_csv(file_path, low_memory=False)
    validate_schema(df, file_path)

    now = datetime.now(timezone.utc).isoformat()
    df["city"] = city
    df["snapshot_month"] = snapshot_month
    df["extract_date_utc"] = now
    df["insert_date_utc"] = now
    df["source_file_name"] = os.path.basename(file_path)
    df["pipeline_run_id"] = run_id

    # Write all columns that exist in the raw_listings table
    raw_cols = db.table_columns(conn, "raw_listings")
    # Exclude raw_id (auto-increment)
    raw_cols.discard("raw_id")
    cols_to_write = [c for c in df.columns if c in raw_cols]

    # Log unknown columns for schema drift visibility
    unknown_cols = sorted(set(df.columns) - raw_cols - {"raw_id"})
    if unknown_cols:
        logger.warning(
            "Schema drift: %d unknown columns ignored in %s: %s",
            len(unknown_cols), os.path.basename(file_path), ", ".join(unknown_cols),
        )

    db.bulk_insert(conn, "raw_listings", df[cols_to_write])

    row_count = len(df)
    logger.info("Loaded %d rows into raw_listings for %s/%s", row_count, city, snapshot_month)
    return row_count


def load_staging(
    conn,
    city: str,
    snapshot_month: str,
) -> tuple[int, int, int]:
    """Transform raw data into stg_listings with cleansing and deduplication.

    Returns:
        Tuple of (row count, rows excluded for invalid price, geo out-of-city count).
    """
    # Read raw data for this city/month
    df = db.read_sql(
        conn,
        "SELECT * FROM raw_listings WHERE city = ? AND snapshot_month = ?",
        (city, snapshot_month),
    )
    if df.empty:
        logger.warning("No raw data found for %s/%s", city, snapshot_month)
        return 0, 0, 0

    df, invalid_price_count = _clean(df)
    df = _deduplicate(df)

    # Geo validation — flag rows outside city bounding box
    df, geo_flagged_count = geo_flag_out_of_city(df, city)

    now = datetime.now(timezone.utc).isoformat()
    df["insert_date_utc"] = now

    # Map to staging columns
    stg_cols = db.table_columns(conn, "stg_listings")
    stg_cols.discard("stg_id")
    cols_to_write = [c for c in df.columns if c in stg_cols]

    # Delete existing staging rows for this city/month before insert
    conn.execute(
        "DELETE FROM stg_listings WHERE city = ? AND snapshot_month = ?",
        (city, snapshot_month),
    )
    db.bulk_insert(conn, "stg_listings", df[cols_to_write])
    conn.commit()

    row_count = len(df)
    logger.info("Loaded %d rows into stg_listings for %s/%s", row_count, city, snapshot_month)
    return row_count, invalid_price_count, geo_flagged_count


# ------------------------------------------------------------------
# Cleaning & Deduplication
# ------------------------------------------------------------------

def _clean(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Clean raw listing data: parse price, standardise booleans, handle nulls.

    Returns:
        Tuple of (cleaned DataFrame, count of rows excluded for invalid price).
    """
    df = df.copy()

    # Parse price — strip $ and commas, convert to float
    df["price_amount"] = _parse_price(df.get("price", pd.Series(dtype=str)))

    # Exclude rows with null or zero price — no synthetic estimation
    invalid_price = df["price_amount"].isna() | (df["price_amount"] <= 0)
    invalid_count = int(invalid_price.sum())
    if invalid_count > 0:
        logger.warning(
            "Excluding %d rows with invalid price (null or <= 0) out of %d total",
            invalid_count, len(df),
        )
    df = df[~invalid_price]

    # Standardise boolean columns
    for col in [
        "host_is_superhost",
        "host_has_profile_pic",
        "host_identity_verified",
        "has_availability",
        "instant_bookable",
    ]:
        if col in df.columns:
            df[col] = df[col].map({"t": 1, "f": 0, True: 1, False: 0}).fillna(0).astype(int)

    # Convert date fields
    if "last_scraped" in df.columns:
        df["last_scraped"] = pd.to_datetime(df["last_scraped"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Trim text fields
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": None, "None": None, "": None})

    # Numeric conversions
    int_cols = [
        "host_listings_count", "host_total_listings_count",
        "calculated_host_listings_count",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df, invalid_count


def _deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate by (city, snapshot_month, id).

    Keep latest last_scraped; tie-break on insert_date_utc.
    """
    sort_cols = []
    if "last_scraped" in df.columns:
        sort_cols.append("last_scraped")
    if "insert_date_utc" in df.columns:
        sort_cols.append("insert_date_utc")

    if sort_cols:
        df = df.sort_values(sort_cols, ascending=False)

    df = df.drop_duplicates(subset=["city", "snapshot_month", "id"], keep="first")
    return df


def _parse_price(series: pd.Series) -> pd.Series:
    """Convert price strings like '$1,200.00' to float."""
    return (
        series
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .apply(pd.to_numeric, errors="coerce")
    )


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def archive_file(file_path: str, city: str, snapshot_month: str) -> str:
    """Move a processed file to an archive subdirectory.

    Target pattern:
        dataset/<city>/archive/<city>_<snapshot_month>_<utc_timestamp>_listings.csv.gz

    Args:
        file_path: Original file path.
        city: City name.
        snapshot_month: Snapshot month string (e.g. '2025-01-01').

    Returns:
        The archived file path.
    """
    parent_dir = os.path.dirname(file_path)
    archive_dir = os.path.join(parent_dir, "archive")
    os.makedirs(archive_dir, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ext = ".csv.gz" if file_path.endswith(".csv.gz") else ".csv"
    archived_name = f"{city}_{snapshot_month}_{timestamp}_listings{ext}"
    archived_path = os.path.join(archive_dir, archived_name)

    os.rename(file_path, archived_path)
    logger.info("Archived %s -> %s", file_path, archived_path)
    return archived_path


def derive_snapshot_month(file_path: str) -> str:
    """Read a listings file and derive the snapshot month from last_scraped.

    Returns a string like '2025-12-01'.
    """
    df = pd.read_csv(file_path, low_memory=False, usecols=["last_scraped"], nrows=1000)
    dates = pd.to_datetime(df["last_scraped"], errors="coerce").dropna()
    if dates.empty:
        # Fallback to current month
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-01")
    dominant = dates.dt.to_period("M").mode()[0]
    return f"{dominant.year}-{dominant.month:02d}-01"
