"""
Data quality and validation controls for the ModularSnapshotETL pipeline.

Implements column validation, type checks, and business rule enforcement
as described in the design document section 5.
"""
import logging

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "id",
    "name",
    "neighbourhood_cleansed",
    "price",
    "last_scraped",
]

CRITICAL_NOT_NULL_COLUMNS = ["id", "last_scraped"]

# Bounding boxes per city for geo validation.
# Rows outside these bounds are flagged (not deleted) with geo_out_of_city_flag = 1.
CITY_BOUNDARIES: dict[str, dict[str, float]] = {
    "new-york": {
        "min_lat": 40.49, "max_lat": 40.92,
        "min_long": -74.26, "max_long": -73.70,
    },
    "chicago": {
        "min_lat": 41.64, "max_lat": 42.03,
        "min_long": -87.94, "max_long": -87.52,
    },
    "los-angeles": {
        "min_lat": 33.70, "max_lat": 34.34,
        "min_long": -118.67, "max_long": -118.15,
    },
    "san-francisco": {
        "min_lat": 37.70, "max_lat": 37.84,
        "min_long": -122.52, "max_long": -122.35,
    },
    "new-orleans": {
        "min_lat": 29.85, "max_lat": 30.10,
        "min_long": -90.20, "max_long": -89.90,
    },
}


class ValidationError(Exception):
    """Raised when a critical validation check fails."""


def validate_file(file_path: str) -> None:
    """Check that the source file exists and is readable."""
    import os

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset not found: {file_path}")


def validate_schema(df: pd.DataFrame, file_path: str) -> None:
    """Verify required columns exist in the DataFrame."""
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValidationError(
            f"Missing required columns in {file_path}: {missing}"
        )


def validate_data_quality(df: pd.DataFrame) -> list[str]:
    """Run data quality checks on a cleaned staging DataFrame.

    Returns a list of warning messages. Raises ValidationError for
    critical failures.
    """
    warnings = []

    # Check for duplicate keys
    dupes = df.duplicated(subset=["id"], keep=False)
    if dupes.any():
        warnings.append(
            f"Duplicate listing ids found: {df.loc[dupes, 'id'].nunique()} distinct ids"
        )

    # Critical not-null columns
    for col in CRITICAL_NOT_NULL_COLUMNS:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                raise ValidationError(
                    f"Critical column '{col}' has {null_count} null values"
                )

    # Price > 0
    if "price_amount" in df.columns:
        bad_prices = (df["price_amount"] <= 0).sum()
        if bad_prices > 0:
            warnings.append(f"{bad_prices} rows with price <= 0")

    # Availability between 0-365
    for col in ["availability_30", "availability_60", "availability_90", "availability_365"]:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            out_of_range = ((s < 0) | (s > 365)).sum()
            if out_of_range > 0:
                warnings.append(
                    f"{col}: {out_of_range} values outside 0-365 range"
                )

    for w in warnings:
        logger.warning("Data quality: %s", w)

    return warnings


def geo_flag_out_of_city(df: pd.DataFrame, city: str) -> tuple[pd.DataFrame, int]:
    """Flag rows whose lat/long falls outside the city bounding box.

    Adds or updates a ``geo_out_of_city_flag`` column (0 = inside, 1 = outside).
    Rows are kept in the DataFrame, never deleted.

    Args:
        df: Staging DataFrame with ``latitude`` and ``longitude`` columns.
        city: City name used to look up CITY_BOUNDARIES.

    Returns:
        Tuple of (DataFrame with flag column, count of flagged rows).
    """
    df = df.copy()
    bounds = CITY_BOUNDARIES.get(city)

    if bounds is None or "latitude" not in df.columns or "longitude" not in df.columns:
        df["geo_out_of_city_flag"] = 0
        return df, 0

    lat = pd.to_numeric(df["latitude"], errors="coerce")
    lon = pd.to_numeric(df["longitude"], errors="coerce")

    outside = (
        (lat < bounds["min_lat"]) | (lat > bounds["max_lat"])
        | (lon < bounds["min_long"]) | (lon > bounds["max_long"])
        | lat.isna() | lon.isna()
    )
    df["geo_out_of_city_flag"] = outside.astype(int)
    flagged_count = int(outside.sum())

    if flagged_count > 0:
        logger.warning(
            "Geo validation: %d/%d rows flagged as out-of-city for %s",
            flagged_count, len(df), city,
        )

    return df, flagged_count
