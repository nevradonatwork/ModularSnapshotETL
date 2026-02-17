"""Shared test fixtures for BreezeBnb tests."""
import sqlite3

import pandas as pd
import pytest

from src.schema import create_all


COLUMNS = [
    "id", "name", "neighbourhood_cleansed",
    "price", "room_type", "minimum_nights", "number_of_reviews", "last_scraped",
    "host_id", "host_name", "neighbourhood", "neighbourhood_group_cleansed",
    "property_type", "accommodates", "bedrooms", "beds", "amenities",
    "latitude", "longitude",
    "availability_30", "availability_60", "availability_90", "availability_365",
    "reviews_per_month", "review_scores_rating",
    "host_is_superhost", "host_has_profile_pic", "host_identity_verified",
    "has_availability", "instant_bookable",
    "host_since", "host_location", "host_about",
    "host_response_time", "host_response_rate", "host_acceptance_rate",
    "host_listings_count", "host_total_listings_count",
    "calculated_host_listings_count",
]

SAMPLE_ROWS = [
    [1, "Cozy Apt", "Williamsburg", "$150.00", "Entire home/apt", 2, 10, "2025-01-15",
     101, "Alice", "Williamsburg", "Brooklyn",
     "Apartment", 4, 1, 2, "WiFi, Kitchen", 40.71, -73.96,
     10, 30, 60, 200, 2.5, 4.8, "t", "t", "t", "t", "f",
     "2015-01-01", "New York", "I love hosting", "within an hour", "100%", "90%", 1, 1, 1],
    [2, "Sunny Room", "Harlem", "$75.50", "Private room", 1, 25, "2025-01-15",
     102, "Bob", "Harlem", "Manhattan",
     "Apartment", 2, 1, 1, "WiFi", 40.81, -73.95,
     20, 40, 70, 300, 5.0, 4.5, "f", "t", "t", "t", "t",
     "2018-06-15", "New York", "Welcome!", "within a day", "95%", "80%", 2, 2, 2],
    [3, "Budget Bed", "Williamsburg", "$40.00", "Shared room", 3, 5, "2025-01-15",
     103, "Charlie", "Williamsburg", "Brooklyn",
     "Hostel", 1, 1, 1, "WiFi", 40.72, -73.95,
     25, 50, 80, 350, 1.0, 4.2, "f", "f", "f", "t", "f",
     "2020-03-10", "Brooklyn", "Budget stay", "a few days or more", "80%", "70%", 1, 1, 1],
]


def write_csv(rows, columns, path, compress=False):
    """Helper to write test CSV data."""
    df = pd.DataFrame(rows, columns=columns)
    if compress:
        df.to_csv(path, index=False, compression="gzip")
    else:
        df.to_csv(path, index=False)
    return str(path)


@pytest.fixture
def db_conn():
    """Create an in-memory SQLite database with all tables."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys=ON")
    create_all(conn)
    yield conn
    conn.close()


@pytest.fixture
def sample_csv(tmp_path):
    """Write sample CSV and return its path."""
    path = write_csv(SAMPLE_ROWS, COLUMNS, str(tmp_path / "listings.csv"))
    return path


@pytest.fixture
def sample_csv_gz(tmp_path):
    """Write sample gzipped CSV and return its path."""
    path = write_csv(
        SAMPLE_ROWS, COLUMNS, str(tmp_path / "listings.csv.gz"), compress=True
    )
    return path
