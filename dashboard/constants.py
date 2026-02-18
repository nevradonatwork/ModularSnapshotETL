"""App-wide constants for the ModularSnapshotETL dashboard."""

import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ModularSnapshotETL.db")
DATASET_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dataset")
LISTING_FILENAME = "listings.csv.gz"

ROOM_TYPES = ["ALL", "Entire home/apt", "Private room", "Shared room", "Hotel room"]

DASHBOARD_URL = "https://modularsnapshotetl.streamlit.app/"

# Full city catalog — derived from src/data_fetcher.CITY_CATALOG.
# Each entry: {"slug": ..., "label": ...}
# This is used by the Load Data page for the searchable city dropdown.
def get_city_options() -> list[dict[str, str]]:
    """Return the full list of cities from the data fetcher catalog."""
    from src.data_fetcher import CITY_CATALOG
    return [
        {"slug": slug, "label": label}
        for slug, _country, _region, _city, label in sorted(CITY_CATALOG, key=lambda c: c[4])
    ]
