"""
Entry point for the ModularSnapshotETL data pipeline.

Usage:
    python main.py

Place each city's listings.csv.gz in a subdirectory under dataset/:
    dataset/new-york/listings.csv.gz
    dataset/chicago/listings.csv.gz

The pipeline creates a SQLite database (ModularSnapshotETL.db) with layered
tables: raw, staging, dimensions, facts, and reporting views.
"""
import logging
import os
import sqlite3
import sys

from src.schema import create_all
from src.pipeline import run
from src.email_notify import send_run_notification

def _load_dotenv(path=".env"):
    """Load key=value pairs from a .env file into os.environ."""
    if not os.path.isfile(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, _, value = line.partition("=")
            if key and _ == "=":
                os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()

DATASET_DIR = "dataset"
LISTING_FILENAME = "listings.csv.gz"
DATABASE_PATH = "ModularSnapshotETL.db"


def discover_cities(dataset_dir):
    """Find city subdirectories that contain a listings file."""
    if not os.path.isdir(dataset_dir):
        return []
    cities = []
    for entry in sorted(os.listdir(dataset_dir)):
        listing_path = os.path.join(dataset_dir, entry, LISTING_FILENAME)
        if os.path.isfile(listing_path):
            cities.append(entry)
    return cities


def get_connection(db_path=DATABASE_PATH):
    """Create a SQLite connection with foreign keys enabled."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    cities = discover_cities(DATASET_DIR)
    if not cities:
        logging.info(
            "No city datasets found. Place listings.csv.gz in dataset/<city>/ subdirectories. "
            "If the pipeline already ran, source files have been archived to dataset/<city>/archive/."
        )
        sys.exit(0)

    conn = get_connection()
    create_all(conn)

    failed = []
    for city in cities:
        dataset_path = os.path.join(DATASET_DIR, city, LISTING_FILENAME)
        try:
            logging.info("Processing city: %s", city)
            row_counts = run(conn, dataset_path, city)
            logging.info(
                "City %s complete. Row counts: %s", city, row_counts
            )
        except Exception as e:
            logging.error("City %s failed: %s", city, e)
            failed.append(city)

    # Send email notification with run summary
    send_run_notification(conn, cities, failed)

    conn.close()

    if failed:
        logging.error("Pipeline failed for cities: %s", ", ".join(failed))
        sys.exit(1)

    logging.info("All %d cities processed successfully.", len(cities))


if __name__ == "__main__":
    main()
