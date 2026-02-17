"""App-wide constants for the ModularSnapshotETL dashboard."""

import os

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "ModularSnapshotETL.db")
DATASET_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dataset")
LISTING_FILENAME = "listings.csv.gz"

ROOM_TYPES = ["ALL", "Entire home/apt", "Private room", "Shared room", "Hotel room"]

INSIDE_AIRBNB_CITIES = [
    {"city": "new-york", "label": "New York, United States"},
    {"city": "chicago", "label": "Chicago, United States"},
    {"city": "los-angeles", "label": "Los Angeles, United States"},
    {"city": "san-francisco", "label": "San Francisco, United States"},
    {"city": "new-orleans", "label": "New Orleans, United States"},
    {"city": "london", "label": "London, United Kingdom"},
    {"city": "paris", "label": "Paris, France"},
    {"city": "amsterdam", "label": "Amsterdam, Netherlands"},
    {"city": "barcelona", "label": "Barcelona, Spain"},
    {"city": "berlin", "label": "Berlin, Germany"},
]
