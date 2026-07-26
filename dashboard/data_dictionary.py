"""Data dictionary definitions for the ModularSnapshotETL platform."""

COLUMN_DICTIONARY = [
    # --- Core Listing Fields ---
    {"column": "id", "description": "Unique Airbnb listing identifier", "layers": "raw, stg, dim_listing (as listing_id)", "type": "INTEGER", "validation": "NOT NULL, dedupe key in staging", "notes": "Natural key from Inside Airbnb"},
    {"column": "name", "description": "Listing title", "layers": "raw, stg", "type": "TEXT", "validation": "Required column", "notes": ""},
    {"column": "description", "description": "Full listing description", "layers": "raw, stg", "type": "TEXT", "validation": "None", "notes": "May be very long"},
    {"column": "listing_url", "description": "URL to the Airbnb listing page", "layers": "raw", "type": "TEXT", "validation": "None", "notes": ""},
    # --- Price ---
    {"column": "price", "description": "Nightly listing price as displayed", "layers": "raw (text)", "type": "TEXT", "validation": "Parsed from '$1,234.00' format", "notes": "Converted to price_amount in staging"},
    {"column": "price_amount", "description": "Parsed nightly price as a number", "layers": "stg, fct", "type": "REAL", "validation": "Must be > 0. Null/zero excluded at staging.", "notes": "Base price only, no taxes/fees"},
    # --- Location ---
    {"column": "neighbourhood_cleansed", "description": "Standardised neighbourhood name", "layers": "raw, stg, dim_neighbourhood", "type": "TEXT", "validation": "Required column. Missing values counted in compliance.", "notes": "Authoritative neighbourhood field per Inside Airbnb"},
    {"column": "neighbourhood_group_cleansed", "description": "Borough / neighbourhood group", "layers": "stg, dim_neighbourhood", "type": "TEXT", "validation": "None", "notes": "Not available for all cities"},
    {"column": "latitude", "description": "Property latitude", "layers": "raw, stg, dim_listing", "type": "REAL", "validation": "Used for geo bounding box check", "notes": "Property location, not host location"},
    {"column": "longitude", "description": "Property longitude", "layers": "raw, stg, dim_listing", "type": "REAL", "validation": "Used for geo bounding box check", "notes": "Property location, not host location"},
    {"column": "city", "description": "City slug identifier", "layers": "raw, stg", "type": "TEXT", "validation": "NOT NULL", "notes": "e.g., new-york, chicago"},
    # --- Property ---
    {"column": "property_type", "description": "Property category", "layers": "raw, stg, dim_listing", "type": "TEXT", "validation": "None", "notes": "e.g., Entire rental unit, Private room"},
    {"column": "room_type", "description": "Room type category", "layers": "raw, stg, dim_listing, fct", "type": "TEXT", "validation": "Missing values counted in compliance", "notes": "Entire home/apt, Private room, Shared room, Hotel room"},
    {"column": "accommodates", "description": "Maximum number of guests", "layers": "raw, stg, dim_listing", "type": "INTEGER", "validation": "None", "notes": ""},
    {"column": "bedrooms", "description": "Number of bedrooms", "layers": "raw, stg, dim_listing", "type": "REAL", "validation": "None", "notes": "Can be null"},
    {"column": "beds", "description": "Number of beds", "layers": "raw, stg, dim_listing", "type": "REAL", "validation": "None", "notes": "Can be null"},
    {"column": "bathrooms", "description": "Number of bathrooms", "layers": "stg", "type": "REAL", "validation": "None", "notes": "Parsed from text in some datasets"},
    # --- Host ---
    {"column": "host_id", "description": "Unique host identifier", "layers": "raw, stg, dim_host", "type": "INTEGER", "validation": "None", "notes": "Natural key for host dimension"},
    {"column": "host_name", "description": "Host display name", "layers": "raw, stg, dim_host", "type": "TEXT", "validation": "None", "notes": "SCD2 tracked attribute"},
    {"column": "host_since", "description": "Date host joined Airbnb", "layers": "raw, stg, dim_host", "type": "TEXT", "validation": "None", "notes": ""},
    {"column": "host_is_superhost", "description": "Superhost status", "layers": "raw (t/f), stg (1/0), dim_host", "type": "TEXT/INT", "validation": "Boolean standardised in staging", "notes": "SCD2 tracked"},
    {"column": "host_response_time", "description": "Host response time category", "layers": "stg, dim_host", "type": "TEXT", "validation": "None", "notes": "e.g., within an hour"},
    {"column": "host_response_rate", "description": "Host response rate", "layers": "stg, dim_host", "type": "TEXT", "validation": "None", "notes": "Percentage as text"},
    # --- Availability ---
    {"column": "availability_30", "description": "Days available in next 30 days", "layers": "raw, stg, fct", "type": "INTEGER", "validation": "0-365 range check (warning)", "notes": ""},
    {"column": "availability_60", "description": "Days available in next 60 days", "layers": "raw, stg, fct", "type": "INTEGER", "validation": "0-365 range check (warning)", "notes": ""},
    {"column": "availability_90", "description": "Days available in next 90 days", "layers": "raw, stg, fct", "type": "INTEGER", "validation": "0-365 range check (warning)", "notes": ""},
    {"column": "availability_365", "description": "Days available in next 365 days", "layers": "raw, stg, fct", "type": "INTEGER", "validation": "0-365 range check (warning)", "notes": ""},
    # --- Reviews ---
    {"column": "number_of_reviews", "description": "Total review count", "layers": "raw, stg, fct", "type": "INTEGER", "validation": "None", "notes": ""},
    {"column": "reviews_per_month", "description": "Average reviews per month", "layers": "raw, stg, fct", "type": "REAL", "validation": "None", "notes": ""},
    {"column": "review_scores_rating", "description": "Overall review score", "layers": "raw, stg, fct", "type": "REAL", "validation": "None", "notes": "Scale varies by dataset version"},
    # --- Dates ---
    {"column": "last_scraped", "description": "Date the listing was last scraped", "layers": "raw, stg", "type": "TEXT", "validation": "NOT NULL (critical). Used to derive snapshot_month.", "notes": "Format: YYYY-MM-DD"},
    {"column": "snapshot_month", "description": "Month of the data snapshot", "layers": "raw, stg, fct (as month_key)", "type": "TEXT", "validation": "Derived from last_scraped, truncated to month start", "notes": "Format: YYYY-MM-01"},
    # --- Flags ---
    {"column": "geo_out_of_city_flag", "description": "Whether listing coords are outside city bounds", "layers": "stg", "type": "INTEGER", "validation": "0 = inside, 1 = outside or missing coords", "notes": "Flagged rows excluded from fact layer"},
    # --- ETL Tracking ---
    {"column": "insert_date_utc", "description": "UTC timestamp when row was inserted", "layers": "raw, stg", "type": "TEXT", "validation": "Auto-set on insert", "notes": "ISO 8601 format"},
    {"column": "pipeline_run_id", "description": "ETL run that created this row", "layers": "raw", "type": "INTEGER", "validation": "FK to pipeline_execution_log", "notes": "Traceability link"},
]

QUALITY_RULES = [
    {"rule": "Price > 0", "implementation": "Staging _clean() filters null/zero prices", "outcome": "Excluded from staging; count logged to pipeline_error_log"},
    {"rule": "Availability 0-365", "implementation": "validate_data_quality() range check", "outcome": "Warning logged, row kept"},
    {"rule": "Duplicate deduplication", "implementation": "_deduplicate() by (city, snapshot_month, id), keep latest last_scraped", "outcome": "Duplicates removed in staging"},
    {"rule": "Geo bounding box", "implementation": "geo_flag_out_of_city() using CITY_BOUNDARIES", "outcome": "Flagged in staging (geo_out_of_city_flag=1), excluded from facts"},
    {"rule": "Required columns", "implementation": "validate_schema() checks id, name, neighbourhood_cleansed, price, last_scraped", "outcome": "Pipeline fails fast (ValidationError) if missing"},
    {"rule": "Critical NOT NULL", "implementation": "id and last_scraped must not be null", "outcome": "ValidationError raised"},
    {"rule": "SCD2 history tracking", "implementation": "dim_host and dim_listing track attribute changes over time", "outcome": "New version row on attribute change; old row closed"},
    {"rule": "Compliance monitoring", "implementation": "fct_data_compliance_monthly counts missing price/neighbourhood/room_type", "outcome": "Monthly quality snapshot per city"},
]

CITY_BOUNDARIES = [
    {"city": "new-york", "min_lat": 40.49, "max_lat": 40.92, "min_long": -74.26, "max_long": -73.70},
    {"city": "chicago", "min_lat": 41.64, "max_lat": 42.03, "min_long": -87.94, "max_long": -87.52},
    {"city": "los-angeles", "min_lat": 33.70, "max_lat": 34.34, "min_long": -118.67, "max_long": -118.15},
    {"city": "san-francisco", "min_lat": 37.70, "max_lat": 37.84, "min_long": -122.52, "max_long": -122.35},
    {"city": "new-orleans", "min_lat": 29.85, "max_lat": 30.10, "min_long": -90.20, "max_long": -89.90},
]
