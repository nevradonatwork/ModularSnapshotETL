"""
Database schema for the ModularSnapshotETL data platform.

Layers:
    raw_     – immutable landing storage
    stg_     – cleansing and deduplication
    dim_     – conformed dimensions
    fct_     – analytical fact tables
    vw_rep_  – reporting views for BI
    rec_     – reconciliation / data comparison
    etl_     – pipeline logging and error tracking
"""
import sqlite3


def create_all(conn: sqlite3.Connection) -> None:
    """Create every table, index, and view in the ModularSnapshotETL database."""
    cur = conn.cursor()
    _create_etl_tables(cur)
    _create_raw_tables(cur)
    _create_staging_tables(cur)
    _create_dimension_tables(cur)
    _create_fact_tables(cur)
    _create_presentation_views(cur)
    _create_reconciliation_tables(cur)
    conn.commit()


# ------------------------------------------------------------------
# ETL Logging
# ------------------------------------------------------------------

def _create_etl_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_run_log (
            run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time          TEXT NOT NULL,
            end_time            TEXT,
            status              TEXT NOT NULL DEFAULT 'RUNNING',
            city                TEXT,
            snapshot_month      TEXT,
            source_file_path    TEXT,
            source_file_name    TEXT,
            archived_file_path  TEXT,
            row_counts          TEXT,
            error_message       TEXT,
            triggered_by        TEXT
        )
    """)

    # Migrate existing databases: add triggered_by column if missing
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(etl_run_log)").fetchall()}
    if "triggered_by" not in existing_cols:
        cur.execute("ALTER TABLE etl_run_log ADD COLUMN triggered_by TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_error_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            table_name      TEXT,
            error_type      TEXT NOT NULL,
            error_details   TEXT,
            timestamp       TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES etl_run_log(run_id)
        )
    """)


# ------------------------------------------------------------------
# Raw Layer
# ------------------------------------------------------------------

def _create_raw_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_listings (
            raw_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            id                  INTEGER,
            listing_url         TEXT,
            scrape_id           INTEGER,
            last_scraped        TEXT,
            source              TEXT,
            name                TEXT,
            description         TEXT,
            neighborhood_overview TEXT,
            picture_url         TEXT,
            host_id             INTEGER,
            host_url            TEXT,
            host_name           TEXT,
            host_since          TEXT,
            host_location       TEXT,
            host_about          TEXT,
            host_response_time  TEXT,
            host_response_rate  TEXT,
            host_acceptance_rate TEXT,
            host_is_superhost   TEXT,
            host_thumbnail_url  TEXT,
            host_picture_url    TEXT,
            host_neighbourhood  TEXT,
            host_listings_count TEXT,
            host_total_listings_count TEXT,
            host_verifications  TEXT,
            host_has_profile_pic TEXT,
            host_identity_verified TEXT,
            neighbourhood       TEXT,
            neighbourhood_cleansed TEXT,
            neighbourhood_group_cleansed TEXT,
            latitude            REAL,
            longitude           REAL,
            property_type       TEXT,
            room_type           TEXT,
            accommodates        INTEGER,
            bathrooms           REAL,
            bathrooms_text      TEXT,
            bedrooms            REAL,
            beds                REAL,
            amenities           TEXT,
            price               TEXT,
            minimum_nights      INTEGER,
            maximum_nights      INTEGER,
            minimum_minimum_nights INTEGER,
            maximum_minimum_nights INTEGER,
            minimum_maximum_nights INTEGER,
            maximum_maximum_nights INTEGER,
            minimum_nights_avg_ntm REAL,
            maximum_nights_avg_ntm REAL,
            calendar_updated    TEXT,
            has_availability    TEXT,
            availability_30     INTEGER,
            availability_60     INTEGER,
            availability_90     INTEGER,
            availability_365    INTEGER,
            calendar_last_scraped TEXT,
            number_of_reviews   INTEGER,
            number_of_reviews_ltm INTEGER,
            number_of_reviews_l30d INTEGER,
            first_review        TEXT,
            last_review         TEXT,
            review_scores_rating REAL,
            review_scores_accuracy REAL,
            review_scores_cleanliness REAL,
            review_scores_checkin REAL,
            review_scores_communication REAL,
            review_scores_location REAL,
            review_scores_value REAL,
            license             TEXT,
            instant_bookable    TEXT,
            calculated_host_listings_count INTEGER,
            calculated_host_listings_count_entire_homes INTEGER,
            calculated_host_listings_count_private_rooms INTEGER,
            calculated_host_listings_count_shared_rooms INTEGER,
            reviews_per_month   REAL,
            city                TEXT NOT NULL,
            snapshot_month      TEXT NOT NULL,
            extract_date_utc    TEXT NOT NULL,
            insert_date_utc     TEXT NOT NULL,
            source_file_name    TEXT,
            pipeline_run_id     INTEGER
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_listings_city_month
        ON raw_listings(city, snapshot_month)
    """)


# ------------------------------------------------------------------
# Staging Layer
# ------------------------------------------------------------------

def _create_staging_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_listings (
            stg_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            id                  INTEGER NOT NULL,
            city                TEXT NOT NULL,
            snapshot_month      TEXT NOT NULL,
            name                TEXT,
            description         TEXT,
            host_id             INTEGER,
            host_name           TEXT,
            host_since          TEXT,
            host_location       TEXT,
            host_about          TEXT,
            host_response_time  TEXT,
            host_response_rate  TEXT,
            host_acceptance_rate TEXT,
            host_is_superhost   INTEGER,
            host_listings_count INTEGER,
            host_total_listings_count INTEGER,
            host_has_profile_pic INTEGER,
            host_identity_verified INTEGER,
            neighbourhood       TEXT,
            neighbourhood_cleansed TEXT,
            neighbourhood_group_cleansed TEXT,
            latitude            REAL,
            longitude           REAL,
            property_type       TEXT,
            room_type           TEXT,
            accommodates        INTEGER,
            bathrooms           REAL,
            bedrooms            REAL,
            beds                REAL,
            amenities           TEXT,
            price_amount        REAL,
            minimum_nights      INTEGER,
            maximum_nights      INTEGER,
            has_availability    INTEGER,
            availability_30     INTEGER,
            availability_60     INTEGER,
            availability_90     INTEGER,
            availability_365    INTEGER,
            number_of_reviews   INTEGER,
            number_of_reviews_ltm INTEGER,
            number_of_reviews_l30d INTEGER,
            first_review        TEXT,
            last_review         TEXT,
            review_scores_rating REAL,
            review_scores_accuracy REAL,
            review_scores_cleanliness REAL,
            review_scores_checkin REAL,
            review_scores_communication REAL,
            review_scores_location REAL,
            review_scores_value REAL,
            reviews_per_month   REAL,
            instant_bookable    INTEGER,
            calculated_host_listings_count INTEGER,
            last_scraped        TEXT,
            geo_out_of_city_flag INTEGER NOT NULL DEFAULT 0,
            insert_date_utc     TEXT NOT NULL,
            UNIQUE(city, snapshot_month, id)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_stg_listings_city_month
        ON stg_listings(city, snapshot_month)
    """)


# ------------------------------------------------------------------
# Dimension Layer
# ------------------------------------------------------------------

def _create_dimension_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key        INTEGER PRIMARY KEY,
            month_key       INTEGER NOT NULL,
            month_start_date TEXT NOT NULL,
            year            INTEGER NOT NULL,
            month           INTEGER NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_city (
            city_key        INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name       TEXT NOT NULL UNIQUE,
            country         TEXT NOT NULL DEFAULT 'US',
            timezone        TEXT NOT NULL DEFAULT 'America/New_York',
            currency_code   TEXT NOT NULL DEFAULT 'USD',
            currency_symbol TEXT NOT NULL DEFAULT '$',
            is_active       INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_neighbourhood (
            neighbourhood_key               INTEGER PRIMARY KEY AUTOINCREMENT,
            city_key                         INTEGER NOT NULL,
            neighbourhood                    TEXT,
            neighbourhood_cleansed           TEXT NOT NULL,
            neighbourhood_group_cleansed     TEXT,
            geometry_type                    TEXT,
            geometry_json                    TEXT,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            UNIQUE(city_key, neighbourhood_cleansed)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_host (
            host_key                INTEGER PRIMARY KEY AUTOINCREMENT,
            host_id                 INTEGER NOT NULL,
            host_name               TEXT,
            host_since              TEXT,
            host_location           TEXT,
            host_about              TEXT,
            host_response_time      TEXT,
            host_response_rate      TEXT,
            host_acceptance_rate    TEXT,
            host_is_superhost       INTEGER,
            host_listings_count     INTEGER,
            host_total_listings_count INTEGER,
            host_has_profile_pic    INTEGER,
            host_identity_verified  INTEGER,
            valid_from              TEXT NOT NULL,
            valid_to                TEXT NOT NULL DEFAULT '9999-12-31',
            is_current              INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dim_host_current
        ON dim_host(host_id, is_current)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_listing (
            listing_key     INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id      INTEGER NOT NULL,
            property_type   TEXT,
            room_type       TEXT,
            accommodates    INTEGER,
            bedrooms        REAL,
            beds            REAL,
            amenities       TEXT,
            latitude        REAL,
            longitude       REAL,
            valid_from      TEXT NOT NULL,
            valid_to        TEXT NOT NULL DEFAULT '9999-12-31',
            is_current      INTEGER NOT NULL DEFAULT 1
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_dim_listing_current
        ON dim_listing(listing_id, is_current)
    """)


# ------------------------------------------------------------------
# Fact Layer
# ------------------------------------------------------------------

def _create_fact_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fct_listing_monthly_snapshot (
            snapshot_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key           INTEGER NOT NULL,
            city_key            INTEGER NOT NULL,
            neighbourhood_key   INTEGER NOT NULL,
            listing_key         INTEGER NOT NULL,
            host_key            INTEGER NOT NULL,
            price_amount        REAL,
            availability_30     INTEGER,
            availability_60     INTEGER,
            availability_90     INTEGER,
            availability_365    INTEGER,
            number_of_reviews   INTEGER,
            reviews_per_month   REAL,
            review_scores_rating REAL,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            FOREIGN KEY (neighbourhood_key) REFERENCES dim_neighbourhood(neighbourhood_key),
            FOREIGN KEY (listing_key) REFERENCES dim_listing(listing_key),
            FOREIGN KEY (host_key) REFERENCES dim_host(host_key),
            UNIQUE(month_key, city_key, listing_key)
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fct_snapshot_month_city
        ON fct_listing_monthly_snapshot(month_key, city_key)
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fct_neighbourhood_monthly_avg_price (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key           INTEGER NOT NULL,
            city_key            INTEGER NOT NULL,
            neighbourhood_key   INTEGER NOT NULL,
            room_type           TEXT NOT NULL DEFAULT 'ALL',
            avg_price           REAL NOT NULL,
            listing_count       INTEGER NOT NULL,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            FOREIGN KEY (neighbourhood_key) REFERENCES dim_neighbourhood(neighbourhood_key),
            UNIQUE(month_key, city_key, neighbourhood_key, room_type)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fct_neighbourhood_monthly_top10_price_delta (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key               INTEGER NOT NULL,
            city_key                INTEGER NOT NULL,
            neighbourhood_key       INTEGER NOT NULL,
            listing_key             INTEGER NOT NULL,
            room_type               TEXT NOT NULL DEFAULT 'ALL',
            price_amount            REAL NOT NULL,
            neighbourhood_avg_price REAL NOT NULL,
            price_delta             REAL NOT NULL,
            price_delta_pct         REAL NOT NULL,
            rank_in_neighbourhood   INTEGER NOT NULL,
            delta_type              TEXT NOT NULL CHECK(delta_type IN ('OVERPRICED', 'UNDERPRICED')),
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            FOREIGN KEY (neighbourhood_key) REFERENCES dim_neighbourhood(neighbourhood_key),
            FOREIGN KEY (listing_key) REFERENCES dim_listing(listing_key)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fct_data_compliance_monthly (
            id                                  INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key                           INTEGER NOT NULL,
            city_key                            INTEGER NOT NULL,
            rows_count                          INTEGER NOT NULL,
            compliance_data_count               INTEGER NOT NULL,
            missing_price_count                 INTEGER NOT NULL,
            missing_neighbourhood_cleansed_count INTEGER NOT NULL,
            missing_room_type_count             INTEGER NOT NULL,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            UNIQUE(month_key, city_key)
        )
    """)


# ------------------------------------------------------------------
# Presentation Layer (Views)
# ------------------------------------------------------------------

def _create_presentation_views(cur: sqlite3.Cursor) -> None:
    cur.execute("DROP VIEW IF EXISTS vw_rep_monthly_neighbourhood_avg_price")
    cur.execute("""
        CREATE VIEW vw_rep_monthly_neighbourhood_avg_price AS
        SELECT
            dd.month_start_date,
            dd.year,
            dd.month,
            dc.city_name,
            dc.currency_code,
            dn.neighbourhood_cleansed AS neighbourhood,
            dn.neighbourhood_group_cleansed AS neighbourhood_group,
            f.room_type,
            f.avg_price,
            f.listing_count
        FROM fct_neighbourhood_monthly_avg_price f
        JOIN dim_date dd ON f.month_key = dd.month_key
        JOIN dim_city dc ON f.city_key = dc.city_key
        JOIN dim_neighbourhood dn ON f.neighbourhood_key = dn.neighbourhood_key
    """)

    cur.execute("DROP VIEW IF EXISTS vw_rep_monthly_top10_overpriced")
    cur.execute("""
        CREATE VIEW vw_rep_monthly_top10_overpriced AS
        SELECT
            dd.month_start_date,
            dd.year,
            dd.month,
            dc.city_name,
            dc.currency_code,
            dn.neighbourhood_cleansed AS neighbourhood,
            f.room_type AS compared_against_room_type,
            dl.listing_id,
            dl.property_type,
            dl.room_type,
            f.price_amount,
            f.neighbourhood_avg_price,
            f.price_delta,
            f.price_delta_pct,
            f.rank_in_neighbourhood
        FROM fct_neighbourhood_monthly_top10_price_delta f
        JOIN dim_date dd ON f.month_key = dd.month_key
        JOIN dim_city dc ON f.city_key = dc.city_key
        JOIN dim_neighbourhood dn ON f.neighbourhood_key = dn.neighbourhood_key
        JOIN dim_listing dl ON f.listing_key = dl.listing_key
        WHERE f.delta_type = 'OVERPRICED'
    """)

    cur.execute("DROP VIEW IF EXISTS vw_rep_monthly_top10_underpriced")
    cur.execute("""
        CREATE VIEW vw_rep_monthly_top10_underpriced AS
        SELECT
            dd.month_start_date,
            dd.year,
            dd.month,
            dc.city_name,
            dc.currency_code,
            dn.neighbourhood_cleansed AS neighbourhood,
            f.room_type AS compared_against_room_type,
            dl.listing_id,
            dl.property_type,
            dl.room_type,
            f.price_amount,
            f.neighbourhood_avg_price,
            f.price_delta,
            f.price_delta_pct,
            f.rank_in_neighbourhood
        FROM fct_neighbourhood_monthly_top10_price_delta f
        JOIN dim_date dd ON f.month_key = dd.month_key
        JOIN dim_city dc ON f.city_key = dc.city_key
        JOIN dim_neighbourhood dn ON f.neighbourhood_key = dn.neighbourhood_key
        JOIN dim_listing dl ON f.listing_key = dl.listing_key
        WHERE f.delta_type = 'UNDERPRICED'
    """)

    cur.execute("DROP VIEW IF EXISTS vw_rep_monthly_data_compliance")
    cur.execute("""
        CREATE VIEW vw_rep_monthly_data_compliance AS
        SELECT
            dd.month_start_date,
            dd.year,
            dd.month,
            dc.city_name,
            f.rows_count,
            f.compliance_data_count,
            f.missing_price_count,
            f.missing_neighbourhood_cleansed_count,
            f.missing_room_type_count
        FROM fct_data_compliance_monthly f
        JOIN dim_date dd ON f.month_key = dd.month_key
        JOIN dim_city dc ON f.city_key = dc.city_key
    """)


# ------------------------------------------------------------------
# Reconciliation Layer (data comparison / audit)
# ------------------------------------------------------------------

def _create_reconciliation_tables(cur: sqlite3.Cursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rec_avg_price_comparison (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key           INTEGER NOT NULL,
            city_key            INTEGER NOT NULL,
            neighbourhood_key   INTEGER NOT NULL,
            room_type           TEXT NOT NULL,
            stg_avg_price       REAL,
            stg_listing_count   INTEGER,
            fct_avg_price       REAL,
            fct_listing_count   INTEGER,
            price_diff          REAL,
            price_diff_pct      REAL,
            count_diff          INTEGER,
            match_status        TEXT NOT NULL CHECK(match_status IN ('MATCH', 'MISMATCH', 'STG_ONLY', 'FCT_ONLY')),
            checked_at          TEXT NOT NULL,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            FOREIGN KEY (neighbourhood_key) REFERENCES dim_neighbourhood(neighbourhood_key)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rec_top10_price_delta_comparison (
            id                          INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key                   INTEGER NOT NULL,
            city_key                    INTEGER NOT NULL,
            neighbourhood_key           INTEGER NOT NULL,
            listing_key                 INTEGER NOT NULL,
            room_type                   TEXT NOT NULL,
            delta_type                  TEXT NOT NULL CHECK(delta_type IN ('OVERPRICED', 'UNDERPRICED')),
            stg_price_amount            REAL,
            stg_neighbourhood_avg_price REAL,
            stg_price_delta             REAL,
            stg_price_delta_pct         REAL,
            stg_rank                    INTEGER,
            fct_price_amount            REAL,
            fct_neighbourhood_avg_price REAL,
            fct_price_delta             REAL,
            fct_price_delta_pct         REAL,
            fct_rank                    INTEGER,
            price_delta_diff            REAL,
            rank_diff                   INTEGER,
            match_status                TEXT NOT NULL CHECK(match_status IN ('MATCH', 'MISMATCH', 'STG_ONLY', 'FCT_ONLY')),
            checked_at                  TEXT NOT NULL,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key),
            FOREIGN KEY (neighbourhood_key) REFERENCES dim_neighbourhood(neighbourhood_key),
            FOREIGN KEY (listing_key) REFERENCES dim_listing(listing_key)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rec_reporting_view_comparison (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            month_key           INTEGER NOT NULL,
            city_key            INTEGER NOT NULL,
            view_name           TEXT NOT NULL,
            fct_row_count       INTEGER NOT NULL,
            view_row_count      INTEGER NOT NULL,
            count_diff          INTEGER NOT NULL,
            match_status        TEXT NOT NULL CHECK(match_status IN ('MATCH', 'MISMATCH')),
            checked_at          TEXT NOT NULL,
            FOREIGN KEY (city_key) REFERENCES dim_city(city_key)
        )
    """)
