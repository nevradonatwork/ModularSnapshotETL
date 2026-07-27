"""
Database schema for the ModularSnapshotETL data platform.

Medallion architecture (Postgres schemas; SQLite has no schema separation
and keeps every table in one flat namespace, see _q() below):
    bronze    – raw_, immutable landing storage
    silver    – stg_, cleansing and deduplication
    gold      – dim_/fct_/vw_rep_, conformed dimensions, facts, reporting views
    metadata  – pipeline execution/error logs, row-count reconciliation,
                watermark control, visitor log, and the business-rule
                reconciliation (rec_) tables
"""
from src.db import is_postgres

# SQLite uses "INTEGER PRIMARY KEY AUTOINCREMENT" for auto-increment PKs;
# Postgres uses "SERIAL PRIMARY KEY". Every CREATE TABLE below is written
# with the SQLite spelling and adapted for Postgres in _DDLCursor.execute().
_SQLITE_PK = "INTEGER PRIMARY KEY AUTOINCREMENT"
_POSTGRES_PK = "SERIAL PRIMARY KEY"

# Every table/view name is globally unique across schemas, so on Postgres
# a connection's `search_path` (set once in db.PGConnection.__init__) lets
# every *unqualified* reference elsewhere in the codebase, SELECT/INSERT/
# UPDATE/DELETE, view bodies, FOREIGN KEY REFERENCES, resolve correctly
# without any per-call-site changes. Only the DDL below, which actually
# creates each object, needs to say which schema it belongs in.
_SCHEMA_OF = {
    "pipeline_execution_log": "metadata",
    "pipeline_error_log": "metadata",
    "row_count_reconciliation": "metadata",
    "watermark_control": "metadata",
    "visitor_log": "metadata",
    "raw_listings": "bronze",
    "stg_listings": "silver",
    "dim_date": "gold",
    "dim_city": "gold",
    "dim_neighbourhood": "gold",
    "dim_host": "gold",
    "dim_listing": "gold",
    "fct_listing_monthly_snapshot": "gold",
    "fct_neighbourhood_monthly_avg_price": "gold",
    "fct_neighbourhood_monthly_top10_price_delta": "gold",
    "fct_data_compliance_monthly": "gold",
    "vw_rep_monthly_neighbourhood_avg_price": "gold",
    "vw_rep_monthly_top10_overpriced": "gold",
    "vw_rep_monthly_top10_underpriced": "gold",
    "vw_rep_monthly_data_compliance": "gold",
    "rec_avg_price_comparison": "metadata",
    "rec_top10_price_delta_comparison": "metadata",
    "rec_reporting_view_comparison": "metadata",
}

SCHEMAS = ("bronze", "silver", "gold", "metadata")


def _q(name: str, postgres: bool) -> str:
    """Schema-qualify a table/view name for Postgres DDL; bare name for SQLite."""
    if not postgres:
        return name
    return f"{_SCHEMA_OF[name]}.{name}"


def create_all(conn) -> None:
    """Create every schema, table, index, and view in the database."""
    postgres = is_postgres(conn)
    cur = _DDLCursor(conn.cursor(), postgres)
    if postgres:
        for schema in SCHEMAS:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        cur.execute("SET search_path TO gold, silver, bronze, metadata, public")
    _create_metadata_tables(cur, postgres)
    _create_raw_tables(cur, postgres)
    _create_staging_tables(cur, postgres)
    _create_dimension_tables(cur, postgres)
    _create_fact_tables(cur, postgres)
    if postgres:
        # Views select dim_listing.listing_id, which blocks ALTER COLUMN
        # TYPE while they exist. Drop them first; _create_presentation_views
        # below recreates all of them unconditionally anyway.
        _drop_presentation_views(cur, postgres)
        _migrate_bigint_columns(cur, postgres)
    _create_presentation_views(cur, postgres)
    _create_reconciliation_tables(cur, postgres)
    conn.commit()


# Airbnb natural keys (scrape_id, listing/host ids) can exceed Postgres's
# 4-byte INTEGER range (e.g. scrape_id is a 14-digit number). CREATE TABLE
# IF NOT EXISTS won't widen a column on a database whose tables were
# already created before this fix, so migrate it explicitly.
_BIGINT_COLUMNS = [
    ("raw_listings", "id"),
    ("raw_listings", "scrape_id"),
    ("raw_listings", "host_id"),
    ("stg_listings", "id"),
    ("stg_listings", "host_id"),
    ("dim_host", "host_id"),
    ("dim_listing", "listing_id"),
]

_PRESENTATION_VIEWS = [
    "vw_rep_monthly_neighbourhood_avg_price",
    "vw_rep_monthly_top10_overpriced",
    "vw_rep_monthly_top10_underpriced",
    "vw_rep_monthly_data_compliance",
]


def _drop_presentation_views(cur, postgres) -> None:
    for view in _PRESENTATION_VIEWS:
        cur.execute(f"DROP VIEW IF EXISTS {_q(view, postgres)}")


def _migrate_bigint_columns(cur, postgres) -> None:
    for table, column in _BIGINT_COLUMNS:
        cur.execute(
            """SELECT data_type FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s AND column_name = %s""",
            (_SCHEMA_OF[table], table, column),
        )
        row = cur.fetchall()
        if not row:
            continue  # column doesn't exist (yet) on this database -- nothing to migrate
        current_type = row[0][0]
        if current_type != "bigint":
            cur.execute(f"ALTER TABLE {_q(table, postgres)} ALTER COLUMN {column} TYPE BIGINT")


class _DDLCursor:
    """Wraps a cursor so every statement is adapted for the target dialect."""

    def __init__(self, cur, postgres: bool):
        self._cur = cur
        self._postgres = postgres

    def execute(self, sql, params=()):
        if self._postgres:
            sql = sql.replace(_SQLITE_PK, _POSTGRES_PK)
        self._cur.execute(sql, params)
        return self

    def fetchall(self):
        return self._cur.fetchall()


# ------------------------------------------------------------------
# Metadata Layer (pipeline execution/error logs, audit, watermark, visitors)
# ------------------------------------------------------------------

def _create_metadata_tables(cur, postgres: bool) -> None:
    t = _q("pipeline_execution_log", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
            run_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name       TEXT,
            start_time          TEXT NOT NULL,
            end_time            TEXT,
            status              TEXT NOT NULL DEFAULT 'RUNNING',
            rows_processed      INTEGER,
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

    # Migrate existing databases: add columns introduced after the initial
    # etl_run_log -> pipeline_execution_log rename, if missing.
    new_cols = {"pipeline_name": "TEXT", "rows_processed": "INTEGER", "triggered_by": "TEXT"}
    if postgres:
        for col, coltype in new_cols.items():
            cur.execute(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS {col} {coltype}")
    else:
        existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(pipeline_execution_log)").fetchall()}
        for col, coltype in new_cols.items():
            if col not in existing_cols:
                cur.execute(f"ALTER TABLE {t} ADD COLUMN {col} {coltype}")

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("pipeline_error_log", postgres)} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id          INTEGER NOT NULL,
            table_name      TEXT,
            error_type      TEXT NOT NULL,
            error_details   TEXT,
            timestamp       TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES pipeline_execution_log(run_id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("row_count_reconciliation", postgres)} (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id              INTEGER NOT NULL,
            table_name          TEXT NOT NULL,
            source_row_count    INTEGER,
            target_row_count    INTEGER,
            checksum_source     TEXT,
            checksum_target     TEXT,
            match_status        TEXT NOT NULL CHECK(match_status IN ('matched', 'mismatched')),
            checked_at          TEXT NOT NULL,
            FOREIGN KEY (run_id) REFERENCES pipeline_execution_log(run_id)
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("watermark_control", postgres)} (
            table_name                   TEXT PRIMARY KEY,
            last_successful_load_timestamp TEXT,
            last_run_id                  INTEGER
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("visitor_log", postgres)} (
            visit_id        INTEGER PRIMARY KEY AUTOINCREMENT,
            session_uuid     TEXT NOT NULL UNIQUE,
            first_seen_at    TEXT NOT NULL,
            last_seen_at     TEXT NOT NULL,
            ip_address       TEXT,
            user_agent       TEXT,
            city             TEXT,
            country          TEXT,
            ran_pipeline     INTEGER NOT NULL DEFAULT 0,
            page_views       INTEGER NOT NULL DEFAULT 1
        )
    """)


# ------------------------------------------------------------------
# Bronze Layer (raw)
# ------------------------------------------------------------------

def _create_raw_tables(cur, postgres: bool) -> None:
    t = _q("raw_listings", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
            raw_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            id                  BIGINT,
            listing_url         TEXT,
            scrape_id           BIGINT,
            last_scraped        TEXT,
            source              TEXT,
            name                TEXT,
            description         TEXT,
            neighborhood_overview TEXT,
            picture_url         TEXT,
            host_id             BIGINT,
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

    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_raw_listings_city_month
        ON {t}(city, snapshot_month)
    """)


# ------------------------------------------------------------------
# Silver Layer (staging)
# ------------------------------------------------------------------

def _create_staging_tables(cur, postgres: bool) -> None:
    t = _q("stg_listings", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
            stg_id              INTEGER PRIMARY KEY AUTOINCREMENT,
            id                  BIGINT NOT NULL,
            city                TEXT NOT NULL,
            snapshot_month      TEXT NOT NULL,
            name                TEXT,
            description         TEXT,
            host_id             BIGINT,
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

    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_stg_listings_city_month
        ON {t}(city, snapshot_month)
    """)


# ------------------------------------------------------------------
# Gold Layer, Dimensions
# ------------------------------------------------------------------

def _create_dimension_tables(cur, postgres: bool) -> None:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("dim_date", postgres)} (
            date_key        INTEGER PRIMARY KEY,
            month_key       INTEGER NOT NULL,
            month_start_date TEXT NOT NULL,
            year            INTEGER NOT NULL,
            month           INTEGER NOT NULL
        )
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("dim_city", postgres)} (
            city_key        INTEGER PRIMARY KEY AUTOINCREMENT,
            city_name       TEXT NOT NULL UNIQUE,
            country         TEXT NOT NULL DEFAULT 'US',
            timezone        TEXT NOT NULL DEFAULT 'America/New_York',
            currency_code   TEXT NOT NULL DEFAULT 'USD',
            currency_symbol TEXT NOT NULL DEFAULT '$',
            is_active       INTEGER NOT NULL DEFAULT 1
        )
    """)

    t = _q("dim_neighbourhood", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
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

    t = _q("dim_host", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
            host_key                INTEGER PRIMARY KEY AUTOINCREMENT,
            host_id                 BIGINT NOT NULL,
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

    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_dim_host_current
        ON {t}(host_id, is_current)
    """)

    t = _q("dim_listing", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
            listing_key     INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id      BIGINT NOT NULL,
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

    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_dim_listing_current
        ON {t}(listing_id, is_current)
    """)


# ------------------------------------------------------------------
# Gold Layer, Facts
# ------------------------------------------------------------------

def _create_fact_tables(cur, postgres: bool) -> None:
    t = _q("fct_listing_monthly_snapshot", postgres)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {t} (
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

    cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_fct_snapshot_month_city
        ON {t}(month_key, city_key)
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("fct_neighbourhood_monthly_avg_price", postgres)} (
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

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("fct_neighbourhood_monthly_top10_price_delta", postgres)} (
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

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("fct_data_compliance_monthly", postgres)} (
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
# Gold Layer, Reporting Views
# ------------------------------------------------------------------

def _create_presentation_views(cur, postgres: bool) -> None:
    cur.execute(f"DROP VIEW IF EXISTS {_q('vw_rep_monthly_neighbourhood_avg_price', postgres)}")
    cur.execute(f"""
        CREATE VIEW {_q('vw_rep_monthly_neighbourhood_avg_price', postgres)} AS
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

    cur.execute(f"DROP VIEW IF EXISTS {_q('vw_rep_monthly_top10_overpriced', postgres)}")
    cur.execute(f"""
        CREATE VIEW {_q('vw_rep_monthly_top10_overpriced', postgres)} AS
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

    cur.execute(f"DROP VIEW IF EXISTS {_q('vw_rep_monthly_top10_underpriced', postgres)}")
    cur.execute(f"""
        CREATE VIEW {_q('vw_rep_monthly_top10_underpriced', postgres)} AS
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

    cur.execute(f"DROP VIEW IF EXISTS {_q('vw_rep_monthly_data_compliance', postgres)}")
    cur.execute(f"""
        CREATE VIEW {_q('vw_rep_monthly_data_compliance', postgres)} AS
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
# Metadata Layer, Business-Rule Reconciliation (data comparison / audit)
# ------------------------------------------------------------------

def _create_reconciliation_tables(cur, postgres: bool) -> None:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("rec_avg_price_comparison", postgres)} (
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

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("rec_top10_price_delta_comparison", postgres)} (
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

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_q("rec_reporting_view_comparison", postgres)} (
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
