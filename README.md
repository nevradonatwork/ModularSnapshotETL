# ModularSnapshotETL
Modular snapshot-based ELT data platform with layered architecture, idempotent processing, and built-in data quality controls.
# BreezeBnb Data Platform

A layered data engineering platform that transforms raw Airbnb-style listing data into a SQLite analytical warehouse with dimensional modelling, data quality controls, and BI-ready reporting views.

## Quick Start

```bash
pip install -r requirements.txt
```

Place each city's monthly `listings.csv.gz` in a subdirectory under `dataset/`:

```
dataset/new-york/listings.csv.gz
dataset/chicago/listings.csv.gz
```

Then run:

```bash
python main.py
```

The pipeline auto-discovers all city subdirectories, processes them through six data layers, and writes everything into `BreezeBnb.db`.

## Business Outputs

The platform produces three core insights for market intelligence:

- **Monthly average price per night by neighbourhood and room type** — aggregated per room type (Entire home/apt, Private room, etc.) **and** as a combined "ALL" average, broken down by city and month, available via `vw_rep_monthly_neighbourhood_avg_price`
- **Top 10 overpriced and top 10 underpriced listings** — each listing compared against both its room-type-specific neighbourhood average and the combined "ALL" neighbourhood average, with delta amount and percentage, available via `vw_rep_monthly_top10_overpriced` and `vw_rep_monthly_top10_underpriced`
- **Monthly data compliance report** — per-city quality snapshot tracking missing prices, neighbourhoods, and room types, available via `vw_rep_monthly_data_compliance`

## Design Philosophy

This solution intentionally follows **KISS** (Keep It Simple, Stupid) and **YAGNI** (You Aren't Gonna Need It):

- **SQLite over cloud warehouses** — a single-file embedded database with zero infrastructure, full SQL support, and portability. The schema translates directly to BigQuery/Snowflake when scale requires it.
- **No heavy frameworks** — no Airflow, no Spark, no ORM. The pipeline uses only `pandas` and Python's built-in `sqlite3`. Complexity is added when justified, not before.
- **Cron-based scheduling** — the simplest reliable job runner. Advanced orchestration (Airflow, Dagster) is deferred until the number of cities or job dependencies demands it.
- **Layering is pragmatic, not ceremonial** — each layer (raw, staging, dimensions, facts, views) exists because it solves one specific problem: auditability, deduplication, history tracking, aggregation, and BI readability.
- **No synthetic business logic** — listings with null or zero prices are excluded, not estimated. Only real observed data flows into analytics to preserve analytical integrity.
- **Refactoring over speculation** — features were built incrementally. Advanced orchestration, distributed processing, and cloud-native services are intentionally deferred until scale requires them.

## Data Architecture

The pipeline stores data across five logical layers inside a single SQLite database:

| Layer | Prefix | Purpose |
|-------|--------|---------|
| Raw | `raw_` | Immutable landing storage — source data exactly as received |
| Staging | `stg_` | Cleansed, standardised, and deduplicated |
| Dimension | `dim_` | Conformed dimensions (date, city, neighbourhood, host, listing) |
| Fact | `fct_` | Analytical fact tables (monthly snapshots, aggregations) |
| Presentation | `vw_rep_` | BI-ready reporting views for dashboards |
| Reconciliation | `rec_` | Data comparison and integrity verification |

### Tables

**Raw & Staging:**
- `raw_listings` — append-only, one row per listing per file load
- `stg_listings` — deduplicated by `(city, snapshot_month, id)`

**Dimensions:**
- `dim_date` — month-level date dimension
- `dim_city` — city metadata (country, timezone, currency)
- `dim_neighbourhood` — neighbourhood hierarchy per city
- `dim_host` — host attributes with SCD Type 2 history
- `dim_listing` — listing attributes with SCD Type 2 history

**Facts:**
- `fct_listing_monthly_snapshot` — base fact at listing + month + city grain
- `fct_neighbourhood_monthly_avg_price` — aggregated neighbourhood averages per room type + combined "ALL"
- `fct_neighbourhood_monthly_top10_price_delta` — top 10 over/underpriced listings per room type + combined "ALL"
- `fct_data_compliance_monthly` — monthly data-compliance counters per city

**Reporting Views:**
- `vw_rep_monthly_neighbourhood_avg_price`
- `vw_rep_monthly_top10_overpriced`
- `vw_rep_monthly_top10_underpriced`
- `vw_rep_monthly_data_compliance`

**Reconciliation:**
- `rec_avg_price_comparison` — staging-calculated avg prices vs `fct_neighbourhood_monthly_avg_price`
- `rec_top10_price_delta_comparison` — staging-calculated top-10 over/underpriced vs `fct_neighbourhood_monthly_top10_price_delta`
- `rec_reporting_view_comparison` — fact table row counts vs reporting view row counts

**ETL Logging:**
- `etl_run_log` — pipeline execution tracking (start, end, status, city, snapshot_month, source file path, archived file path, row counts)
- `etl_error_log` — detailed error and warning records

## Pipeline Execution Flow

1. Validate file existence
2. Derive `snapshot_month` from `last_scraped` timestamps
3. Load into `raw_listings` (append-only)
4. Validate schema (required columns, types)
5. **Archive source file** to `dataset/<city>/archive/` (preserves processed artifact)
6. Load `stg_listings` with cleansing, deduplication, and **geo validation**
7. Upsert dimension tables (SCD2 for hosts and listings)
8. Load fact tables (base + aggregated) — **geo-flagged rows excluded**
9. **Reconciliation** — independently verify staging vs fact tables vs reporting views
10. Reporting views refresh automatically (SQLite views)
11. Log execution metrics to `etl_run_log` (with file metadata and archived path)

## Idempotency

The pipeline is safe to rerun for any city and month:

- **Raw layer**: append-only — rerunning adds a new copy of the source data (full audit trail)
- **Staging layer**: delete-then-insert scoped to `(city, snapshot_month)` — rerunning replaces only that month's cleaned data
- **Dimensions**: upsert logic — existing records are matched; SCD2 creates new versions only when attributes change
- **Facts**: delete-then-insert scoped to `(month_key, city_key)` — rerunning replaces that month's facts cleanly
- **No destructive operations**: other cities and months are never touched

The `snapshot_month` key ensures complete isolation between monthly runs.

## File Traceability & Archiving

Each pipeline run records full file metadata in `etl_run_log`:

- **city** — the city being processed
- **snapshot_month** — derived from the data
- **source_file_path** — original file location
- **source_file_name** — file basename
- **archived_file_path** — location after archiving

After successful raw ingestion, the source file is moved to an archive directory:

```
dataset/<city>/archive/<city>_<snapshot_month>_<utc_timestamp>_listings.csv.gz
```

Example: `dataset/new-york/archive/new-york_2025-09-01_20260213T104527Z_listings.csv.gz`

This ensures the exact processed file is preserved, source files are not accidentally overwritten, and future reprocessing is possible from the archive. If ingestion fails, the file is **not** archived.

## Geo Validation

Listings are validated against per-city bounding boxes during staging. The `geo_out_of_city_flag` column in `stg_listings` indicates whether a listing's geographic coordinates fall within the expected city boundaries.

### What does `geo_out_of_city_flag` mean?

| Value | Meaning |
|-------|---------|
| `geo_out_of_city_flag = 0` | Listing coordinates (latitude/longitude) are **within** the city bounding box |
| `geo_out_of_city_flag = 1` | Listing coordinates are **outside** the city bounding box, or lat/long is missing |

This is a **data quality / geo-consistency flag**. It validates the listing's physical property location (`latitude`, `longitude` columns from the dataset) against the expected geographic boundaries of the target city. For example, a listing filed under `new-york` but with coordinates pointing to Los Angeles (34.05, -118.25) would be flagged as `geo_out_of_city_flag = 1`.

**Note:** This flag checks the **property's recorded coordinates**, not the host's declared location. A host may live in New Jersey but list a property in Manhattan — that property would pass geo validation because its lat/long falls within New York's bounding box.

### Policy

- **Raw layer**: all rows kept as-is (immutable audit trail)
- **Staging layer**: flagged rows are **kept** with `geo_out_of_city_flag = 1` — data is never silently deleted
- **Fact layer**: flagged rows are **excluded** from aggregates to prevent skewed analytics
- The count of flagged rows is logged to `etl_error_log` as `GEO_OUT_OF_CITY`
- Row counts include `geo_out_of_city_count` for monitoring

## Data Compliance Tracking

A monthly compliance fact is recorded per city/month in `fct_data_compliance_monthly` and exposed through `vw_rep_monthly_data_compliance`.

Tracked fields (must be present for a row to be compliant):
- `price_amount`
- `neighbourhood_cleansed`
- `room_type`

Stored metrics:
- `rows_count`
- `compliance_data_count`
- `missing_price_count`
- `missing_neighbourhood_cleansed_count`
- `missing_room_type_count`

### Compliance behavior summary

| Condition | What happens |
|---|---|
| `price` is NULL / invalid | Excluded at staging (`price_amount` must be > 0), so it does not enter fact calculations |
| `neighbourhood_cleansed` is NULL | No neighbourhood dimension key is resolved; listing is skipped in fact loading |
| `room_type` is NULL | Excluded from per-room-type averages and top 10, but **included** in the combined "ALL" averages and top 10. Counted as non-compliant in monthly compliance fact |
| Multiple missing fields | Counted in each applicable missing-field counter and excluded from `compliance_data_count` |

### Neighbourhood data importance (master + geographic fill)

Neighbourhood data is a critical business key in this model: averages, top-10 pricing deltas, and compliance reporting all depend on correct neighbourhood attribution.

For data quality operations, the recommended policy is:
- Maintain a master neighbourhood dimension (including `neighbourhood_group` when available).
- Treat missing/empty `neighbourhood_cleansed` in listings as a quality gap.
- Fill empty neighbourhood values using geographic lookup (`latitude`/`longitude` point-in-polygon against neighbourhood GeoJSON boundaries).
- Keep unmatched rows visible in compliance metrics (`missing_neighbourhood_cleansed_count`) for monitoring and remediation.

This keeps reporting stable across months and prevents silent loss of listings in neighbourhood-level calculations.

### Why this is important

This compliance dataset is intended as a monthly control point for release confidence and stakeholder reporting.

- It gives a clear quality snapshot per `(city, snapshot_month)` before BI consumption.
- It makes missing required fields visible as explicit counters instead of hidden data loss.
- It can be used as a deployment/run gate (e.g., alert if `compliance_data_count / rows_count` drops below threshold).

Example query:

```sql
SELECT month_start_date, city_name, rows_count, compliance_data_count,
       missing_price_count, missing_neighbourhood_cleansed_count, missing_room_type_count
FROM vw_rep_monthly_data_compliance
ORDER BY month_start_date DESC, city_name;
```

### Configured Cities

Bounding boxes are defined in `CITY_BOUNDARIES` in `src/validation.py`:

| City | Latitude Range | Longitude Range |
|------|---------------|-----------------|
| new-york | 40.49 – 40.92 | -74.26 – -73.70 |
| chicago | 41.64 – 42.03 | -87.94 – -87.52 |
| los-angeles | 33.70 – 34.34 | -118.67 – -118.15 |
| san-francisco | 37.70 – 37.84 | -122.52 – -122.35 |
| new-orleans | 29.85 – 30.10 | -90.20 – -89.90 |

Cities without a bounding box configuration have all rows pass (flag = 0). To add a new city, add its bounding box to `CITY_BOUNDARIES` in `src/validation.py`.


## Neighbourhood Reference Files (per city)

Each city folder can contain optional neighbourhood reference files:

- `dataset/<city>/neighbourhoods.csv`
- `dataset/<city>/neighbourhoods.geojson`

These files are loaded before fact-building to strengthen neighbourhood quality:
- CSV seeds `dim_neighbourhood` (including `neighbourhood_group` when provided).
- GeoJSON enriches `dim_neighbourhood` with geometry metadata.
- If listing rows have empty `neighbourhood_cleansed`, the pipeline attempts geographic backfill (point-in-polygon using listing latitude/longitude and GeoJSON boundaries).

If files are missing, pipeline continues using listing-provided neighbourhood values (backward compatible behavior).

## Reconciliation Layer

After loading all fact tables, the pipeline runs an independent reconciliation step that cross-checks results across layers. This catches silent data loss, join key mismatches, and calculation drift.

### What it checks

| Table | Comparison | Flags |
|-------|-----------|-------|
| `rec_avg_price_comparison` | Recalculates avg price per neighbourhood/room_type from `stg_listings` and compares against `fct_neighbourhood_monthly_avg_price` | `MATCH`, `MISMATCH`, `STG_ONLY`, `FCT_ONLY` |
| `rec_top10_price_delta_comparison` | Independently ranks top-10 overpriced/underpriced from staging, compares price deltas and ranks against `fct_neighbourhood_monthly_top10_price_delta` | `MATCH`, `MISMATCH`, `STG_ONLY`, `FCT_ONLY` |
| `rec_reporting_view_comparison` | Compares row counts between all 4 fact tables and their `vw_rep_*` reporting views | `MATCH`, `MISMATCH` |

### Why this exists

- **Multi-hop verification**: data passes through 5 layers (CSV -> raw -> staging -> dimensions -> facts -> views). Each hop can lose or distort rows.
- **Join integrity**: fact tables depend on dimension key lookups. A missing key silently drops rows. The reconciliation detects this.
- **Idempotency safety net**: if a re-run partially fails (facts deleted but not re-inserted), the reconciliation catches the row count gap.
- **Zero overhead**: runs in milliseconds after the pipeline, reads existing data only.

Example query to check for issues:

```sql
SELECT * FROM rec_avg_price_comparison WHERE match_status != 'MATCH';
SELECT * FROM rec_reporting_view_comparison WHERE match_status != 'MATCH';
```

## Running Tests

```bash
pytest tests/ -v
```

70 tests covering ingestion, dimensions, facts (including room-type breakdown), reporting views, reconciliation, email notifications, pipeline orchestration, geo validation, file archiving, city discovery, monthly data-compliance tracking, and neighbourhood reference-file backfill behavior.

### Testing Approach (What the tests actually do)

The test suite executes real ETL code paths against an isolated SQLite test database, not mocks-only checks.

- **Isolated database per test:** `tests/conftest.py` creates a fresh in-memory SQLite connection (`:memory:`) and applies the full schema via `create_all(conn)`.
- **Realistic source input:** fixtures generate temporary `listings.csv` / `listings.csv.gz` files from representative sample rows.
- **Actual inserts and SQL assertions:** tests run `load_raw`, `load_staging`, and `pipeline.run`, then query tables with SQL to verify row counts, flags, and history behavior.
- **No production side effects:** tests do not write to production `BreezeBnb.db`; in-memory DB state disappears when each test ends.

### Outcome Visibility

When running with `-v`, each test line shows `PASSED` / `FAILED`; the final line summarizes the run result (e.g., `54 passed`).

## Project Structure

```
BreezeBnb/
├── main.py                # Entry point — creates BreezeBnb.db, discovers cities, runs pipeline
├── crontab                # Schedule definition for the orchestrator
├── requirements.txt       # Python dependencies
├── src/
│   ├── schema.py          # DDL for all tables, indexes, and views
│   ├── ingestion.py       # Raw and staging layer loading
│   ├── validation.py      # Schema checks and data quality controls
│   ├── etl_logging.py     # ETL run/error logging
│   ├── email_notify.py    # Email notifications after pipeline runs
│   ├── dimensions.py      # Dimension table upserts (SCD2)
│   ├── facts.py           # Fact table loading and aggregations
│   ├── reconciliation.py  # Data comparison across layers
│   └── pipeline.py        # Orchestrates the full execution flow
├── tests/
│   ├── conftest.py        # Shared fixtures (in-memory DB, sample data)
│   ├── test_ingestion.py  # Raw/staging layer tests
│   ├── test_dimensions.py # Dimension loading and SCD2 tests
│   ├── test_facts.py      # Fact tables and presentation view tests
│   ├── test_reconciliation.py # Reconciliation layer tests
│   ├── test_pipeline.py   # End-to-end pipeline tests
│   └── test_main.py       # City discovery tests
├── dataset/               # Input: dataset/<city>/listings.csv.gz (not committed)
└── BreezeBnb.db           # Output: SQLite database (not committed)
```

## Email Notifications

The pipeline sends an email notification after every run with a summary of the execution, including run status, row counts, and any errors or warnings.

### Setup

Set the following environment variables before running the pipeline:

```bash
export BREEZEBNB_SMTP_HOST="smtp.gmail.com"       # SMTP server (default: smtp.gmail.com)
export BREEZEBNB_SMTP_PORT="587"                   # SMTP port (default: 587)
export BREEZEBNB_SMTP_USER="your-sender@gmail.com" # Sender email / SMTP username
export BREEZEBNB_SMTP_PASSWORD="your-app-password"  # SMTP password or Gmail app password
export BREEZEBNB_NOTIFY_EMAIL="nevradonatwork@gmail.com"  # Recipient email address
```

**Gmail users:** Generate an [App Password](https://myaccount.google.com/apppasswords) (requires 2-Step Verification) and use it as `BREEZEBNB_SMTP_PASSWORD`.

If `BREEZEBNB_SMTP_USER` or `BREEZEBNB_SMTP_PASSWORD` are not set, the email notification is silently skipped and the pipeline continues normally.

### Email Content

Each notification email includes:

- **Run ID** and **status** (SUCCESS / FAILED)
- **Start and end timestamps**
- **Cities processed** and **cities failed**
- **Row counts** per table layer
- **Reconciliation summary** — match/mismatch counts across all 3 rec tables, with specific mismatch details when issues are detected
- **Detailed errors and warnings** from `etl_error_log`

## Scheduling

The included `crontab` file configures the pipeline to run on the 2nd of each month at 6:00 AM UTC. The orchestrator reads this file and schedules the job automatically.

## Assumptions

See [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md) for production deployment strategy and full list of assumptions.

