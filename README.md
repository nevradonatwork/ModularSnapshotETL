# ModularSnapshotETL Data Platform

A medallion-architecture data engineering platform that transforms raw Airbnb-style listing data into a Postgres (Neon) analytical warehouse — SQLite locally — with dimensional modelling, data quality controls, pipeline observability, and BI-ready reporting views.

**Live Dashboard:** [modularsnapshotetl.streamlit.app](https://modularsnapshotetl.streamlit.app/)

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

The pipeline auto-discovers all city subdirectories, processes them through the bronze/silver/gold layers, and writes everything into `ModularSnapshotETL.db` locally (or the configured Postgres/Neon database in production — see `src/db.py`).

## Business Outputs

The platform produces three core insights for market intelligence:

- **Monthly average price per night by neighbourhood and room type** — aggregated per room type (Entire home/apt, Private room, etc.) **and** as a combined "ALL" average, broken down by city and month, available via `vw_rep_monthly_neighbourhood_avg_price`
- **Top 10 overpriced and top 10 underpriced listings** — each listing compared against both its room-type-specific neighbourhood average and the combined "ALL" neighbourhood average, with delta amount and percentage, available via `vw_rep_monthly_top10_overpriced` and `vw_rep_monthly_top10_underpriced`
- **Monthly data compliance report** — per-city quality snapshot tracking missing prices, neighbourhoods, and room types, available via `vw_rep_monthly_data_compliance`

## Design Philosophy

This solution intentionally follows **KISS** (Keep It Simple, Stupid) and **YAGNI** (You Aren't Gonna Need It):

- **Postgres in production, SQLite locally** — Neon (serverless Postgres) backs the live dashboard so data persists across restarts; a single-file SQLite database is used for local development and the test suite, via a thin backend-agnostic connection layer (`src/db.py`).
- **No heavy frameworks** — no Airflow, no Spark, no ORM. The pipeline uses only `pandas` and a small SQL layer. Complexity is added when justified, not before.
- **Cron-based scheduling** — the simplest reliable job runner. Advanced orchestration (Airflow, Dagster) is deferred until the number of cities or job dependencies demands it.
- **Layering is pragmatic, not ceremonial** — each layer (bronze, silver, gold, metadata) exists because it solves one specific problem: auditability, deduplication, history tracking, aggregation, and pipeline observability.
- **No synthetic business logic** — listings with null or zero prices are excluded, not estimated. Only real observed data flows into analytics to preserve analytical integrity.
- **Refactoring over speculation** — features were built incrementally. Advanced orchestration, distributed processing, and cloud-native services are intentionally deferred until scale requires them.

## Data Architecture

The pipeline follows a **medallion architecture**. In production (Postgres/Neon)
each layer is a real Postgres schema; locally (SQLite) every table lives in one
flat namespace, since SQLite has no equivalent to Postgres schemas — every
table/view name is unique across layers, so a single `SET search_path` per
Postgres connection lets all application SQL stay schema-agnostic (see
`src/db.py`).

| Schema | Prefix | Purpose |
|--------|--------|---------|
| `bronze` | `raw_` | Immutable landing storage — source data exactly as received |
| `silver` | `stg_` | Cleansed, standardised, and deduplicated |
| `gold` | `dim_` / `fct_` / `vw_rep_` | Conformed dimensions, fact tables, and BI-ready reporting views |
| `metadata` | `pipeline_`, `row_count_`, `watermark_`, `visitor_`, `rec_` | Pipeline execution/error logs, row-count + checksum reconciliation, watermark tracking, visitor analytics, and business-rule reconciliation |

### Tables

**Bronze (raw):**
- `raw_listings` — append-only, one row per listing per file load

**Silver (staging):**
- `stg_listings` — deduplicated by `(city, snapshot_month, id)`

**Gold — Dimensions:**
- `dim_date` — month-level date dimension
- `dim_city` — city metadata (country, timezone, currency)
- `dim_neighbourhood` — neighbourhood hierarchy per city
- `dim_host` — host attributes with SCD Type 2 history
- `dim_listing` — listing attributes with SCD Type 2 history

**Gold — Facts:**
- `fct_listing_monthly_snapshot` — base fact at listing + month + city grain
- `fct_neighbourhood_monthly_avg_price` — aggregated neighbourhood averages per room type + combined "ALL"
- `fct_neighbourhood_monthly_top10_price_delta` — top 10 over/underpriced listings per room type + combined "ALL"
- `fct_data_compliance_monthly` — monthly data-compliance counters per city

**Gold — Reporting Views:**
- `vw_rep_monthly_neighbourhood_avg_price`
- `vw_rep_monthly_top10_overpriced`
- `vw_rep_monthly_top10_underpriced`
- `vw_rep_monthly_data_compliance`

**Metadata — Pipeline Logging:**
- `pipeline_execution_log` — pipeline execution tracking (pipeline name, start, end, status, rows processed, city, snapshot_month, source file path, archived file path, row counts)
- `pipeline_error_log` — detailed error and warning records

**Metadata — Audit (generic, table-agnostic):**
- `row_count_reconciliation` — source vs target row count + checksum per load stage, per run (`src/audit.py`)
- `watermark_control` — last successful load timestamp + run id, per table

**Metadata — Visitor Analytics:**
- `visitor_log` — one row per browser session: first/last seen, IP, user-agent, best-effort city/country, whether the session ran the pipeline, page-view count (`src/visitor_log.py`)

**Metadata — Business-Rule Reconciliation** (deeper than the generic audit tables above — independently recomputes values and compares them, not just row counts):
- `rec_avg_price_comparison` — staging-calculated avg prices vs `fct_neighbourhood_monthly_avg_price`
- `rec_top10_price_delta_comparison` — staging-calculated top-10 over/underpriced vs `fct_neighbourhood_monthly_top10_price_delta`
- `rec_reporting_view_comparison` — fact table row counts vs reporting view row counts

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
10. Reporting views refresh automatically (native database views, both backends)
11. Log execution metrics to `pipeline_execution_log` (with file metadata and archived path)

## Idempotency

The pipeline is safe to rerun for any city and month:

- **Raw layer**: append-only — rerunning adds a new copy of the source data (full audit trail)
- **Staging layer**: delete-then-insert scoped to `(city, snapshot_month)` — rerunning replaces only that month's cleaned data
- **Dimensions**: upsert logic — existing records are matched; SCD2 creates new versions only when attributes change
- **Facts**: delete-then-insert scoped to `(month_key, city_key)` — rerunning replaces that month's facts cleanly
- **No destructive operations**: other cities and months are never touched

The `snapshot_month` key ensures complete isolation between monthly runs.

## File Traceability & Archiving

Each pipeline run records full file metadata in `pipeline_execution_log`:

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
- The count of flagged rows is logged to `pipeline_error_log` as `GEO_OUT_OF_CITY`
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

## Pipeline Observability — Row-Count Reconciliation & Watermarks

Separate from the business-rule reconciliation above, `src/audit.py` runs a
shallower, generic check at three points in every pipeline run — bronze
(`raw_listings`), silver (`stg_listings`), and the base gold fact table
(`fct_listing_monthly_snapshot`) — comparing source vs target row counts and a
portable SHA-256 checksum over the id set, logged to
`metadata.row_count_reconciliation`. The staging checkpoint commonly shows
`mismatched` by design (dedup + invalid-price exclusion legitimately drop
rows) — the table records the delta for visibility, it isn't a pass/fail gate.

Every successfully loaded table also updates `metadata.watermark_control`
(`last_successful_load_timestamp`, `last_run_id`) — bookkeeping on "when was
this table last fully loaded," not a resumable-extraction cursor, since the
pipeline ingests whole files per `(city, snapshot_month)` upload rather than a
continuously-queryable growing source.

```sql
SELECT * FROM row_count_reconciliation WHERE match_status = 'mismatched';
SELECT * FROM watermark_control ORDER BY last_successful_load_timestamp DESC;
```

## Visitor Analytics

Every dashboard session (not just pipeline runs) is logged to
`metadata.visitor_log` via `src/visitor_log.py`: first/last seen timestamps,
IP address, user-agent, best-effort city/country (`ip-api.com`), whether the
session ran the ETL pipeline, and a page-view count. Logged once per browser
session (guarded by `st.session_state` in `app.py`, which reruns on every
widget interaction/page switch). IP-based geolocation is best-effort and can
be empty or wrong — Streamlit Cloud's proxy layer doesn't always expose a
clean client IP — failures are silent and never block the dashboard.

```sql
SELECT COUNT(*) AS total_visits, SUM(ran_pipeline) AS visits_that_ran_etl
FROM visitor_log;
```

## Data Fetcher — Automatic City Discovery

The platform includes a built-in data fetcher (`src/data_fetcher.py`) that can automatically download listing data from [Inside Airbnb](https://insideairbnb.com/get-the-data/).

**Strategy (2-tier discovery):**

1. **Live scrape** — parses `insideairbnb.com/get-the-data/` for download links. Works when the page serves static HTML.
2. **Built-in catalog fallback** — since the page is a React SPA (JavaScript-rendered), the live scrape may return no results. The catalog contains **100 cities across 30+ countries** with their exact `data.insideairbnb.com` URL path components. When a city is selected, the fetcher sends HEAD requests to probe for the latest available snapshot date.

### Catalog Coverage

| Region | Cities |
|--------|--------|
| United States | 32 — New York, Los Angeles, San Francisco, Chicago, Seattle, Austin, Denver, Nashville, Boston, Washington D.C., and more |
| Canada | 8 — Toronto, Montreal, Vancouver, Ottawa, Quebec City, Victoria, New Brunswick, Winnipeg |
| United Kingdom | 5 — London, Manchester, Bristol, Edinburgh, Glasgow |
| Spain | 9 — Barcelona, Madrid, Mallorca, Valencia, Sevilla, Malaga, Girona, Menorca, Basque Country |
| Italy | 10 — Rome, Milan, Florence, Venice, Naples, Bologna, Sicily, Sardinia, Puglia, Bergamo |
| France | 3 — Paris, Lyon, Bordeaux |
| Germany | 2 — Berlin, Munich |
| Netherlands | 1 — Amsterdam |
| Portugal | 2 — Lisbon, Porto |
| Greece | 4 — Athens, Crete, Thessaloniki, South Aegean |
| Australia | 6 — Sydney, Melbourne, Tasmania, Northern Rivers, Barossa Valley, Western Australia |
| Other | Ireland, Belgium, Austria, Switzerland, Denmark, Sweden, Norway, Czech Republic, Turkey, New Zealand, Japan, China, Thailand, Singapore, South Africa, Mexico, Brazil, Argentina, Colombia, Cuba |

### CLI Usage

```bash
python -m src.data_fetcher --list              # list all 100 cities
python -m src.data_fetcher --city new-york-city # download a specific city
python -m src.data_fetcher --all               # download all cities
```

To add a new city, add one line to `CITY_CATALOG` in `src/data_fetcher.py` with the URL path components.

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
- **No production side effects:** tests do not write to production `ModularSnapshotETL.db`; in-memory DB state disappears when each test ends.

### Outcome Visibility

When running with `-v`, each test line shows `PASSED` / `FAILED`; the final line summarizes the run result (e.g., `54 passed`).

## Project Structure

```
ModularSnapshotETL/
├── main.py                # Entry point — creates ModularSnapshotETL.db, discovers cities, runs pipeline
├── app.py                 # Streamlit dashboard entry point
├── crontab                # Schedule definition for the orchestrator
├── requirements.txt       # Python dependencies
├── src/
│   ├── db.py              # Backend-agnostic connection layer (Postgres/Neon or SQLite)
│   ├── schema.py          # DDL for all schemas, tables, indexes, and views
│   ├── ingestion.py       # Bronze and silver layer loading
│   ├── validation.py      # Schema checks and data quality controls
│   ├── etl_logging.py     # Pipeline execution/error logging
│   ├── audit.py           # Row-count/checksum reconciliation and watermark tracking
│   ├── visitor_log.py     # Visitor session logging for the dashboard
│   ├── email_notify.py    # Email notifications after pipeline runs
│   ├── dimensions.py      # Dimension table upserts (SCD2)
│   ├── facts.py           # Fact table loading and aggregations
│   ├── reconciliation.py  # Business-rule data comparison across layers
│   ├── pipeline.py        # Orchestrates the full execution flow
│   └── data_fetcher.py    # Inside Airbnb data fetcher (100-city catalog + live scraper)
├── pages/
│   ├── 1_Home.py          # Dashboard home / project overview
│   ├── 2_Load_Data.py     # City selection + ETL pipeline runner
│   ├── 3_Dashboard.py     # Interactive analytics dashboard
│   └── 4_Data_Dictionary.py # Table/column documentation
├── dashboard/
│   ├── constants.py       # App-wide constants and city catalog
│   ├── db.py              # Database query helpers
│   ├── charts.py          # Chart visualization functions
│   ├── filters.py         # City and filter rendering
│   ├── pipeline_runner.py # ETL pipeline execution for the dashboard
│   └── data_dictionary.py # Data dictionary utilities
├── scripts/
│   └── migrate_to_medallion_schema.sql # One-time production migration to bronze/silver/gold/metadata
├── tests/
│   ├── conftest.py        # Shared fixtures (in-memory DB, sample data)
│   ├── test_ingestion.py  # Raw/staging layer tests
│   ├── test_dimensions.py # Dimension loading and SCD2 tests
│   ├── test_facts.py      # Fact tables and presentation view tests
│   ├── test_reconciliation.py # Reconciliation layer tests
│   ├── test_audit.py      # Row-count/checksum reconciliation and watermark tests
│   ├── test_visitor_log.py # Visitor session logging tests
│   ├── test_pipeline.py   # End-to-end pipeline tests
│   └── test_main.py       # City discovery tests
├── dataset/               # Input: dataset/<city>/listings.csv.gz (not committed)
└── ModularSnapshotETL.db  # Output: local SQLite database (not committed; Postgres/Neon in production)
```

## Live Dashboard

The Streamlit dashboard is deployed at: [modularsnapshotetl.streamlit.app](https://modularsnapshotetl.streamlit.app/)

To run locally:
```bash
streamlit run app.py
```

## Email Notifications

The pipeline sends an email notification after every run with a summary of the execution, including run status, row counts, and any errors or warnings.

### Setup

Set the following environment variables before running the pipeline:

```bash
export MODULARSNAPSHOTETL_SMTP_HOST="smtp.gmail.com"       # SMTP server (default: smtp.gmail.com)
export MODULARSNAPSHOTETL_SMTP_PORT="587"                   # SMTP port (default: 587)
export MODULARSNAPSHOTETL_SMTP_USER="your-sender@gmail.com" # Sender email / SMTP username
export MODULARSNAPSHOTETL_SMTP_PASSWORD="your-app-password"  # SMTP password or Gmail app password
export MODULARSNAPSHOTETL_NOTIFY_EMAIL="nevradonatwork@gmail.com"  # Recipient email address
```

**Gmail users:** Generate an [App Password](https://myaccount.google.com/apppasswords) (requires 2-Step Verification) and use it as `MODULARSNAPSHOTETL_SMTP_PASSWORD`.

If `MODULARSNAPSHOTETL_SMTP_USER` or `MODULARSNAPSHOTETL_SMTP_PASSWORD` are not set, the email notification is silently skipped and the pipeline continues normally.

### Email Content

Each notification email includes:

- **Run ID** and **status** (SUCCESS / FAILED)
- **Start and end timestamps**
- **Cities processed** and **cities failed**
- **Row counts** per table layer
- **Reconciliation summary** — match/mismatch counts across all 3 rec tables, with specific mismatch details when issues are detected
- **Detailed errors and warnings** from `pipeline_error_log`

## Scheduling

The included `crontab` file configures the pipeline to run on the 2nd of each month at 6:00 AM UTC. The orchestrator reads this file and schedules the job automatically.

## Assumptions

See [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md) for production deployment strategy and full list of assumptions.
