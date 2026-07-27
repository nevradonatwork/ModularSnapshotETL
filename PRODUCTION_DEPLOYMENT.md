# Production Deployment View

**Live Dashboard:** [modularsnapshotetl.streamlit.app](https://modularsnapshotetl.streamlit.app/)

## Architecture

In production, the pipeline runs as a scheduled job on a cloud platform (e.g., AWS, GCP).
The orchestrator reads the `crontab` file and triggers the pipeline on the 2nd of each month,
giving end-users a one-day window to upload the monthly `listings.csv.gz`.

**Database:** The pipeline writes all data into a SQLite database (`ModularSnapshotETL.db`) with five
logical layers: raw, staging, dimensions, facts, and presentation views. For production at
scale, the SQLite database can be replaced with a cloud data warehouse (BigQuery, Snowflake,
or Redshift), the layered architecture and SQL schema translate directly.

**Storage:** Input datasets move from a local `dataset/` directory to cloud object storage
(e.g., S3 or GCS), partitioned by city and month (`s3://modularsnapshotetl/raw/{city}/listings.csv.gz`).
The raw layer (`raw_listings`) provides an immutable audit trail within the database, enabling
replay and reprocessing without re-downloading source files.

**Data Layers:** The five-layer architecture supports clean separation of concerns:
- **Raw** (`raw_listings`): Append-only, no transformations, full audit trail
- **Staging** (`stg_listings`): Cleansed, standardised booleans, parsed prices, deduplicated
- **Dimensions** (`dim_date`, `dim_city`, `dim_neighbourhood`, `dim_host`, `dim_listing`): Conformed dimensions with SCD Type 2 history tracking for hosts and listings
- **Facts** (`fct_listing_monthly_snapshot`, `fct_neighbourhood_monthly_avg_price`, `fct_neighbourhood_monthly_top10_price_delta`, `fct_data_compliance_monthly`): Analytical grain at listing + month + city level, with room-type breakdowns (per room type + combined "ALL"), plus data-compliance monitoring
- **Presentation** (`vw_rep_*`): BI-ready views joining facts and dimensions with human-readable fields, excluding surrogate keys

**Orchestration:** The crontab-based scheduler works for the current setup. As
complexity grows (multiple cities, dependencies between jobs, retries), migrating to a
workflow orchestrator like Apache Airflow or Dagster would provide dependency management,
automatic retries, backfill capabilities, and a monitoring UI.

**Data Quality & Validation:** The pipeline includes built-in validation controls:
- Schema validation: required columns, parsable data types
- Data quality checks: no duplicate keys in staging, critical columns not null, price > 0, availability between 0–365
- Geo validation: listing coordinates checked against per-city bounding boxes (see Geo Validation section below)
- Critical validation failures cause the pipeline to fail fast
- Warnings are logged to `pipeline_error_log` with full context
- All runs tracked in `pipeline_execution_log` with start/end times, status, city, snapshot_month, source file path, archived file path, row counts, and error messages
- Pipeline returns non-zero exit code on failure for cron monitoring

**File Archiving:** After successful raw ingestion, the source file is moved to an archive
subdirectory to preserve the exact processed artifact and prevent accidental overwrites:

```
dataset/<city>/archive/<city>_<snapshot_month>_<utc_timestamp>_listings.csv.gz
```

Example: `dataset/new-york/archive/new-york_2025-09-01_20260213T104527Z_listings.csv.gz`

Key behaviours:
- The archive directory (`dataset/<city>/archive/`) is created automatically
- Both the original path and archived path are recorded in `pipeline_execution_log` for full traceability
- If raw ingestion fails, the file is **not** archived, it remains in place for investigation and retry
- UTC timestamp in the filename ensures uniqueness across multiple runs of the same month
- For reprocessing, retrieve the file from the archive directory and place it back in the city directory

In production with cloud object storage, the archive step would move files to a cold-storage
tier (e.g., S3 Glacier, GCS Nearline) instead of a local subdirectory.

**Geo Validation:** Listings are validated against per-city geographic bounding boxes during
staging. A `geo_out_of_city_flag` column in `stg_listings` indicates whether a listing's
coordinates fall within the expected city boundaries:

- `geo_out_of_city_flag = 0`, Listing property coordinates (latitude/longitude) are within the city bounding box
- `geo_out_of_city_flag = 1`, Listing property coordinates are outside the city bounding box, or lat/long is missing

This is a **data quality / geo-consistency flag** that validates the listing's physical
property location, not the host's declared location. For example, a listing filed under
`new-orleans` but with coordinates in Houston would be flagged. A host living in Baton Rouge
who lists a property physically located in New Orleans would **not** be flagged.

Data flow for geo-flagged rows:
- **Raw layer**: all rows kept (immutable audit trail)
- **Staging layer**: flagged rows are kept with `geo_out_of_city_flag = 1`, data is never silently deleted
- **Fact layer**: flagged rows are excluded from aggregates to prevent skewed neighbourhood pricing analytics
- **Logging**: count of flagged rows logged to `pipeline_error_log` as `GEO_OUT_OF_CITY`

Bounding boxes are configured in `CITY_BOUNDARIES` in `src/validation.py` for: new-york,
chicago, los-angeles, san-francisco, new-orleans. Cities without a configured bounding box
have all rows pass (flag = 0). Adding a new city requires only adding its bounding box
coordinates, no code changes needed.

**Data Fetcher:** The platform includes an automated data fetcher (`src/data_fetcher.py`) that downloads
listing data directly from [Inside Airbnb](https://insideairbnb.com/get-the-data/). It uses a 2-tier
discovery strategy:

1. **Live scrape**, parses the Inside Airbnb page for download links (works when HTML is server-rendered)
2. **Built-in catalog**, a comprehensive catalog of **100 cities across 30+ countries** with exact
   `data.insideairbnb.com` URL path components, used as a reliable fallback when the JS-rendered page
   returns no links

When using the catalog fallback, the fetcher probes `data.insideairbnb.com` with HEAD requests to
discover the latest available snapshot date (checks days 1–5 of each month for the past 12 months).
In production, this eliminates the manual step of downloading and placing files, users select a city
from the dashboard and the pipeline handles ingestion end-to-end.

The dashboard is deployed at [modularsnapshotetl.streamlit.app](https://modularsnapshotetl.streamlit.app/)
and provides a searchable city dropdown covering all 100 catalogued cities.

**Error Handling:** The pipeline handles:
- Missing file (FileNotFoundError)
- Corrupted gzip (pandas read error)
- Missing columns (ValidationError, fail fast)
- Invalid date format (coerced to null)
- Schema drift, only columns matching the raw table schema are written; unknown columns are ignored and logged for visibility but do not break the pipeline. Staging uses a known column set, isolating downstream layers from source schema changes.
- Null or zero prices, listings with invalid prices are excluded from analysis and logged to `pipeline_error_log` with the count of excluded rows. No synthetic price estimation is performed.

**Monitoring & Alerting:** Pipeline health metrics are stored in `pipeline_execution_log`:
- Run duration (start_time, end_time)
- City and snapshot_month per run
- Source file path and file name
- Archived file path (after successful ingestion)
- Row counts per layer (JSON in row_counts column), including `geo_out_of_city_count`
- Success/failure status
- Error messages for failed runs
- Detailed error log in `pipeline_error_log` with table name, error type, and timestamp
- The pipeline returns a non-zero exit code on failure. In production, this exit code would trigger alerting via CloudWatch, Stackdriver, PagerDuty, or email integration with the orchestrator.

**Scaling, Multi-City Expansion:** The dimensional model scales naturally to 20+ cities.
Each city gets its own `dim_city` record, and all facts are partitioned by `city_key` and
`month_key`. City auto-discovery (`dataset/<city>/listings.csv.gz`) means adding a new city
requires zero code changes, just drop the file and rerun. For true multi-city parallelism,
each city can be processed in a separate pipeline run sharing the same database, or in
parallel workers writing to separate databases that are merged downstream.

**Scaling, Compute:** Pandas handles single-city datasets well. If individual city datasets
grow beyond memory limits, the processing layer can switch to Dask (minimal code change) or
PySpark. The modular design, where each layer is an independent module, means scaling
affects the execution engine, not the business logic.

**Idempotency:** The pipeline is safe to rerun for any city and month without manual cleanup:
- Raw layer is append-only (new rows are always added for auditability)
- Staging, facts, and aggregations use delete-then-insert scoped to `(city, snapshot_month)`, only the target month is replaced
- Dimension upserts match on natural keys; SCD2 creates new versions only when attributes actually change
- No destructive operations, other cities and months are never touched
- The `snapshot_month` key ensures complete isolation between monthly runs


**Data Compliance Monitoring:** A dedicated monthly compliance fact is stored in `fct_data_compliance_monthly` and published via `vw_rep_monthly_data_compliance`.

**Neighbourhood Files per City:** The production dataset folder may include:
- `dataset/<city>/neighbourhoods.csv` (master neighbourhood + group definitions)
- `dataset/<city>/neighbourhoods.geojson` (neighbourhood polygons)

During pipeline runs, these are used to seed/enrich `dim_neighbourhood` and to backfill empty listing neighbourhood values via geographic matching.


**Neighbourhood Master Data Policy:** Neighbourhood attribution is treated as critical reference data.
- `dim_neighbourhood` should be seeded from curated neighbourhood files (including neighbourhood groups where provided).
- If listing rows arrive with empty `neighbourhood_cleansed`, they should be backfilled from geographic boundaries (GeoJSON point-in-polygon using listing latitude/longitude).
- If no polygon match is found, rows remain visible in compliance counters and operational alerts for remediation.


For each `(city, snapshot_month)`, the pipeline records:
- total staged rows (`rows_count`)
- compliant rows (`compliance_data_count`) where `price_amount`, `neighbourhood_cleansed`, and `room_type` are all present
- missing-field counters: `missing_price_count`, `missing_neighbourhood_cleansed_count`, `missing_room_type_count`

This provides an auditable quality snapshot per month before downstream consumption.

Operational expectation in production:
- Compliance metrics are reviewed every run and retained historically for trend analysis.
- Drops in compliance rate should trigger investigation before distributing monthly reporting outputs.
- `vw_rep_monthly_data_compliance` is the primary contract for operational monitoring dashboards/alerts.

**Failure Strategy:** The pipeline follows a fail-fast, isolate-per-city approach:
- Structural failures (missing file, missing columns, corrupted gzip) halt the pipeline immediately for that city
- Data quality warnings (duplicates, out-of-range values) are logged to `pipeline_error_log` but do not halt processing
- If one city fails, the ETL run is marked FAILED in `pipeline_execution_log` with the full error message; other cities continue processing independently
- The pipeline returns a non-zero exit code if any city fails, enabling cron/orchestrator alerting
- Partial state is never left in an ambiguous state, staging and fact layers are fully replaced per run, not incrementally appended

**Rerun & Backfill Strategy:**
- **Rerunning a month:** Retrieve the file from `dataset/<city>/archive/` (or place a corrected `listings.csv.gz`) in the city directory and rerun. The staging and fact layers for that `(city, snapshot_month)` are fully replaced. Raw layer retains both the original and corrected loads for auditability. The reprocessed file is archived with a new timestamp, preserving both versions.
- **Backfilling historical months:** Place historical files one at a time, run the pipeline for each. Each run is isolated by `snapshot_month`, so backfilling January does not affect February's data. Each processed file is automatically archived after ingestion.
- **No automatic backfill:** The pipeline processes whatever file is present. It does not maintain a manifest of expected months or attempt to fill gaps on its own.

**Data Retention:** All data layers are retained indefinitely by default:
- Raw layer provides a complete audit trail of every file ever loaded
- SCD Type 2 dimensions preserve the full history of host and listing attribute changes
- Fact tables retain every monthly snapshot
- In production, a retention policy (e.g., archive raw data older than 24 months to cold storage) should be defined based on compliance and storage requirements

**Configuration Management:** The pipeline uses minimal configuration:
- `DATASET_DIR`, `LISTING_FILENAME`, and `DATABASE_PATH` are constants in `main.py`
- City list is auto-discovered from the filesystem, no configuration file needed
- Dimension defaults (country, timezone, currency) are set in `dim_city` schema defaults
- For production, these constants can be moved to environment variables or a YAML config file. No framework-level config management (Hydra, OmegaConf) is introduced until the complexity justifies it.

## Testing & Release Confidence

The repository includes an automated `pytest` suite that is intended to be part of pre-release validation.

- **Execution command:** `pytest tests/ -v`
- **Current scope:** ingestion, staging cleansing/deduplication, dimension upserts (including SCD2 behavior), fact loading, reporting views, pipeline orchestration, archiving, geo validation, and city discovery.
- **Database strategy in tests:** tests run against a per-test in-memory SQLite DB (`:memory:`), so they perform real inserts/updates/selects without mutating production data.
- **Practical guarantee:** each change should keep all tests green before deployment; failures indicate a behavioral regression in transformation logic or orchestration flow.

This testing model aligns with the project’s KISS/YAGNI philosophy by validating business-critical behavior directly with lightweight infrastructure (SQLite + pytest) instead of introducing additional test platforms.

## Design Philosophy

This solution intentionally follows **KISS** and **YAGNI** principles. It uses:

- A simple layered architecture (raw → staging → dimensions → facts → views) where each layer solves one specific problem
- SQLite for portability and minimal setup, zero infrastructure, single file, full SQL support
- Cron-based scheduling, the simplest reliable job runner
- Clear table-based modelling without unnecessary frameworks or abstractions
- Only `pandas` and Python's built-in `sqlite3`, no ORM, no Spark, no Airflow

Advanced orchestration, distributed processing, and cloud-native services are intentionally
deferred until scale requires them. The architecture is designed so that migration to
BigQuery/Snowflake or Airflow/Dagster requires changing the execution engine, not the business
logic or data model.

## Assumptions

### Data

1. End-users upload `listings.csv.gz` by the 1st of each month; the pipeline runs on the 2nd.
2. The dataset schema (Inside Airbnb format) remains stable across cities and months.
3. The `price` field represents the nightly rate and includes only the base price (no taxes/fees).
4. All prices are in USD. Currency metadata is stored in `dim_city` to support future
   multi-currency expansion, but no conversion is performed currently.
5. `neighbourhood_cleansed` is the authoritative neighbourhood field (as per Inside Airbnb conventions).
6. Listings with a price of $0 or missing prices are excluded as invalid data.
7. Exactly one `listings.csv.gz` per city per run, not multiple files or incremental deltas.
8. Input data may contain duplicates. The staging layer deduplicates by `(city, snapshot_month, id)`,
   keeping the row with the latest `last_scraped` date (tie-broken by `insert_date_utc`).
9. `snapshot_month` is derived from each record's `last_scraped` date truncated to month start
   (e.g., `2025-12-05` becomes `2025-12-01`). This is deterministic and does not depend on file
   metadata or upload timing.
10. Each city's dataset fits in memory. Pandas loads the full file into RAM for processing.

### Pipeline Behaviour

11. Each city is processed independently, no cross-city insights are needed initially.
12. The `snapshot_month` is derived from `last_scraped` truncated to month start and stored as a
    date string (e.g., `2025-12-01`) in all tables. This is deterministic per record.
13. If the pipeline fails for a city, the ETL run is marked as FAILED in `pipeline_execution_log` with the
    error message preserved. Other cities continue processing.
14. Historical data is preserved across runs, the raw layer is append-only, and dimensions use
    SCD Type 2 to track changes over time.
15. Reporting views (`vw_rep_*`) refresh automatically as SQLite views read live from fact and
    dimension tables.

### Operational

16. The orchestrator environment has Python and the required dependencies pre-installed.
17. The crontab schedule assumes monthly data drops. A change to weekly or daily frequency
    would require updating both the schedule and the snapshot_month derivation logic.
18. The pipeline logs execution metrics to `pipeline_execution_log` and `pipeline_error_log` and sends email
    notifications with run summaries (configurable via environment variables). Business user
    notification that fresh insights are available remains an external concern.
19. The cron schedule uses UTC (6:00 AM). This is assumed acceptable for all teams regardless
    of city timezone.
20. The SQLite database file (`ModularSnapshotETL.db`) is excluded from version control. In production,
    this would be replaced by a persistent cloud database.
21. Processed source files are archived after successful raw ingestion. The archive directory
    (`dataset/<city>/archive/`) preserves the exact file that was processed for audit compliance
    and reprocessing capability.
22. Geo validation bounding boxes are approximate city-level boundaries. Listings near city
    borders may be incorrectly flagged. The flag is a data quality indicator, not a hard filter,
    flagged rows are kept in staging for transparency and only excluded from fact-layer aggregates.
