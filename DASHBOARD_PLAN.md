# ModularSnapshotETL — Web Dashboard Developer Plan

## Goal

Build a single-page-style web application that demonstrates the ModularSnapshotETL ETL project end-to-end:

1. **Explain** the project clearly on a landing page (interview-ready).
2. **Let users load** Inside Airbnb listing data by selecting a city.
3. **Run the pipeline** and show real-time progress.
4. **Display results** in an interactive dashboard built from the 4 reporting views.
5. **Expose the data dictionary** so users understand fields and quality rules.

The app must be good enough to use as a live demo in interviews.

---

## Technology Stack

| Layer | Choice | Rationale |
|-------|--------|-----------|
| **Framework** | **Streamlit** | Fastest path to a polished, interactive web app. No separate frontend build. Native charts, tables, progress bars, sidebar filters, and file uploader. |
| **Charts** | **Plotly** (via `plotly.express`) | Interactive hover, zoom, click. Seamless Streamlit integration via `st.plotly_chart`. |
| **Backend** | Existing Python modules (`src/pipeline.py`, `src/ingestion.py`, etc.) | No new API layer needed — Streamlit calls Python functions directly. |
| **Database** | Existing `ModularSnapshotETL.db` (SQLite) | Already produced by the pipeline. Read with `sqlite3` + `pandas`. |
| **New dependencies** | `streamlit`, `plotly` | Add to `requirements.txt`. |

### Why Streamlit over React

- Zero build tooling — `streamlit run app.py` and it works.
- Native Python — reuses every existing module directly.
- Sidebar filters, file upload, progress bars, tabs, and modal-style expanders are built-in.
- Interview demo: open laptop, run one command, show the app.
- Looks professional enough for a data engineering showcase.

---

## New Files to Create

```
ModularSnapshotETL/
├── app.py                          # Streamlit entry point (multi-page router)
├── pages/
│   ├── 1_Home.py                   # Page 1: Project overview
│   ├── 2_Load_Data.py              # Page 2: Data load panel
│   ├── 3_Dashboard.py              # Page 3: Interactive dashboard (4 views)
│   └── 4_Data_Dictionary.py        # Page 4: Column dictionary + quality rules
├── dashboard/
│   ├── __init__.py
│   ├── db.py                       # Shared DB connection helpers
│   ├── charts.py                   # Plotly chart-building functions
│   ├── filters.py                  # Sidebar filter widgets (city, month, room_type)
│   ├── pipeline_runner.py          # Wrapper: run pipeline with Streamlit progress
│   ├── data_dictionary.py          # Data dictionary definitions (list of dicts)
│   └── constants.py                # App-wide constants, page config
├── assets/
│   └── architecture_diagram.png    # Simple pipeline flow diagram (optional)
```

### Existing files modified

| File | Change |
|------|--------|
| `requirements.txt` | Add `streamlit>=1.30.0` and `plotly>=5.18.0` |
| `.gitignore` | Add `.streamlit/` |

No changes to any `src/` or `tests/` files.

---

## Page-by-Page Specification

---

### Page 1 — Home / Project Overview (`pages/1_Home.py`)

**Purpose:** Interview-friendly explanation of the project.

#### Sections (top to bottom)

1. **Hero**
   - Project name: "ModularSnapshotETL Data Platform"
   - One-sentence value statement: "A layered data engineering platform that transforms raw Airbnb-style listing data into a SQLite analytical warehouse with dimensional modelling, data quality controls, and BI-ready reporting views."
   - Two CTA buttons: `[Load Data →]` (links to Page 2) and `[View Dashboard →]` (links to Page 3).

2. **Architecture Summary**
   - Display a simple 5-step flow using `st.columns` or an image:
     ```
     CSV Upload → Raw Layer → Staging Layer → Dimensions + Facts → Reporting Views
     ```
   - Under each step, one-line description:
     - **Raw**: Immutable append-only landing (audit trail)
     - **Staging**: Cleansed, deduplicated, geo-validated
     - **Dimensions**: Conformed dims with SCD2 history (host, listing)
     - **Facts**: Monthly snapshots, neighbourhood averages, top-10 deltas, compliance
     - **Views**: BI-ready reporting — 4 views ready for dashboards

3. **What the Pipeline Produces**
   - Bullet list (use `st.markdown`):
     - Monthly average price per night by neighbourhood and room type → `vw_rep_monthly_neighbourhood_avg_price`
     - Top 10 overpriced and underpriced listings → `vw_rep_monthly_top10_overpriced` / `vw_rep_monthly_top10_underpriced`
     - Monthly data compliance report → `vw_rep_monthly_data_compliance`

4. **Production Mindset Highlights**
   - Render as a 2×3 grid of metric-style cards using `st.columns`:
     - Idempotent reruns
     - ETL run logging + error tracking
     - File archiving with traceability
     - Geo validation (bounding box)
     - Data compliance monitoring
     - Multi-city auto-discovery

5. **Tech Stack**
   - Simple table: Python 3.11, SQLite, Pandas, pytest (70 tests), cron scheduling.
   - GitHub repo link (if public).
   - "How to run locally" code block: `pip install -r requirements.txt && python main.py`

---

### Page 2 — Data Load (`pages/2_Load_Data.py`)

**Purpose:** Let users ingest a city dataset and run the pipeline.

#### Layout

Two sections stacked vertically: **Data Load Form** on top, **Run Status** below.

#### A) Data Load Form

| Element | Widget | Details |
|---------|--------|---------|
| City name | `st.text_input` + help text | Required. Show placeholder: `new-york`. Show note: "Use lowercase slug format: `new-york`, `chicago`, `los-angeles`." |
| Source method | `st.radio` | Three options (see below) |
| Action button | `st.button("Ingest & Run Pipeline")` | Disabled until city name filled. |

**Source method options:**

1. **Upload file** — `st.file_uploader(type=["csv.gz", "gz"])`. Save uploaded file to `dataset/<city>/listings.csv.gz`, creating the directory if needed.
2. **Paste URL** — `st.text_input("URL to listings.csv.gz")`. Download with `urllib.request.urlretrieve` or `requests.get` to `dataset/<city>/listings.csv.gz`.
3. **Pick from Inside Airbnb** — Show a curated list of cities/URLs scraped or hard-coded from `https://insideairbnb.com/get-the-data/`. User selects from a `st.selectbox`. On select, auto-fill city name and URL.

**Pre-flight validation** (before running pipeline):

1. City name is not empty.
2. File exists at `dataset/<city>/listings.csv.gz` after upload/download.
3. Quick schema check: read first 5 rows with pandas, verify `REQUIRED_COLUMNS` from `src/validation.py` are present (`id`, `name`, `neighbourhood_cleansed`, `price`, `last_scraped`).
4. If validation fails, show `st.error()` with details. Do not run pipeline.

#### B) Pipeline Execution (on button click)

Use `st.status` (or `st.spinner` + `st.progress`) to show live progress through the pipeline stages.

**Implementation in `dashboard/pipeline_runner.py`:**

```python
def run_with_progress(city: str, dataset_path: str, db_path: str):
    """
    Wraps src/pipeline.py to run the ETL with Streamlit progress updates.
    """
```

The function should:

1. Open SQLite connection via `main.get_connection(db_path)`.
2. Call `schema.create_all(conn)` (idempotent).
3. Call `pipeline.run(conn, dataset_path, city)`.
4. Return the `row_counts` dict and run status.

Display progress steps as they complete (approximate — the pipeline runs as one call, so show a `st.status` expander that updates):

| Step | Label |
|------|-------|
| 1 | Discovering city... |
| 2 | Loading raw data... |
| 3 | Transforming staging... |
| 4 | Updating dimensions... |
| 5 | Building fact tables... |
| 6 | Running reconciliation... |
| 7 | Done |

**Note:** Since `pipeline.run()` is a single function call, the simplest approach is to show a spinner during execution and display detailed results afterward. If finer-grained progress is desired in the future, `pipeline.run()` could be refactored to accept a callback — but that is out of scope for this plan. Do not modify `src/pipeline.py`.

#### C) Run Status Card (after pipeline completes)

Display with `st.success` / `st.error` + `st.metric` cards:

| Metric | Source |
|--------|--------|
| Status | SUCCESS / FAILED |
| Snapshot month | From `derive_snapshot_month` (returned in row_counts context) |
| Raw rows ingested | `row_counts["raw_listings"]` |
| Staging rows | `row_counts["stg_listings"]` |
| Invalid price excluded | `row_counts["invalid_price_excluded"]` |
| Geo flagged count | `row_counts["geo_out_of_city_count"]` |
| Facts loaded | `row_counts["fct_listing_monthly_snapshot"]` |
| Compliance rows | `row_counts["fct_data_compliance_monthly"]` |

Show a `[View Dashboard →]` button linking to Page 3.

If the run fails, show the error message from the exception.

#### D) Run History Panel

Below the run status, show a collapsible `st.expander("Run History")`:

- Query `pipeline_execution_log` ordered by `start_time DESC`, limit 20.
- Display as `st.dataframe` with columns: `run_id`, `start_time`, `end_time`, `status`, `city`, `snapshot_month`, `source_file_name`, `archived_file_path`.
- Click a row (use `st.data_editor` with selection or `st.selectbox`) to show:
  - `row_counts` JSON parsed and displayed as metric cards.
  - Errors from `pipeline_error_log` for that `run_id`.
  - Schema drift warnings (error_type containing "UNKNOWN_COLUMN" or "SCHEMA").

---

### Page 3 — Dashboard (`pages/3_Dashboard.py`)

**Purpose:** Visualise the 4 reporting views interactively.

#### Pre-requisite Check

On page load, check if `ModularSnapshotETL.db` exists and has data:

```python
conn = get_connection()
count = pd.read_sql("SELECT COUNT(*) as n FROM fct_listing_monthly_snapshot", conn)
```

If empty, show `st.warning("No data loaded yet.")` with a link to Page 2.

#### Global Sidebar Filters (`dashboard/filters.py`)

All filters are rendered in `st.sidebar` and applied to every query on this page.

| Filter | Widget | Source Query |
|--------|--------|--------------|
| City | `st.selectbox` | `SELECT DISTINCT city_name FROM dim_city WHERE is_active = 1` |
| Snapshot month | `st.selectbox` | `SELECT DISTINCT month_start_date FROM dim_date d JOIN fct_listing_monthly_snapshot f ON d.month_key = f.month_key ORDER BY month_start_date DESC` |
| Room type | `st.selectbox` | `['ALL', 'Entire home/apt', 'Private room', 'Shared room', 'Hotel room']` — hard-coded list + "ALL" |
| Neighbourhood | `st.multiselect` (optional) | `SELECT DISTINCT neighbourhood FROM vw_rep_monthly_neighbourhood_avg_price WHERE city_name = ? AND month_start_date = ?` |

All view queries use `WHERE city_name = ? AND month_start_date = ?` plus optional `room_type` and `neighbourhood` filters.

#### Dashboard Components

Render using `st.tabs` with 4 tabs, one per view.

---

##### Tab 1: Monthly Neighbourhood Avg Price

**Data source:** `vw_rep_monthly_neighbourhood_avg_price`

**Query:**
```sql
SELECT neighbourhood, neighbourhood_group, room_type,
       avg_price, listing_count, currency_code
FROM   vw_rep_monthly_neighbourhood_avg_price
WHERE  city_name = :city
  AND  month_start_date = :month
  AND  (:room_type = 'ALL' OR room_type = :room_type)
ORDER BY avg_price DESC
```

**Chart:** Horizontal bar chart (Plotly `px.bar`).
- X-axis: `avg_price`
- Y-axis: `neighbourhood` (sorted by avg_price descending)
- Color: `room_type`
- Hover: neighbourhood, room_type, avg_price, listing_count

**Table below chart:** Full data as `st.dataframe`, sortable, with columns: neighbourhood, neighbourhood_group, room_type, avg_price (formatted as currency), listing_count.

**Tooltip / info:** `st.info("ℹ️ Average nightly price per neighbourhood, calculated from fact table fct_neighbourhood_monthly_avg_price. Geo-flagged listings are excluded.")`

---

##### Tab 2: Top 10 Overpriced

**Data source:** `vw_rep_monthly_top10_overpriced`

**Query:**
```sql
SELECT listing_id, property_type, room_type,
       compared_against_room_type, neighbourhood,
       price_amount, neighbourhood_avg_price,
       price_delta, price_delta_pct, rank_in_neighbourhood,
       currency_code
FROM   vw_rep_monthly_top10_overpriced
WHERE  city_name = :city
  AND  month_start_date = :month
  AND  (:room_type = 'ALL' OR compared_against_room_type = :room_type)
ORDER BY price_delta DESC
```

**Chart:** Plotly `px.bar` — grouped bar showing `price_amount` vs `neighbourhood_avg_price` per listing, colored by delta direction.

**Table:** `st.dataframe` with columns:
- `rank_in_neighbourhood`
- `listing_id`
- `neighbourhood`
- `room_type`
- `compared_against_room_type`
- `price_amount` (formatted as currency)
- `neighbourhood_avg_price` (formatted as currency)
- `price_delta` (formatted, colored green/red)
- `price_delta_pct` (formatted as %)

**Listing Detail Expander:** When a user clicks/selects a row, show an `st.expander` with:
- Query `stg_listings` for that `listing_id`, `city`, `snapshot_month`:
  ```sql
  SELECT id, name, host_name, neighbourhood_cleansed, room_type,
         price_amount, property_type, accommodates, bedrooms, beds,
         availability_365, number_of_reviews, review_scores_rating,
         latitude, longitude, geo_out_of_city_flag, last_scraped
  FROM   stg_listings
  WHERE  id = :listing_id AND city = :city AND snapshot_month = :month
  ```
- Display fields in two columns.
- Show "Why overpriced" explanation: "This listing is priced at ${price} vs neighbourhood avg ${avg} for {room_type}, which is {delta_pct}% above average."

---

##### Tab 3: Top 10 Underpriced

Identical structure to Tab 2 but queries `vw_rep_monthly_top10_underpriced` and orders by `price_delta ASC`.

"Why underpriced" explanation: "This listing is priced at ${price} vs neighbourhood avg ${avg} for {room_type}, which is {delta_pct}% below average."

---

##### Tab 4: Monthly Data Compliance

**Data source:** `vw_rep_monthly_data_compliance`

**Query:**
```sql
SELECT month_start_date, city_name, rows_count,
       compliance_data_count, missing_price_count,
       missing_neighbourhood_cleansed_count, missing_room_type_count
FROM   vw_rep_monthly_data_compliance
WHERE  city_name = :city
ORDER BY month_start_date DESC
```

**KPI Cards** (top row, using `st.metric`):
- **Compliance Rate**: `compliance_data_count / rows_count * 100` (for selected month)
- **Total Rows**: `rows_count`
- **Missing Price**: `missing_price_count`
- **Missing Neighbourhood**: `missing_neighbourhood_cleansed_count`
- **Missing Room Type**: `missing_room_type_count`

**Also query geo count** from `pipeline_error_log`:
```sql
SELECT CAST(error_details AS TEXT) as details
FROM   pipeline_error_log
WHERE  error_type = 'GEO_OUT_OF_CITY'
  AND  run_id = (SELECT MAX(run_id) FROM pipeline_execution_log WHERE city = :city AND snapshot_month = :month)
```

- **Geo Out-of-City**: Parse count from error_details string.

**Trend chart** (if multiple months exist): Plotly `px.line` showing compliance_rate over time (month_start_date on x-axis).

---

### Page 4 — Data Dictionary (`pages/4_Data_Dictionary.py`)

**Purpose:** Explain every column, layer, type, and validation rule.

#### Implementation

Define the dictionary as a list of dicts in `dashboard/data_dictionary.py`:

```python
DATA_DICTIONARY = [
    {
        "column_name": "id",
        "description": "Unique Airbnb listing identifier",
        "layers": "raw, stg, dim_listing (as listing_id)",
        "data_type": "INTEGER",
        "validation": "NOT NULL, dedupe key in staging",
        "notes": "Natural key from Inside Airbnb"
    },
    {
        "column_name": "price",
        "description": "Nightly listing price as displayed",
        "layers": "raw (text), stg (as price_amount REAL)",
        "data_type": "TEXT → REAL",
        "validation": "Parsed from '$1,234.00' format. Must be > 0. Null/zero excluded.",
        "notes": "Base price only, no taxes/fees"
    },
    # ... all columns
]
```

#### Content to include

**Section 1: Column Dictionary Table**

Render as `st.dataframe` with search/filter. Columns:
- column_name
- description
- layers (raw / stg / dim / fct / view)
- data_type
- validation_rules
- notes

Cover all key columns from:
- `stg_listings` (45 columns — the main working set)
- `dim_*` tables (key columns)
- `fct_*` tables (measures)
- `vw_rep_*` views (output columns)

**Section 2: Data Quality Rules**

Display as a styled table or markdown:

| Rule | Implementation | Outcome |
|------|---------------|---------|
| Price > 0 | `_clean()` in `ingestion.py` filters null/zero prices | Excluded from staging; count logged |
| Availability 0–365 | `validate_data_quality()` in `validation.py` | Warning logged, row kept |
| Duplicate dedup | `_deduplicate()` by `(city, snapshot_month, id)`, keep latest `last_scraped` | Duplicates removed in staging |
| Geo bounding box | `geo_flag_out_of_city()` in `validation.py` using `CITY_BOUNDARIES` | Flagged in staging, excluded from facts |
| Required columns | `validate_schema()` checks `id, name, neighbourhood_cleansed, price, last_scraped` | Pipeline fails fast if missing |
| Critical NOT NULL | `id` and `last_scraped` must not be null | ValidationError raised |
| SCD2 history | Hosts and listings tracked in `dim_host`, `dim_listing` | New version on attribute change |
| Compliance tracking | `fct_data_compliance_monthly` counts missing price/neighbourhood/room_type | Monthly quality snapshot per city |

**Section 3: City Geo Boundaries**

Show the `CITY_BOUNDARIES` dict from `src/validation.py` as a table:

| City | Min Lat | Max Lat | Min Long | Max Long |
|------|---------|---------|----------|----------|
| new-york | 40.49 | 40.92 | -74.26 | -73.70 |
| chicago | 41.64 | 42.03 | -87.94 | -87.52 |
| los-angeles | 33.70 | 34.34 | -118.67 | -118.15 |
| san-francisco | 37.70 | 37.84 | -122.52 | -122.35 |
| new-orleans | 29.85 | 30.10 | -90.20 | -89.90 |

**Section 4: Table Lineage**

Simple text or diagram showing the layer flow:

```
listings.csv.gz
  → raw_listings (append-only, immutable)
    → stg_listings (cleansed, deduped, geo-flagged)
      → dim_date, dim_city, dim_neighbourhood, dim_host (SCD2), dim_listing (SCD2)
      → fct_listing_monthly_snapshot (base fact, geo-flagged excluded)
        → fct_neighbourhood_monthly_avg_price (per room_type + ALL)
        → fct_neighbourhood_monthly_top10_price_delta (OVERPRICED / UNDERPRICED)
        → fct_data_compliance_monthly
          → vw_rep_monthly_neighbourhood_avg_price
          → vw_rep_monthly_top10_overpriced
          → vw_rep_monthly_top10_underpriced
          → vw_rep_monthly_data_compliance
```

---

## Shared Modules

### `dashboard/db.py` — Database Connection Helper

```python
import sqlite3
import pandas as pd
import streamlit as st

DB_PATH = "ModularSnapshotETL.db"

@st.cache_resource
def get_connection() -> sqlite3.Connection:
    """Return a shared SQLite connection (cached across reruns)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def query_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame."""
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params or {})

def db_exists() -> bool:
    """Check if the database file exists and has fact data."""
    import os
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = get_connection()
        count = pd.read_sql("SELECT COUNT(*) as n FROM fct_listing_monthly_snapshot", conn).iloc[0]["n"]
        return count > 0
    except Exception:
        return False
```

### `dashboard/filters.py` — Sidebar Filters

```python
import streamlit as st
from dashboard.db import query_df

def render_filters() -> dict:
    """Render sidebar filters and return selected values."""
    st.sidebar.header("Filters")

    cities = query_df("SELECT DISTINCT city_name FROM dim_city WHERE is_active = 1 ORDER BY city_name")
    city = st.sidebar.selectbox("City", cities["city_name"].tolist())

    months = query_df("""
        SELECT DISTINCT d.month_start_date
        FROM dim_date d
        JOIN fct_listing_monthly_snapshot f ON d.month_key = f.month_key
        JOIN dim_city c ON f.city_key = c.city_key
        WHERE c.city_name = :city
        ORDER BY d.month_start_date DESC
    """, {"city": city})
    month = st.sidebar.selectbox("Snapshot Month", months["month_start_date"].tolist())

    room_types = ["ALL", "Entire home/apt", "Private room", "Shared room", "Hotel room"]
    room_type = st.sidebar.selectbox("Room Type", room_types)

    neighbourhoods = query_df("""
        SELECT DISTINCT neighbourhood
        FROM vw_rep_monthly_neighbourhood_avg_price
        WHERE city_name = :city AND month_start_date = :month
        ORDER BY neighbourhood
    """, {"city": city, "month": month})
    selected_neighbourhoods = st.sidebar.multiselect(
        "Neighbourhoods (optional)",
        neighbourhoods["neighbourhood"].tolist()
    )

    return {
        "city": city,
        "month": month,
        "room_type": room_type,
        "neighbourhoods": selected_neighbourhoods,
    }
```

### `dashboard/charts.py` — Plotly Chart Builders

One function per chart type. Each takes a DataFrame and returns a Plotly figure.

```python
import plotly.express as px

def neighbourhood_avg_price_bar(df):
    """Horizontal bar chart of avg price by neighbourhood."""
    fig = px.bar(
        df.sort_values("avg_price", ascending=True),
        x="avg_price", y="neighbourhood",
        color="room_type",
        orientation="h",
        hover_data=["listing_count", "currency_code"],
        labels={"avg_price": "Avg Price ($)", "neighbourhood": "Neighbourhood"},
        title="Average Nightly Price by Neighbourhood"
    )
    fig.update_layout(yaxis=dict(dtick=1), height=max(400, len(df) * 25))
    return fig

def top10_price_comparison_bar(df, delta_type):
    """Grouped bar: listing price vs neighbourhood avg."""
    title = f"Top 10 {'Overpriced' if delta_type == 'OVERPRICED' else 'Underpriced'} Listings"
    fig = px.bar(
        df,
        x="listing_id",
        y=["price_amount", "neighbourhood_avg_price"],
        barmode="group",
        hover_data=["neighbourhood", "room_type", "price_delta_pct"],
        title=title,
        labels={"value": "Price ($)", "listing_id": "Listing ID"}
    )
    return fig

def compliance_trend_line(df):
    """Line chart of compliance rate over time."""
    df = df.copy()
    df["compliance_rate"] = (df["compliance_data_count"] / df["rows_count"] * 100).round(1)
    fig = px.line(
        df,
        x="month_start_date", y="compliance_rate",
        markers=True,
        title="Data Compliance Rate Over Time",
        labels={"month_start_date": "Month", "compliance_rate": "Compliance Rate (%)"}
    )
    fig.update_layout(yaxis_range=[0, 105])
    return fig
```

---

## `app.py` — Entry Point

```python
import streamlit as st

st.set_page_config(
    page_title="ModularSnapshotETL",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
```

Streamlit's multi-page app feature auto-discovers pages in the `pages/` directory. `app.py` serves as the config entry point and redirects to Page 1.

**Run command:**
```bash
streamlit run app.py
```

---

## Inside Airbnb City List (for "Pick from Inside Airbnb")

In `dashboard/constants.py`, define a curated list of cities and their listing URLs from `https://insideairbnb.com/get-the-data/`:

```python
INSIDE_AIRBNB_CITIES = [
    {"city": "new-york", "label": "New York, United States", "url": "http://data.insideairbnb.com/united-states/ny/new-york-city/{date}/data/listings.csv.gz"},
    {"city": "chicago", "label": "Chicago, United States", "url": "http://data.insideairbnb.com/united-states/il/chicago/{date}/data/listings.csv.gz"},
    {"city": "los-angeles", "label": "Los Angeles, United States", "url": "http://data.insideairbnb.com/united-states/ca/los-angeles/{date}/data/listings.csv.gz"},
    {"city": "san-francisco", "label": "San Francisco, United States", "url": "http://data.insideairbnb.com/united-states/ca/san-francisco/{date}/data/listings.csv.gz"},
    {"city": "new-orleans", "label": "New Orleans, United States", "url": "http://data.insideairbnb.com/united-states/la/new-orleans/{date}/data/listings.csv.gz"},
    # ... extend as needed
]
```

> **Note:** Inside Airbnb URLs change with each scrape date. The app should either hard-code the latest known URLs (updated periodically) or scrape the get-the-data page to find current links. For the first version, hard-code a few known working URLs and let the user paste custom URLs as a fallback.

---

## Interview-Impressive Features

### 1. Run History Panel (Page 2)

- Query `pipeline_execution_log` and display as an interactive table.
- Click a row → expand to show `row_counts` JSON (parsed), `pipeline_error_log` entries, archived file path.
- Shows real pipeline provenance and auditability.

### 2. "What Am I Seeing?" Tooltips (Page 3)

On each dashboard tab, include an `st.info()` block:

- **Avg Price tab**: "Average nightly price per neighbourhood, calculated from `fct_neighbourhood_monthly_avg_price`. Geo-flagged listings (coordinates outside city bounds) are excluded. Room type 'ALL' combines all room types."
- **Overpriced tab**: "Top 10 listings priced highest above their neighbourhood average. Delta = listing price − neighbourhood avg. Compared per room type and against the combined 'ALL' average."
- **Underpriced tab**: "Top 10 listings priced lowest below their neighbourhood average."
- **Compliance tab**: "Monthly data quality snapshot. A row is compliant when price, neighbourhood, and room_type are all present. Geo-flagged rows are excluded."

### 3. Listing Detail Expander (Page 3)

When a user selects a row in Top 10 tables, show:
- Key listing fields from `stg_listings`
- Geo flag status
- "Why it's overpriced/underpriced" calculation breakdown

### 4. Schema Drift Visibility (Page 2)

After a run, query `pipeline_error_log` for error_type containing "UNKNOWN_COLUMN":
```sql
SELECT error_details FROM pipeline_error_log
WHERE run_id = :run_id AND error_type LIKE '%UNKNOWN%'
```
If results exist, show a yellow badge: "Schema changes detected" with the column list.

### 5. Reconciliation Summary (Page 3)

Add a collapsible `st.expander("Reconciliation Results")` at the bottom of the Dashboard page:
- Query all three `rec_*` tables for the selected city/month.
- Show match/mismatch counts.
- If any MISMATCH exists, show a warning badge.

---

## Non-Functional Requirements

| Requirement | Implementation |
|-------------|---------------|
| No changes to `src/` | Dashboard is read-only against existing modules. Pipeline runs via function call. |
| No changes to `tests/` | Existing 70 tests remain untouched and passing. |
| Fail fast on missing columns | Pre-flight validation on Page 2 checks required columns before running pipeline. |
| Clear error messages | Use `st.error()` with descriptive text. Never show raw tracebacks. |
| Scoped deletes only | Pipeline already scopes deletes to `(city, snapshot_month)`. No change needed. |
| Run logging | Every run logged to `pipeline_execution_log` with file paths. Visible in Run History panel. |
| Minimal UI | Clean Streamlit defaults. No custom CSS unless strictly needed. Wide layout. |
| Demo-ready | One command: `streamlit run app.py`. Works with or without pre-loaded data. |

---

## Dependency Changes

Add to `requirements.txt`:

```
streamlit>=1.30.0
plotly>=5.18.0
```

---

## Implementation Order

| Step | Task | Estimated Scope |
|------|------|-----------------|
| 1 | Create `dashboard/` package: `db.py`, `constants.py`, `__init__.py` | Small |
| 2 | Create `app.py` with page config | Small |
| 3 | Build **Page 1 — Home** | Medium |
| 4 | Build `dashboard/pipeline_runner.py` | Medium |
| 5 | Build **Page 2 — Data Load** (upload + URL + pipeline run + status) | Large |
| 6 | Build `dashboard/filters.py` | Small |
| 7 | Build `dashboard/charts.py` | Medium |
| 8 | Build **Page 3 — Dashboard** (4 tabs with charts + tables + detail) | Large |
| 9 | Build `dashboard/data_dictionary.py` definitions | Medium |
| 10 | Build **Page 4 — Data Dictionary** | Medium |
| 11 | Add Run History panel to Page 2 | Small |
| 12 | Add Reconciliation summary to Page 3 | Small |
| 13 | Update `requirements.txt` and `.gitignore` | Small |
| 14 | End-to-end testing: load a city, check all 4 dashboard tabs | Manual QA |

---

## Running the App

```bash
# Install dependencies
pip install -r requirements.txt

# Option 1: Load data via CLI first, then view dashboard
python main.py
streamlit run app.py

# Option 2: Load data through the web UI directly
streamlit run app.py
# → Navigate to "Load Data" page → Upload or paste URL → Run pipeline → View dashboard
```
