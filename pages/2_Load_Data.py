"""Page 2 — Data Load + Run History."""

from __future__ import annotations

import json
import os
import sys

import pandas as pd
import streamlit as st

# Ensure project root is importable
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dashboard.constants import DATASET_DIR, LISTING_FILENAME, DB_PATH, CITY_OPTIONS
from dashboard.pipeline_runner import run_with_progress
from src import db, etl_logging
from src.schema import create_all
from src.validation import REQUIRED_COLUMNS


def _log_user_error(city_slug: str, message: str) -> None:
    """Record a pre-pipeline user-facing failure (bad city, download, schema)
    in pipeline_execution_log/pipeline_error_log, so it shows up in Run History alongside
    pipeline runs."""
    conn = db.get_connection(DB_PATH)
    create_all(conn)
    run_id = etl_logging.start_run(conn, city=city_slug, triggered_by="dashboard")
    etl_logging.log_error(conn, run_id, "USER_ERROR", message)
    etl_logging.finish_run(conn, run_id, "FAILED", error_message=message)
    conn.close()


@st.cache_data(ttl=3600, show_spinner=False)
def _load_live_data() -> tuple[list[tuple[str, str]], dict]:
    """Fetch city list from Inside Airbnb; fall back to built-in catalog.

    Returns (city_options, snapshot_map) where snapshot_map maps slug ->
    CitySnapshot (only populated when live scraping succeeds).
    """
    try:
        from src.data_fetcher import _scrape_live_page, CITY_CATALOG, get_latest_per_city
        snapshots = _scrape_live_page()
        if snapshots:
            catalog_labels = {slug: label for slug, _c, _r, _ci, label in CITY_CATALOG}
            latest = get_latest_per_city(snapshots)
            options: dict[str, str] = {}
            for slug, snap in latest.items():
                label = catalog_labels.get(slug, snap.city_label)
                options[slug] = label
            sorted_options = sorted(options.items(), key=lambda x: x[1])
            return sorted_options, latest
    except Exception:
        pass
    return CITY_OPTIONS, {}


# ---------------------------------------------------------------------------
# Page Header — Data Source Explanation
# ---------------------------------------------------------------------------
st.title("Load Data")

st.markdown("""
**Data Source**

This platform uses publicly available Airbnb listing data from
[insideairbnb.com/get-the-data](https://insideairbnb.com/get-the-data/).

Select a city below to ingest the latest listings dataset into the
ModularSnapshotETL pipeline. Once processed, the dashboard will display:

- **Top 10 overpriced listings** per neighbourhood
- **Top 10 underpriced listings** per neighbourhood
- **Average neighbourhood price comparisons** by room type
- **Monthly data quality and compliance metrics**
""")

st.divider()

# ---------------------------------------------------------------------------
# City Selection — Searchable Dropdown
# ---------------------------------------------------------------------------
st.header("Select City")

_city_options, _live_snapshots = _load_live_data()
city_labels = [label for _slug, label in _city_options]
slug_by_label = {label: slug for slug, label in _city_options}

selected_label = st.selectbox(
    "Search and select a city",
    options=city_labels,
    index=None,
    placeholder="Type to search (e.g. Istanbul, Paris, New York)...",
)

city_slug = slug_by_label.get(selected_label, "") if selected_label else ""

if selected_label:
    st.caption(f"City slug: `{city_slug}` — will download from data.insideairbnb.com")

st.divider()

# ---------------------------------------------------------------------------
# Run Pipeline
# ---------------------------------------------------------------------------
st.header("Run Pipeline")

run_disabled = not city_slug
if st.button("Ingest & Run Pipeline", disabled=run_disabled, type="primary"):
    # Defer heavy imports to button-click time
    from src.data_fetcher import fetch_city_data, probe_catalog_city

    city_dir = os.path.join(DATASET_DIR, city_slug)
    os.makedirs(city_dir, exist_ok=True)
    target_path = os.path.join(city_dir, LISTING_FILENAME)

    # --- Probe & Download ---
    try:
        with st.status("Fetching data from Inside Airbnb...", expanded=True) as status:
            st.write(f"Probing latest snapshot for **{selected_label}**...")
            # Use pre-fetched snapshot from live scrape if available
            snapshot = _live_snapshots.get(city_slug)
            if snapshot is None:
                snapshot = probe_catalog_city(city_slug)
            if snapshot is None:
                msg = (
                    f"No recent snapshot found for {selected_label}. "
                    "This city may be temporarily unavailable on Inside Airbnb."
                )
                st.error(msg)
                _log_user_error(city_slug, msg)
                st.stop()

            st.write(f"Found snapshot: **{snapshot.snapshot_date}**")
            st.write("Downloading `listings.csv.gz` ...")
            st.write("Downloading `neighbourhoods.csv` ...")
            st.write("Downloading `neighbourhoods.geojson` ...")

            downloaded = fetch_city_data(
                city_slug,
                snapshot=snapshot,
                dest_dir=DATASET_DIR,
            )
            status.update(
                label=f"Downloaded {len(downloaded)} files ({snapshot.snapshot_date})",
                state="complete",
            )

        for ftype, fpath in downloaded.items():
            st.success(f"`{ftype}` -> `{fpath}`")

        if "listings" in downloaded:
            target_path = downloaded["listings"]
        else:
            msg = "listings.csv.gz was not downloaded."
            st.error(msg)
            _log_user_error(city_slug, msg)
            st.stop()

    except Exception as e:
        msg = f"Failed to download data: {e}"
        st.error(msg)
        _log_user_error(city_slug, msg)
        st.stop()

    # --- Pre-flight validation ---
    try:
        preview = pd.read_csv(target_path, nrows=5, compression="gzip")
        missing = set(REQUIRED_COLUMNS) - set(preview.columns)
        if missing:
            msg = f"Missing required columns: {missing}"
            st.error(msg)
            _log_user_error(city_slug, msg)
            st.stop()
        st.success(f"Schema check passed. Columns found: {len(preview.columns)}")
    except Exception as e:
        msg = f"Cannot read file: {e}"
        st.error(msg)
        _log_user_error(city_slug, msg)
        st.stop()

    # --- Run pipeline ---
    try:
        row_counts = run_with_progress(city_slug, target_path)

        st.success("Pipeline completed successfully!")

        if "visitor_session_uuid" in st.session_state:
            try:
                from src import visitor_log
                visitor_log.mark_ran_pipeline(db.get_connection(DB_PATH), st.session_state.visitor_session_uuid)
            except Exception:
                pass

        # --- Run Status Card ---
        st.subheader("Run Results")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Raw Rows", f"{row_counts.get('raw_listings', 0):,}")
        m2.metric("Staging Rows", f"{row_counts.get('stg_listings', 0):,}")
        m3.metric("Fact Rows", f"{row_counts.get('fct_listing_monthly_snapshot', 0):,}")
        m4.metric("Avg Price Rows", f"{row_counts.get('fct_neighbourhood_monthly_avg_price', 0):,}")

        m5, m6, m7, m8 = st.columns(4)
        m5.metric("Invalid Price Excluded", f"{row_counts.get('invalid_price_excluded', 0):,}")
        m6.metric("Geo Flagged", f"{row_counts.get('geo_out_of_city_count', 0):,}")
        m7.metric("Top 10 Delta Rows", f"{row_counts.get('fct_neighbourhood_monthly_top10_price_delta', 0):,}")
        m8.metric("Compliance Rows", f"{row_counts.get('fct_data_compliance_monthly', 0):,}")

        st.json(row_counts)

        if st.button("View Dashboard"):
            st.switch_page("pages/3_Dashboard.py")

    except Exception as e:
        st.error(f"Pipeline failed: {e}")

st.divider()

# ---------------------------------------------------------------------------
# Run History
# ---------------------------------------------------------------------------
st.header("Run History")

try:
    conn = db.get_connection(DB_PATH)
    create_all(conn)

    runs = db.read_sql(
        conn,
        """
        SELECT run_id, start_time, end_time, status, city, snapshot_month,
               source_file_name, archived_file_path, row_counts,
               error_message, triggered_by
        FROM pipeline_execution_log
        ORDER BY start_time DESC
        LIMIT 20
        """,
    )
    if runs.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        # Show summary table with key columns
        display_cols = ["run_id", "start_time", "status", "city", "snapshot_month", "triggered_by"]
        st.dataframe(
            runs[[c for c in display_cols if c in runs.columns]],
            use_container_width=True,
            hide_index=True,
        )

        selected_run = st.selectbox(
            "Select a run to view details",
            runs["run_id"].tolist(),
            format_func=lambda rid: f"Run #{rid} — {runs.loc[runs['run_id'] == rid, 'city'].values[0]} ({runs.loc[runs['run_id'] == rid, 'status'].values[0]})",
        )

        if selected_run:
            run_row = runs[runs["run_id"] == selected_run].iloc[0]
            with st.expander("Run Details", expanded=True):
                st.markdown(f"**Status:** {run_row['status']}")
                st.markdown(f"**City:** {run_row['city']}")
                st.markdown(f"**Snapshot Month:** {run_row['snapshot_month']}")
                st.markdown(f"**Start:** {run_row['start_time']}")
                st.markdown(f"**End:** {run_row['end_time']}")

                triggered = run_row.get("triggered_by", "")
                st.markdown(f"**Triggered By:** `{triggered or 'cli'}`")

                if run_row["archived_file_path"]:
                    st.markdown(f"**Archived File:** `{run_row['archived_file_path']}`")

                if run_row["row_counts"]:
                    try:
                        rc = json.loads(run_row["row_counts"])
                        st.markdown("**Row Counts:**")
                        st.json(rc)
                    except (json.JSONDecodeError, TypeError):
                        st.text(run_row["row_counts"])

                if run_row["error_message"]:
                    st.error(f"Error: {run_row['error_message']}")

                # Show error log entries
                errors = db.read_sql(
                    conn,
                    "SELECT table_name, error_type, error_details, timestamp "
                    "FROM pipeline_error_log WHERE run_id = ? ORDER BY timestamp",
                    (int(selected_run),),
                )
                if not errors.empty:
                    st.markdown("**Error / Warning Log:**")
                    st.dataframe(errors, use_container_width=True, hide_index=True)

    conn.close()
except Exception as e:
    st.warning(f"Could not load run history: {e}")
