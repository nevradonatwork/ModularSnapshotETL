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
from src.validation import REQUIRED_COLUMNS

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

city_labels = [label for _slug, label in CITY_OPTIONS]
slug_by_label = {label: slug for slug, label in CITY_OPTIONS}

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
            snapshot = probe_catalog_city(city_slug)
            if snapshot is None:
                st.error(
                    f"No recent snapshot found for **{selected_label}**. "
                    "This city may be temporarily unavailable on Inside Airbnb."
                )
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
            st.error("listings.csv.gz was not downloaded.")
            st.stop()

    except Exception as e:
        st.error(f"Failed to download data: {e}")
        st.stop()

    # --- Pre-flight validation ---
    try:
        preview = pd.read_csv(target_path, nrows=5, compression="gzip")
        missing = set(REQUIRED_COLUMNS) - set(preview.columns)
        if missing:
            st.error(f"Missing required columns: {missing}")
            st.stop()
        st.success(f"Schema check passed. Columns found: {len(preview.columns)}")
    except Exception as e:
        st.error(f"Cannot read file: {e}")
        st.stop()

    # --- Run pipeline ---
    try:
        row_counts = run_with_progress(city_slug, target_path)

        st.success("Pipeline completed successfully!")

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

if os.path.exists(DB_PATH):
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)

        # Check which columns exist (handles DB created before client metadata was added)
        cursor = conn.execute("PRAGMA table_info(etl_run_log)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        has_client_cols = "client_ip" in existing_cols

        has_geo_cols = "client_city" in existing_cols
        extra_cols = ""
        if has_client_cols:
            extra_cols += ", triggered_by, client_ip, client_user_agent"
        if has_geo_cols:
            extra_cols += ", client_city, client_country"
        runs = pd.read_sql(
            f"""
            SELECT run_id, start_time, end_time, status, city, snapshot_month,
                   source_file_name, archived_file_path, row_counts,
                   error_message{extra_cols}
            FROM etl_run_log
            ORDER BY start_time DESC
            LIMIT 20
            """,
            conn,
        )
        if runs.empty:
            st.info("No pipeline runs recorded yet.")
        else:
            # Build a "location" column for the summary table
            if has_geo_cols and "client_city" in runs.columns:
                runs["client_location"] = runs.apply(
                    lambda r: ", ".join(filter(None, [r.get("client_city", ""), r.get("client_country", "")])) or "N/A",
                    axis=1,
                )

            # Show summary table with key columns
            display_cols = ["run_id", "start_time", "status", "city", "snapshot_month"]
            if has_client_cols:
                display_cols += ["triggered_by", "client_ip"]
            if has_geo_cols and "client_location" in runs.columns:
                display_cols += ["client_location"]
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

                    # Client metadata section
                    if has_client_cols:
                        st.markdown("---")
                        st.markdown("**Triggered By**")
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            triggered = run_row.get("triggered_by", "")
                            st.markdown(f"**Source:** `{triggered or 'cli'}`")
                            ip = run_row.get("client_ip", "")
                            st.markdown(f"**Client IP:** `{ip or 'N/A'}`")
                        with c2:
                            # Location from IP geolocation
                            geo_city = run_row.get("client_city", "") if has_geo_cols else ""
                            geo_country = run_row.get("client_country", "") if has_geo_cols else ""
                            location = ", ".join(filter(None, [geo_city, geo_country])) or "N/A"
                            st.markdown(f"**Location:** {location}")
                        with c3:
                            ua = run_row.get("client_user_agent", "")
                            if ua:
                                # Parse browser name from User-Agent
                                browser = "Unknown"
                                ua_lower = ua.lower()
                                if "edg/" in ua_lower:
                                    browser = "Edge"
                                elif "chrome/" in ua_lower and "safari/" in ua_lower:
                                    browser = "Chrome"
                                elif "firefox/" in ua_lower:
                                    browser = "Firefox"
                                elif "safari/" in ua_lower:
                                    browser = "Safari"
                                st.markdown(f"**Browser:** {browser}")
                                st.caption(f"`{ua[:120]}{'...' if len(ua) > 120 else ''}`")
                            else:
                                st.markdown("**Browser:** N/A")
                        st.markdown("---")

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
                    errors = pd.read_sql(
                        "SELECT table_name, error_type, error_details, timestamp "
                        "FROM etl_error_log WHERE run_id = ? ORDER BY timestamp",
                        conn,
                        params=(int(selected_run),),
                    )
                    if not errors.empty:
                        st.markdown("**Error / Warning Log:**")
                        st.dataframe(errors, use_container_width=True, hide_index=True)

        conn.close()
    except Exception as e:
        st.warning(f"Could not load run history: {e}")
else:
    st.info("No database found. Load data to create one.")
