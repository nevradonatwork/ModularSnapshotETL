"""Page 2 — Data Load + Run History."""

import json
import os
import sys
import urllib.request

import pandas as pd
import streamlit as st

# Ensure project root is importable
_project_root = os.path.dirname(os.path.dirname(__file__))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from dashboard.constants import DATASET_DIR, LISTING_FILENAME, INSIDE_AIRBNB_CITIES, DB_PATH
from dashboard.pipeline_runner import run_with_progress
from src.validation import REQUIRED_COLUMNS
from src.data_fetcher import discover_snapshots, get_latest_per_city, fetch_city_data

st.title("Load Data")
st.markdown(
    "Ingest an Inside Airbnb `listings.csv.gz` file from Inside Airbnb, using the dataset available at" \
    " https://insideairbnb.com/get-the-data/ and execute the complete end-to-end ETL pipeline."
)

# ---------------------------------------------------------------------------
# Data Load Form
# ---------------------------------------------------------------------------
st.header("1. Select City & Data Source")

source_method = st.radio(
    "Data source",
    [
        "Auto-fetch from Inside Airbnb",
        "Upload file",
        "Paste URL",
    ],
    horizontal=True,
)

city_name = ""
uploaded_file = None
paste_url = ""
auto_fetch_snapshot = None

if source_method == "Auto-fetch from Inside Airbnb":
    st.markdown(
        "Automatically discovers available cities from "
        "[insideairbnb.com/get-the-data](https://insideairbnb.com/get-the-data/) "
        "and downloads **listings.csv.gz**, **neighbourhoods.csv**, and "
        "**neighbourhoods.geojson** into the correct folder."
    )

    # Discover available cities (cached per session)
    if "airbnb_snapshots" not in st.session_state:
        st.session_state.airbnb_snapshots = None

    if st.button("Discover available cities"):
        with st.spinner("Scanning insideairbnb.com ..."):
            try:
                snapshots = discover_snapshots()
                latest = get_latest_per_city(snapshots)
                st.session_state.airbnb_snapshots = latest
                st.success(f"Found {len(latest)} cities.")
            except Exception as e:
                st.error(f"Could not reach Inside Airbnb: {e}")
                st.caption(
                    "The page may require JavaScript. As a fallback, use "
                    "'Paste URL' and copy the direct link from the website."
                )

    latest = st.session_state.airbnb_snapshots
    if latest:
        city_options = sorted(latest.keys())
        selected_city = st.selectbox(
            "Select city",
            city_options,
            format_func=lambda s: f"{s}  ({latest[s].snapshot_date})",
        )
        if selected_city:
            city_name = selected_city
            auto_fetch_snapshot = latest[selected_city]
            st.info(
                f"**{selected_city}** — snapshot {auto_fetch_snapshot.snapshot_date}\n\n"
                f"Files to download:\n"
                f"- `listings.csv.gz`\n"
                f"- `neighbourhoods.csv`\n"
                f"- `neighbourhoods.geojson`"
            )

elif source_method == "Upload file":
    city_name = st.text_input(
        "City name (required)",
        placeholder="new-york",
        help="Use lowercase slug format: new-york, chicago, los-angeles",
    )
    uploaded_file = st.file_uploader(
        "Upload listings.csv.gz",
        type=["gz"],
        help="Select the listings.csv.gz file downloaded from Inside Airbnb.",
    )

elif source_method == "Paste URL":
    city_name = st.text_input(
        "City name (required)",
        placeholder="new-york",
        help="Use lowercase slug format: new-york, chicago, los-angeles",
    )
    paste_url = st.text_input(
        "URL to listings.csv.gz",
        placeholder="https://data.insideairbnb.com/.../listings.csv.gz",
    )

st.divider()

# ---------------------------------------------------------------------------
# Run Pipeline
# ---------------------------------------------------------------------------
st.header("2. Run Pipeline")

run_disabled = not city_name.strip() if isinstance(city_name, str) else True
if st.button("Ingest & Run Pipeline", disabled=run_disabled, type="primary"):
    city_slug = city_name.strip().lower()
    city_dir = os.path.join(DATASET_DIR, city_slug)
    os.makedirs(city_dir, exist_ok=True)
    target_path = os.path.join(city_dir, LISTING_FILENAME)

    # --- Save / download the file ---
    try:
        if source_method == "Auto-fetch from Inside Airbnb":
            if auto_fetch_snapshot is None:
                st.error("Please discover cities and select one first.")
                st.stop()
            with st.status("Downloading all files...", expanded=True) as status:
                st.write(f"Downloading listings.csv.gz ...")
                st.write(f"Downloading neighbourhoods.csv ...")
                st.write(f"Downloading neighbourhoods.geojson ...")
                downloaded = fetch_city_data(
                    city_slug,
                    snapshot=auto_fetch_snapshot,
                    dest_dir=DATASET_DIR,
                )
                status.update(label=f"Downloaded {len(downloaded)} files", state="complete")
            for ftype, fpath in downloaded.items():
                st.success(f"`{ftype}` -> `{fpath}`")
            # Point pipeline at the listings file
            if "listings" in downloaded:
                target_path = downloaded["listings"]
            else:
                st.error("listings.csv.gz was not downloaded.")
                st.stop()

        elif source_method == "Upload file":
            if uploaded_file is None:
                st.error("Please upload a file first.")
                st.stop()
            with open(target_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.success(f"File saved to `{target_path}`")

        elif source_method == "Paste URL":
            url = paste_url.strip()
            if not url:
                st.error("Please provide a URL.")
                st.stop()
            with st.spinner("Downloading file..."):
                urllib.request.urlretrieve(url, target_path)
            st.success(f"Downloaded to `{target_path}`")

    except Exception as e:
        st.error(f"Failed to save/download file: {e}")
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
st.header("3. Run History")

if os.path.exists(DB_PATH):
    try:
        import sqlite3
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        runs = pd.read_sql(
            """
            SELECT run_id, start_time, end_time, status, city, snapshot_month,
                   source_file_name, archived_file_path, row_counts, error_message
            FROM etl_run_log
            ORDER BY start_time DESC
            LIMIT 20
            """,
            conn,
        )
        if runs.empty:
            st.info("No pipeline runs recorded yet.")
        else:
            st.dataframe(
                runs[["run_id", "start_time", "end_time", "status", "city",
                      "snapshot_month", "source_file_name"]],
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
