"""Wrapper to run the ETL pipeline with Streamlit progress feedback."""

import os
import sys
import sqlite3

import streamlit as st

# Ensure project root is on the path so src/ imports work
_project_root = os.path.dirname(os.path.dirname(__file__))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.schema import create_all
from src.pipeline import run as pipeline_run
from dashboard.constants import DB_PATH


def run_with_progress(city: str, dataset_path: str) -> dict:
    """Run the full pipeline for a city, showing Streamlit status updates.

    Returns the row_counts dict on success, or raises on failure.
    """
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    try:
        create_all(conn)
        with st.status("Running pipeline...", expanded=True) as status:
            st.write("Loading raw data...")
            st.write("Transforming staging...")
            st.write("Updating dimensions...")
            st.write("Building fact tables...")
            st.write("Running reconciliation...")

            row_counts = pipeline_run(
                conn, dataset_path, city,
                triggered_by="dashboard",
            )

            status.update(label="Pipeline complete!", state="complete")

        return row_counts

    except Exception:
        raise

    finally:
        conn.close()
        # Clear cached DB connection so dashboard reads fresh data
        from dashboard.db import get_connection
        get_connection.clear()
