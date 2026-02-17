"""Database connection helpers for the Streamlit dashboard."""

import os
import sqlite3

import pandas as pd
import streamlit as st

from dashboard.constants import DB_PATH


@st.cache_resource
def get_connection() -> sqlite3.Connection:
    """Return a shared SQLite connection (cached across reruns)."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_fresh_connection() -> sqlite3.Connection:
    """Return a new (uncached) SQLite connection — used after pipeline writes."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def query_df(sql: str, params: tuple | dict | None = None) -> pd.DataFrame:
    """Run a read-only SQL query and return a DataFrame."""
    conn = get_connection()
    return pd.read_sql(sql, conn, params=params or ())


def db_has_data() -> bool:
    """Check if the database file exists and contains fact rows."""
    if not os.path.exists(DB_PATH):
        return False
    try:
        conn = get_connection()
        row = conn.execute(
            "SELECT COUNT(*) FROM fct_listing_monthly_snapshot"
        ).fetchone()
        return row[0] > 0
    except Exception:
        return False
