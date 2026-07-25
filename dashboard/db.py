"""Database connection helpers for the Streamlit dashboard."""

import pandas as pd
import streamlit as st

from dashboard.constants import DB_PATH
from src import db
from src.schema import create_all


@st.cache_resource
def get_connection():
    """Return a shared database connection (cached across reruns).

    Uses Postgres (Neon) when DATABASE_URL is configured, else a local
    SQLite file — see src/db.py. Ensures the schema exists so a fresh/empty
    database doesn't look like a broken connection.
    """
    conn = db.get_connection(DB_PATH)
    create_all(conn)
    return conn


def get_fresh_connection():
    """Return a new (uncached) connection — used after pipeline writes."""
    return db.get_connection(DB_PATH)


def query_df(sql: str, params: tuple | dict | None = None) -> pd.DataFrame:
    """Run a read-only SQL query and return a DataFrame."""
    conn = get_connection()
    return db.read_sql(conn, sql, params)


def db_has_data() -> bool:
    """Check if the database contains any fact rows.

    Raises if the connection/query itself fails, so callers can
    distinguish "genuinely empty" from "can't reach the database" —
    the previous version swallowed all errors and showed the same
    "no data" message for both.
    """
    conn = get_connection()
    row = conn.execute(
        "SELECT COUNT(*) FROM fct_listing_monthly_snapshot"
    ).fetchone()
    return row[0] > 0
