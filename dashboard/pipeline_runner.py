"""Wrapper to run the ETL pipeline with Streamlit progress feedback."""

import json
import logging
import os
import sys
import sqlite3
import urllib.request
import urllib.error

import streamlit as st

# Ensure project root is on the path so src/ imports work
_project_root = os.path.dirname(os.path.dirname(__file__))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.schema import create_all
from src.pipeline import run as pipeline_run
from dashboard.constants import DB_PATH

logger = logging.getLogger(__name__)


def _is_private_ip(ip: str) -> bool:
    """Return True if the IP is a private/local address."""
    if not ip:
        return True
    return (
        ip.startswith(("127.", "10.", "192.168.", "0."))
        or ip.startswith("172.") and 16 <= int(ip.split(".")[1]) <= 31
        or ip in ("::1", "localhost")
    )


def _geolocate_ip(ip: str | None) -> dict:
    """Resolve an IP address to city/country using ip2location.io.

    If the IP is private/local or missing, calls the API without an ip
    parameter so it auto-detects the server's public IP.
    Free tier allows 1,000 queries/day without an API key.
    """
    try:
        if ip and not _is_private_ip(ip):
            url = f"https://api.ip2location.io/?ip={ip}&format=json"
        else:
            # No usable client IP — let the API auto-detect
            url = "https://api.ip2location.io/?format=json"
        req = urllib.request.Request(url, headers={"User-Agent": "ModularSnapshotETL/1.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return {
                "ip": data.get("ip", ""),
                "city": data.get("city_name", ""),
                "country": data.get("country_name", ""),
            }
    except Exception as e:
        logger.debug("IP geolocation failed for %s: %s", ip, e)
    return {"ip": "", "city": "", "country": ""}


def _get_client_metadata() -> dict:
    """Extract client IP and geolocation from Streamlit headers."""
    ip = None
    user_agent = None
    try:
        headers = st.context.headers
        # Streamlit Cloud sets X-Forwarded-For for the real client IP
        ip = (
            headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or headers.get("X-Real-Ip", "")
            or headers.get("Remote-Addr", "")
        )
        user_agent = headers.get("User-Agent", "")
    except Exception:
        pass

    ip = ip or None
    # Always call geolocation — if IP is local/missing, API auto-detects
    geo = _geolocate_ip(ip)

    return {
        "client_ip": geo.get("ip") or ip or None,
        "client_user_agent": user_agent or None,
        "client_city": geo["city"] or None,
        "client_country": geo["country"] or None,
    }


def run_with_progress(city: str, dataset_path: str) -> dict:
    """Run the full pipeline for a city, showing Streamlit status updates.

    Returns the row_counts dict on success, or raises on failure.
    """
    # Capture client metadata before the pipeline runs
    meta = _get_client_metadata()

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
                client_ip=meta["client_ip"],
                client_user_agent=meta["client_user_agent"],
                client_city=meta["client_city"],
                client_country=meta["client_country"],
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
