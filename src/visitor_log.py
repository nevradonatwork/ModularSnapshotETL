"""
Visitor session logging for the ModularSnapshotETL Streamlit dashboard.

Tracks every visit (not just pipeline runs) -- when, IP, city, country, and
whether that session ran the ETL pipeline. Kept separate from
etl_logging.py, which is a pipeline-run concern used by the Streamlit-
agnostic CLI path (main.py) too; this module is a Streamlit-session
concern and imports streamlit directly.

IP geolocation was previously tried in this repo and removed as
unreliable (Streamlit Cloud's proxy layer doesn't always expose a clean
client IP). Re-added at the user's request, on the same basis: it's a
best-effort enrichment, not a source of truth, and failures are silent.
"""
import ipaddress
import json
import logging
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_GEOLOCATE_TIMEOUT = 2


def _get_client_metadata() -> dict:
    """Extract client IP and user-agent from Streamlit's request headers."""
    ip = None
    user_agent = None
    try:
        import streamlit as st

        headers = st.context.headers
        ip = (
            headers.get("X-Forwarded-For", "").split(",")[0].strip()
            or headers.get("X-Real-Ip", "")
            or headers.get("Remote-Addr", "")
        )
        user_agent = headers.get("User-Agent", "")
    except Exception:
        pass
    return {"ip": ip or None, "user_agent": user_agent or None}


def _geolocate_ip(ip: str | None) -> tuple[str, str]:
    """Resolve an IP to (city, country) using ip-api.com (free, no key, 45 req/min).

    Best-effort: private/loopback IPs and any failure return ("", "").
    """
    if not ip:
        return "", ""
    try:
        if ipaddress.ip_address(ip).is_private:
            return "", ""
    except ValueError:
        return "", ""
    try:
        url = f"http://ip-api.com/json/{ip}?fields=status,city,country"
        req = urllib.request.Request(url, headers={"User-Agent": "ModularSnapshotETL/1.0"})
        with urllib.request.urlopen(req, timeout=_GEOLOCATE_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if data.get("status") == "success":
                return data.get("city", ""), data.get("country", "")
    except Exception as e:
        logger.debug("IP geolocation failed for %s: %s", ip, e)
    return "", ""


def start_visit(conn) -> str:
    """Insert a new visitor_log row for a brand-new browser session.

    Returns the generated session_uuid (store it in st.session_state so
    later calls in the same session can touch/update this same row).
    """
    session_uuid = str(uuid.uuid4())
    meta = _get_client_metadata()
    city, country = _geolocate_ip(meta["ip"])
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """INSERT INTO visitor_log
           (session_uuid, first_seen_at, last_seen_at, ip_address, user_agent,
            city, country, ran_pipeline, page_views)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1)""",
        (session_uuid, now, now, meta["ip"], meta["user_agent"], city or None, country or None),
    )
    conn.commit()
    return session_uuid


def touch_visit(conn, session_uuid: str) -> None:
    """Update last_seen_at and increment page_views for an existing session."""
    conn.execute(
        """UPDATE visitor_log
           SET last_seen_at = ?, page_views = page_views + 1
           WHERE session_uuid = ?""",
        (datetime.now(timezone.utc).isoformat(), session_uuid),
    )
    conn.commit()


def mark_ran_pipeline(conn, session_uuid: str) -> None:
    """Flag a session as having run the ETL pipeline at least once."""
    conn.execute(
        "UPDATE visitor_log SET ran_pipeline = 1 WHERE session_uuid = ?",
        (session_uuid,),
    )
    conn.commit()
