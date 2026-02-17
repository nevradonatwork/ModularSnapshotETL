"""
Email notification for BreezeBnb pipeline runs.

Sends a summary email after each pipeline execution with run status,
row counts, and error details from the ETL log tables.

Configuration via environment variables:
    BREEZEBNB_SMTP_HOST     — SMTP server hostname (default: smtp.gmail.com)
    BREEZEBNB_SMTP_PORT     — SMTP server port (default: 587)
    BREEZEBNB_SMTP_USER     — SMTP username / sender email (required)
    BREEZEBNB_SMTP_PASSWORD — SMTP password or app password (required)
    BREEZEBNB_NOTIFY_EMAIL  — Recipient email address (required)
"""
import json
import logging
import os
import smtplib
import sqlite3
from datetime import datetime, timezone
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


def _get_smtp_config() -> dict:
    """Read SMTP configuration from environment variables at call time."""
    return {
        "host": os.environ.get("BREEZEBNB_SMTP_HOST", "smtp.gmail.com"),
        "port": int(os.environ.get("BREEZEBNB_SMTP_PORT", "587")),
        "user": os.environ.get("BREEZEBNB_SMTP_USER", ""),
        "password": os.environ.get("BREEZEBNB_SMTP_PASSWORD", ""),
        "notify_email": os.environ.get("BREEZEBNB_NOTIFY_EMAIL", "nevradonatwork@gmail.com"),
    }


def _get_latest_run(conn: sqlite3.Connection) -> dict | None:
    """Fetch the most recent ETL run log entry."""
    row = conn.execute(
        "SELECT run_id, start_time, end_time, status, row_counts, error_message "
        "FROM etl_run_log ORDER BY run_id DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return {
        "run_id": row[0],
        "start_time": row[1],
        "end_time": row[2],
        "status": row[3],
        "row_counts": row[4],
        "error_message": row[5],
    }


def _get_run_errors(conn: sqlite3.Connection, run_id: int) -> list[dict]:
    """Fetch all error log entries for a given run."""
    rows = conn.execute(
        "SELECT table_name, error_type, error_details, timestamp "
        "FROM etl_error_log WHERE run_id = ? ORDER BY id",
        (run_id,),
    ).fetchall()
    return [
        {
            "table_name": r[0],
            "error_type": r[1],
            "error_details": r[2],
            "timestamp": r[3],
        }
        for r in rows
    ]


def _get_reconciliation_summary(conn: sqlite3.Connection) -> dict:
    """Query rec tables for the most recent reconciliation results.

    Returns dict with counts per table:
        {
            "avg_price": {"total": N, "match": N, "mismatch": N, "stg_only": N, "fct_only": N},
            "top10_delta": {"total": N, "match": N, "mismatch": N, "stg_only": N, "fct_only": N},
            "views": {"total": N, "match": N, "mismatch": N},
            "mismatches": [list of human-readable mismatch details],
        }
    """
    summary: dict = {"mismatches": []}

    # Average price reconciliation
    avg_rows = conn.execute(
        "SELECT match_status, COUNT(*) FROM rec_avg_price_comparison GROUP BY match_status"
    ).fetchall()
    avg = {"total": 0, "match": 0, "mismatch": 0, "stg_only": 0, "fct_only": 0}
    for status, cnt in avg_rows:
        avg["total"] += cnt
        avg[status.lower()] = cnt
    summary["avg_price"] = avg

    # Top-10 delta reconciliation
    delta_rows = conn.execute(
        "SELECT match_status, COUNT(*) FROM rec_top10_price_delta_comparison GROUP BY match_status"
    ).fetchall()
    delta = {"total": 0, "match": 0, "mismatch": 0, "stg_only": 0, "fct_only": 0}
    for status, cnt in delta_rows:
        delta["total"] += cnt
        delta[status.lower()] = cnt
    summary["top10_delta"] = delta

    # Reporting view reconciliation
    view_rows = conn.execute(
        "SELECT match_status, COUNT(*) FROM rec_reporting_view_comparison GROUP BY match_status"
    ).fetchall()
    views = {"total": 0, "match": 0, "mismatch": 0}
    for status, cnt in view_rows:
        views["total"] += cnt
        views[status.lower()] = cnt
    summary["views"] = views

    # Collect specific mismatch details for the email
    avg_mismatches = conn.execute(
        """SELECT dc.city_name, dn.neighbourhood_cleansed, r.room_type,
                  r.stg_avg_price, r.fct_avg_price, r.price_diff
           FROM rec_avg_price_comparison r
           JOIN dim_city dc ON r.city_key = dc.city_key
           JOIN dim_neighbourhood dn ON r.neighbourhood_key = dn.neighbourhood_key
           WHERE r.match_status != 'MATCH'"""
    ).fetchall()
    for city, nb, rt, stg_p, fct_p, diff in avg_mismatches:
        summary["mismatches"].append(
            f"Avg price: {city}/{nb} ({rt}) — stg={stg_p}, fct={fct_p}, diff={diff}"
        )

    view_mismatches = conn.execute(
        """SELECT dc.city_name, r.view_name, r.fct_row_count, r.view_row_count, r.count_diff
           FROM rec_reporting_view_comparison r
           JOIN dim_city dc ON r.city_key = dc.city_key
           WHERE r.match_status != 'MATCH'"""
    ).fetchall()
    for city, vn, fct_c, view_c, diff in view_mismatches:
        summary["mismatches"].append(
            f"View count: {city}/{vn} — fct={fct_c}, view={view_c}, diff={diff}"
        )

    return summary


def _build_email_body(
    run_info: dict,
    errors: list[dict],
    cities: list[str],
    failed: list[str],
    rec_summary: dict | None = None,
) -> str:
    """Build a plain-text email body from run data."""
    lines = [
        "BreezeBnb Pipeline Run Report",
        "=" * 40,
        "",
        f"Timestamp   : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Run ID      : {run_info['run_id']}",
        f"Status      : {run_info['status']}",
        f"Start Time  : {run_info['start_time']}",
        f"End Time    : {run_info['end_time'] or 'N/A'}",
        "",
        f"Cities Processed : {', '.join(cities) if cities else 'None'}",
        f"Cities Failed    : {', '.join(failed) if failed else 'None'}",
        "",
    ]

    if run_info["row_counts"]:
        lines.append("Row Counts:")
        lines.append("-" * 30)
        try:
            counts = json.loads(run_info["row_counts"])
            for table, count in counts.items():
                lines.append(f"  {table}: {count}")
        except (json.JSONDecodeError, TypeError):
            lines.append(f"  {run_info['row_counts']}")
        lines.append("")

    # Reconciliation summary
    if rec_summary:
        _append_reconciliation_section(lines, rec_summary)

    if run_info["error_message"]:
        lines.append(f"Error Message: {run_info['error_message']}")
        lines.append("")

    if errors:
        lines.append("Detailed Errors / Warnings:")
        lines.append("-" * 30)
        for err in errors:
            lines.append(
                f"  [{err['error_type']}] {err['table_name'] or 'N/A'}: "
                f"{err['error_details']}"
            )
        lines.append("")

    lines.append("--")
    lines.append("BreezeBnb Data Platform — Automated Notification")
    return "\n".join(lines)


def _append_reconciliation_section(lines: list[str], rec: dict) -> None:
    """Append reconciliation results to the email body lines."""
    avg = rec.get("avg_price", {})
    delta = rec.get("top10_delta", {})
    views = rec.get("views", {})
    mismatches = rec.get("mismatches", [])

    has_issues = (
        avg.get("mismatch", 0) + avg.get("stg_only", 0) + avg.get("fct_only", 0)
        + delta.get("mismatch", 0) + delta.get("stg_only", 0) + delta.get("fct_only", 0)
        + views.get("mismatch", 0)
    )

    header = "ISSUES DETECTED" if has_issues else "ALL PASSED"
    lines.append(f"Reconciliation ({header}):")
    lines.append("-" * 30)

    def _fmt(label, d):
        parts = [f"{d.get('match', 0)}/{d.get('total', 0)} match"]
        for key in ("mismatch", "stg_only", "fct_only"):
            val = d.get(key, 0)
            if val:
                parts.append(f"{val} {key}")
        return f"  {label}: {', '.join(parts)}"

    lines.append(_fmt("Avg Price", avg))
    lines.append(_fmt("Top-10 Delta", delta))
    lines.append(_fmt("Report Views", views))

    if mismatches:
        lines.append("")
        lines.append("  Mismatch Details:")
        for m in mismatches:
            lines.append(f"    - {m}")

    lines.append("")


def send_run_notification(
    conn: sqlite3.Connection,
    cities: list[str],
    failed: list[str],
) -> bool:
    """Send an email notification with the latest pipeline run summary.

    Args:
        conn: SQLite database connection (to read etl_run_log / etl_error_log).
        cities: List of cities that were processed.
        failed: List of cities that failed.

    Returns:
        True if the email was sent successfully, False otherwise.
    """
    config = _get_smtp_config()

    if not config["user"] or not config["password"]:
        logger.warning(
            "Email notification skipped: BREEZEBNB_SMTP_USER and "
            "BREEZEBNB_SMTP_PASSWORD environment variables are not set."
        )
        return False

    if not config["notify_email"]:
        logger.warning("Email notification skipped: BREEZEBNB_NOTIFY_EMAIL is not set.")
        return False

    run_info = _get_latest_run(conn)
    if not run_info:
        logger.warning("Email notification skipped: no ETL run records found.")
        return False

    errors = _get_run_errors(conn, run_info["run_id"])
    rec_summary = _get_reconciliation_summary(conn)
    body = _build_email_body(run_info, errors, cities, failed, rec_summary)

    status = run_info["status"]
    subject = f"BreezeBnb Pipeline Run #{run_info['run_id']} — {status}"

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = config["user"]
    msg["To"] = config["notify_email"]

    try:
        with smtplib.SMTP(config["host"], config["port"]) as server:
            server.starttls()
            server.login(config["user"], config["password"])
            server.sendmail(config["user"], [config["notify_email"]], msg.as_string())
        logger.info("Notification email sent to %s", config["notify_email"])
        return True
    except Exception as e:
        logger.error("Failed to send notification email: %s", e)
        return False
