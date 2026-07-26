"""
Row-count + checksum reconciliation and watermark tracking.

Kept separate from src/reconciliation.py, which does deep business-rule
comparison of independently-recomputed values (avg price, rank deltas).
This module is generic, table-agnostic pipeline-health bookkeeping:
did N rows make it from stage A to stage B, and do the ids match.
"""
import hashlib
import logging
from datetime import datetime, timezone

import pandas as pd

from src import db

logger = logging.getLogger(__name__)


def compute_checksum(conn, sql: str, params=None) -> str:
    """Checksum a query's (single-column) result set.

    Sorts values so row order never affects the checksum, then hashes
    with SHA-256. Computed in Python (not a backend-specific SQL function
    like Postgres's md5()) so it works identically on SQLite and Postgres.
    """
    df = db.read_sql(conn, sql, params)
    if df.empty:
        return hashlib.sha256(b"").hexdigest()
    values = sorted(str(v) for v in df.iloc[:, 0].tolist())
    return hashlib.sha256(",".join(values).encode("utf-8")).hexdigest()


def checksum_from_series(values) -> str:
    """Checksum an in-memory list/Series of values — same rule as compute_checksum."""
    sorted_values = sorted(str(v) for v in values)
    return hashlib.sha256(",".join(sorted_values).encode("utf-8")).hexdigest()


def log_row_count_check(
    conn,
    run_id: int,
    table_name: str,
    source_count: int,
    target_count: int,
    checksum_source: str,
    checksum_target: str,
) -> str:
    """Insert one row_count_reconciliation row for a source -> target load.

    Returns the match_status ("matched"/"mismatched").
    """
    match_status = (
        "matched"
        if source_count == target_count and checksum_source == checksum_target
        else "mismatched"
    )
    conn.execute(
        """INSERT INTO row_count_reconciliation
           (run_id, table_name, source_row_count, target_row_count,
            checksum_source, checksum_target, match_status, checked_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            run_id, table_name, source_count, target_count,
            checksum_source, checksum_target, match_status,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    if match_status == "mismatched":
        logger.warning(
            "Row count reconciliation MISMATCH for %s (run_id=%d): "
            "source=%d target=%d",
            table_name, run_id, source_count, target_count,
        )
    return match_status


def update_watermark(conn, table_name: str, run_id: int) -> None:
    """Upsert watermark_control after a table's successful load.

    Bookkeeping only ("last time this table was fully loaded, by which
    run") -- the pipeline ingests whole files per (city, snapshot_month)
    upload rather than a continuously-queryable growing source, so this
    isn't a resumable-extraction cursor.
    """
    df = pd.DataFrame([{
        "table_name": table_name,
        "last_successful_load_timestamp": datetime.now(timezone.utc).isoformat(),
        "last_run_id": run_id,
    }])
    db.bulk_upsert(conn, "watermark_control", df, conflict_cols=["table_name"])
