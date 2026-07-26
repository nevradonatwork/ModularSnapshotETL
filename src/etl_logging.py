"""
ETL logging and error handling for the ModularSnapshotETL pipeline.

Provides structured logging to pipeline_execution_log and pipeline_error_log
tables, plus Python logging integration.
"""
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

PIPELINE_NAME = "ModularSnapshotETL"


def start_run(
    conn,
    city: str | None = None,
    snapshot_month: str | None = None,
    source_file_path: str | None = None,
    triggered_by: str | None = None,
) -> int:
    """Create a new pipeline execution record and return its run_id."""
    source_file_name = os.path.basename(source_file_path) if source_file_path else None
    run_id = conn.execute(
        """INSERT INTO pipeline_execution_log
           (pipeline_name, start_time, status, city, snapshot_month, source_file_path,
            source_file_name, triggered_by)
           VALUES (?, ?, 'RUNNING', ?, ?, ?, ?, ?) RETURNING run_id""",
        (
            PIPELINE_NAME,
            datetime.now(timezone.utc).isoformat(),
            city,
            snapshot_month,
            source_file_path,
            source_file_name,
            triggered_by or "cli",
        ),
    ).fetchone()[0]
    conn.commit()
    logger.info("Pipeline run started: run_id=%d city=%s triggered_by=%s", run_id, city, triggered_by)
    return run_id


def finish_run(
    conn,
    run_id: int,
    status: str,
    row_counts: dict | None = None,
    error_message: str | None = None,
) -> None:
    """Finalise a pipeline execution record."""
    rows_processed = sum(v for v in row_counts.values() if isinstance(v, int)) if row_counts else None
    conn.execute(
        """UPDATE pipeline_execution_log
           SET end_time = ?, status = ?, row_counts = ?, rows_processed = ?, error_message = ?
           WHERE run_id = ?""",
        (
            datetime.now(timezone.utc).isoformat(),
            status,
            json.dumps(row_counts) if row_counts else None,
            rows_processed,
            error_message,
            run_id,
        ),
    )
    conn.commit()
    logger.info("Pipeline run finished: run_id=%d status=%s", run_id, status)


def update_archived_path(
    conn,
    run_id: int,
    archived_file_path: str,
) -> None:
    """Store the archived file path on the pipeline execution record."""
    conn.execute(
        "UPDATE pipeline_execution_log SET archived_file_path = ? WHERE run_id = ?",
        (archived_file_path, run_id),
    )
    conn.commit()
    logger.info("Archived file path logged: run_id=%d path=%s", run_id, archived_file_path)


def log_error(
    conn,
    run_id: int,
    error_type: str,
    error_details: str,
    table_name: str | None = None,
) -> None:
    """Record an error in the pipeline_error_log table."""
    conn.execute(
        """INSERT INTO pipeline_error_log
           (run_id, table_name, error_type, error_details, timestamp)
           VALUES (?, ?, ?, ?, ?)""",
        (
            run_id,
            table_name,
            error_type,
            error_details,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()
    logger.error("[run_id=%d] %s: %s", run_id, error_type, error_details)
