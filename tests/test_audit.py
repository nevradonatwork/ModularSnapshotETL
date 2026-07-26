import pandas as pd
import pytest

from src import audit, db


class TestChecksum:
    def test_checksum_from_series_is_order_independent(self):
        assert audit.checksum_from_series([1, 2, 3]) == audit.checksum_from_series([3, 1, 2])

    def test_checksum_from_series_differs_on_different_data(self):
        assert audit.checksum_from_series([1, 2, 3]) != audit.checksum_from_series([1, 2, 4])

    def test_checksum_from_series_empty(self):
        # Should not raise, and should be a stable, deterministic value
        assert audit.checksum_from_series([]) == audit.checksum_from_series([])

    def test_compute_checksum_matches_in_memory_equivalent(self, db_conn):
        db_conn.execute("CREATE TABLE t (id INTEGER)")
        db_conn.executemany("INSERT INTO t (id) VALUES (?)", [(1,), (2,), (3,)])
        db_conn.commit()

        checksum = audit.compute_checksum(db_conn, "SELECT id FROM t")
        assert checksum == audit.checksum_from_series([1, 2, 3])

    def test_compute_checksum_empty_result(self, db_conn):
        db_conn.execute("CREATE TABLE empty_t (id INTEGER)")
        db_conn.commit()

        checksum = audit.compute_checksum(db_conn, "SELECT id FROM empty_t")
        assert checksum == audit.checksum_from_series([])


class TestRowCountReconciliation:
    @pytest.fixture
    def run_id(self, db_conn):
        return db_conn.execute(
            "INSERT INTO pipeline_execution_log (start_time, status) "
            "VALUES ('2025-01-01T00:00:00', 'RUNNING') RETURNING run_id"
        ).fetchone()[0]

    def test_matched_when_counts_and_checksums_equal(self, db_conn, run_id):
        status = audit.log_row_count_check(
            db_conn, run_id, "some_table",
            source_count=3, target_count=3,
            checksum_source="abc", checksum_target="abc",
        )
        assert status == "matched"

        row = db_conn.execute(
            "SELECT match_status, source_row_count, target_row_count FROM row_count_reconciliation "
            "WHERE run_id = ?", (run_id,)
        ).fetchone()
        assert row == ("matched", 3, 3)

    def test_mismatched_on_count_difference(self, db_conn, run_id):
        status = audit.log_row_count_check(
            db_conn, run_id, "some_table",
            source_count=5, target_count=3,
            checksum_source="abc", checksum_target="abc",
        )
        assert status == "mismatched"

    def test_mismatched_on_checksum_difference(self, db_conn, run_id):
        status = audit.log_row_count_check(
            db_conn, run_id, "some_table",
            source_count=3, target_count=3,
            checksum_source="abc", checksum_target="xyz",
        )
        assert status == "mismatched"


class TestWatermark:
    @pytest.fixture
    def run_id(self, db_conn):
        return db_conn.execute(
            "INSERT INTO pipeline_execution_log (start_time, status) "
            "VALUES ('2025-01-01T00:00:00', 'RUNNING') RETURNING run_id"
        ).fetchone()[0]

    def test_insert_new_watermark(self, db_conn, run_id):
        audit.update_watermark(db_conn, "raw_listings", run_id)

        row = db_conn.execute(
            "SELECT table_name, last_run_id FROM watermark_control WHERE table_name = ?",
            ("raw_listings",),
        ).fetchone()
        assert row == ("raw_listings", run_id)

    def test_update_existing_watermark(self, db_conn, run_id):
        audit.update_watermark(db_conn, "raw_listings", run_id)

        second_run_id = db_conn.execute(
            "INSERT INTO pipeline_execution_log (start_time, status) "
            "VALUES ('2025-01-02T00:00:00', 'RUNNING') RETURNING run_id"
        ).fetchone()[0]
        audit.update_watermark(db_conn, "raw_listings", second_run_id)

        rows = db_conn.execute(
            "SELECT last_run_id FROM watermark_control WHERE table_name = ?",
            ("raw_listings",),
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == second_run_id
