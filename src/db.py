"""
Database connection layer for the ModularSnapshotETL pipeline.

Uses Postgres (e.g. Neon) when a DATABASE_URL is configured, and falls back
to a local SQLite file otherwise (used for local development and tests).

All call sites write plain '?'-style placeholder SQL, same as sqlite3. The
PGConnection/PGCursor wrappers below translate that to psycopg2's '%s' style
so the rest of the codebase does not need to branch on backend.
"""
import os
import re

import pandas as pd

DEFAULT_SQLITE_PATH = "ModularSnapshotETL.db"

_QMARK_RE = re.compile(r"\?")


def _qmark_to_pyformat(sql: str) -> str:
    return _QMARK_RE.sub("%s", sql)


def _load_dotenv(path=".env"):
    """Load key=value pairs from a .env file into os.environ."""
    if not os.path.isfile(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, sep, value = line.partition("=")
            if key and sep == "=":
                os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()


class PGCursor:
    """Thin wrapper so a psycopg2 cursor accepts '?' placeholders like sqlite3."""

    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql, params=()):
        self._cur.execute(_qmark_to_pyformat(sql), tuple(params))
        return self

    def executemany(self, sql, seq_of_params):
        self._cur.executemany(_qmark_to_pyformat(sql), seq_of_params)
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    @property
    def description(self):
        return self._cur.description

    @property
    def rowcount(self):
        return self._cur.rowcount

    def close(self):
        self._cur.close()


class PGConnection:
    """Wraps a psycopg2 connection to add sqlite3's `.execute()` shortcut."""

    def __init__(self, dsn):
        import time

        import psycopg2

        # Neon (and similar serverless Postgres) can be suspended after
        # idleness; the connection that wakes it can fail once ("SSL
        # connection has been closed unexpectedly") while the compute
        # spins back up, then succeed moments later. Retry a few times
        # before giving up. connect_timeout also avoids hanging forever
        # on a genuinely unreachable host.
        attempts = 3
        delay = 2
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                self._conn = psycopg2.connect(dsn, connect_timeout=10)
                return
            except psycopg2.OperationalError as e:
                last_error = e
                if attempt < attempts:
                    time.sleep(delay)
        raise last_error

    def execute(self, sql, params=()):
        return PGCursor(self._conn.cursor()).execute(sql, params)

    def cursor(self):
        return PGCursor(self._conn.cursor())

    def raw_cursor(self):
        """Native psycopg2 cursor, for calls that need psycopg2-specific helpers."""
        return self._conn.cursor()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def is_postgres(conn) -> bool:
    return isinstance(conn, PGConnection)


def get_database_url() -> str | None:
    """Resolve the Postgres connection string from env or Streamlit secrets."""
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    try:
        import streamlit as st

        return st.secrets.get("DATABASE_URL")
    except Exception:
        return None


def get_connection(sqlite_path: str = DEFAULT_SQLITE_PATH):
    """Return a Postgres connection if configured, else a local SQLite connection."""
    dsn = get_database_url()
    if dsn:
        return PGConnection(dsn)

    import sqlite3

    conn = sqlite3.connect(sqlite_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def read_sql(conn, sql, params=None) -> pd.DataFrame:
    """Run a query and return a DataFrame — portable across sqlite3/Postgres."""
    cur = conn.cursor()
    cur.execute(sql, params or ())
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _rows_for_insert(df: pd.DataFrame) -> list:
    """DataFrame rows as tuples with NaN/NaT converted to None.

    Pandas' own to_sql does this implicitly, but Postgres (unlike SQLite)
    rejects a bare NaN for non-float columns.
    """
    clean_df = df.astype(object).where(df.notna(), None)
    return [tuple(r) for r in clean_df.itertuples(index=False, name=None)]


def bulk_insert(conn, table: str, df: pd.DataFrame) -> None:
    """Append DataFrame rows into `table` — portable across sqlite3/Postgres."""
    if df.empty:
        return

    cols = list(df.columns)
    values = _rows_for_insert(df)

    if is_postgres(conn):
        from psycopg2.extras import execute_values

        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
        execute_values(conn.raw_cursor(), sql, values, page_size=1000)
    else:
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
        conn.executemany(sql, values)

    conn.commit()


def bulk_upsert(conn, table: str, df: pd.DataFrame, conflict_cols: list) -> None:
    """Bulk INSERT ... ON CONFLICT DO UPDATE, portable across sqlite3/Postgres.

    Much faster than one execute() per row against Postgres, where each
    round trip pays full network latency. update_cols is every column
    that isn't part of the conflict key.
    """
    if df.empty:
        return

    cols = list(df.columns)
    update_cols = [c for c in cols if c not in conflict_cols]
    values = _rows_for_insert(df)

    conflict_clause = ", ".join(conflict_cols)
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    on_conflict = f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"

    if is_postgres(conn):
        from psycopg2.extras import execute_values

        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s {on_conflict}"
        execute_values(conn.raw_cursor(), sql, values, page_size=1000)
    else:
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) {on_conflict}"
        conn.executemany(sql, values)

    conn.commit()


def bulk_insert_returning(conn, table: str, df: pd.DataFrame, returning_cols: list) -> list:
    """Bulk INSERT with RETURNING, returning a list of result tuples.

    Postgres only (uses psycopg2's execute_values fetch mode) — callers
    on SQLite should insert row-by-row instead, since that's already fast
    without network round trips.
    """
    if df.empty:
        return []

    cols = list(df.columns)
    values = _rows_for_insert(df)

    from psycopg2.extras import execute_values

    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
        f"RETURNING {', '.join(returning_cols)}"
    )
    return execute_values(conn.raw_cursor(), sql, values, page_size=1000, fetch=True)


def table_columns(conn, table_name: str) -> set:
    """Return the set of column names for a table — portable across sqlite3/Postgres."""
    if is_postgres(conn):
        cur = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            (table_name,),
        )
        return {row[0] for row in cur.fetchall()}

    cur = conn.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}
