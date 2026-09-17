# CLAUDE.md

Project-specific context for Claude Code sessions working on **ModularSnapshotETL** —
a monthly Airbnb pricing intelligence pipeline (Inside Airbnb snapshots -> medallion
warehouse -> Streamlit dashboard). Read this before making structural changes.

## Architecture

- **Medallion schemas on Postgres**: `bronze` (raw), `silver` (staging), `gold`
  (dimensions/facts/reporting views), `metadata` (pipeline logging, audit,
  watermark, visitor analytics, business-rule reconciliation). See `src/schema.py`.
- **SQLite has no schema separation** — it stays flat (single namespace), used only
  for local dev and the test suite. Every table/view name is globally unique across
  the four Postgres schemas, so application SQL never needs to be schema-qualified;
  a single `SET search_path` per Postgres connection handles resolution. Don't
  qualify table names in `src/dimensions.py`, `src/facts.py`, `src/ingestion.py`,
  `src/reconciliation.py`, etc. — only `schema.py`'s DDL needs to know about schemas.
- **`src/db.py`** is the backend-agnostic connection layer: `get_connection()`
  returns Postgres (when `DATABASE_URL` is set, via env or Streamlit secrets) or a
  local SQLite file. All application code writes plain `?`-placeholder SQL; the
  `PGConnection`/`PGCursor` wrappers translate to psycopg2's `%s` internally.
  Reuse `db.read_sql`, `db.bulk_insert`, `db.bulk_upsert`, `db.bulk_insert_returning`,
  `db.table_columns` rather than hand-rolling new query helpers.

## Idempotency rules (don't break these)

- **`raw_listings` (bronze) is append-only.** Rerunning a city/month adds a new
  copy of the source data — this is the full audit trail. Never delete from it.
- **`stg_listings` (silver) and every `fct_*` table are scoped delete-then-insert**
  per `(city, snapshot_month)` or `(month_key, city_key)`. Rerunning replaces only
  that scope; other cities/months are never touched.
- **`dim_host` / `dim_listing` are SCD Type 2**: a changed attribute closes the old
  row (`valid_to`, `is_current = 0`) and opens a new one — it never overwrites.
- When adding a new load step, follow this same pattern: idempotent, scoped, and
  it must not require a specific run order relative to other cities.

## Postgres/Neon gotchas learned the hard way this project

- **Use Neon's direct (non-pooled) connection string, not the `-pooler` one.**
  PgBouncer transaction-pooling can serve each transaction from a different
  backend session, silently dropping the session-level `SET search_path` —
  queries then fail with "relation does not exist" despite having worked moments
  earlier. `PGConnection` re-asserts `search_path` after every commit/rollback as
  defense in depth, but the direct connection string avoids the problem entirely.
- **Neon free tier auto-suspends after idleness.** The connection that wakes it can
  fail once (`SSL connection has been closed unexpectedly`) before succeeding —
  `PGConnection.__init__` retries a few times with a short delay and a
  `connect_timeout`, rather than hanging or failing on the first attempt.
- **Airbnb natural keys need `BIGINT`, not `INTEGER`.** `scrape_id` is a 14-digit
  number; listing/host ids keep growing past 2^31. SQLite's dynamic typing hid
  this; Postgres enforces it strictly. If you add a new natural-key column from
  source data, default to `BIGINT`.
- **`ROUND()` on an aggregated `REAL`/`float8` needs a `NUMERIC` cast in Postgres**
  (`ROUND(CAST(AVG(x) AS NUMERIC), 2)`) — `ROUND(double precision, integer)`
  doesn't exist as a Postgres function, only `ROUND(numeric, integer)`.
- **NaN must become `NULL` before a Postgres insert.** `db.bulk_insert`/
  `bulk_upsert` already convert NaN/NaT to `None`; if you write a new raw insert
  path, do the same (`df.astype(object).where(df.notna(), None)`) — Postgres
  rejects a bare NaN for non-float columns; SQLite silently tolerated it.
- **Batch writes for Postgres; row-by-row is fine for SQLite.** A `for` loop doing
  one `execute()` per row is invisible on a local SQLite file but costs full
  network latency per row against Neon — for a few-thousand-row city this is the
  difference between seconds and many minutes. Use `db.bulk_insert`/`bulk_upsert`/
  `bulk_insert_returning` (built on `execute_values`, `page_size=1000`) for any
  new per-row write loop. Where per-row SCD2 branching logic must stay row-by-row
  (SQLite has no network cost to avoid), it's fine to keep it — but add a
  batched Postgres-only fast path, following `src/dimensions.py`'s
  `_load_dim_hosts_batched` / `_load_dim_hosts_row_by_row` split as the template.

## Testing discipline

- The 90+ test suite (`tests/`) runs entirely against SQLite (`tests/conftest.py`'s
  `db_conn` fixture) and passes on every PR — but **every real Postgres-only bug
  found this project (BIGINT overflow, `ROUND()` cast, connection pooling,
  N+1 performance) was invisible to that suite.** Before merging a change that
  touches `src/schema.py`, `src/db.py`, or any bulk write path, verify it against
  a real Postgres instance manually (a local `postgresql` service is fine), not
  just the SQLite test suite.
- When simulating "an already-migrated production database" for a schema-change
  test, build the old state with the *actual* pre-change code (e.g. via a git
  worktree checked out to the previous commit) rather than a hand-written stub —
  a hand-rolled stub table is easy to make subtly incomplete and gives false
  confidence.

## Style

- No em-dash character (`—`) anywhere in code, comments, docs, or UI text. Use a
  comma or restructure the sentence instead.
- Comments explain the non-obvious *why* (a constraint, a workaround, an
  invariant) — not what the code already says through naming.

## Git workflow for this repo

- Small, sequential PRs verified end to end (SQLite tests + manual Postgres check)
  before merging — not one large PR for a multi-part change.
- A schema change that renames or moves existing tables needs a separate,
  explicit production migration step (see `scripts/migrate_to_medallion_schema.sql`
  as the template) run manually against the live Neon database *before* the
  dependent code is deployed — `CREATE TABLE IF NOT EXISTS` never moves existing
  data on its own.
- The Streamlit Community Cloud app auto-deploys on push to `main`, so merging is
  effectively deploying — treat it accordingly.
