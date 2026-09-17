# Nevra's Perfect ETL — Standing Checklist

A general checklist to apply on every future ETL build, independent of the
specific project or database engine. Established from real lessons across ETL
work (including the Vax GA4 pipeline); recorded here for reuse.

- **HARD RULE**: if the source data is available at daily grain, never build
  gold as a pre-aggregated total (monthly, weekly, etc.) via `MERGE` that
  overwrites the whole aggregate. Gold must be built at the finest grain the
  source supports (day), with any higher-level reporting total (month, week)
  populated *from* that day-grain gold, never computed as the only physical
  representation. A partial reprocess or backfill against a monthly-grain
  `MERGE` corrupts the entire month; against day-grain gold it's isolated to
  the affected days.

- Every bronze, history, and gold table should carry `etl_processed_at`
  (TIMESTAMP, nullable) and `etl_run_id` (STRING, nullable) columns, so the
  origin and processing time of any row can always be traced end to end. Not
  just on bronze — extend to history and gold on every future build.

- The load process should process only unprocessed rows
  (`WHERE etl_processed_at IS NULL`) from bronze each run. No `NOT EXISTS`
  anti-join dedup against history, no rolling-window (e.g. 28-day)
  reprocessing logic. This relies on the source (e.g. Adverity) doing a full
  truncate+reload per partition/day, so a corrected day is always either
  fully unprocessed or fully done, never partially stale.

- Before processing, run a preflight check that any source streams which
  jointly compute a derived value (e.g. a fan-out scale factor computed from
  two streams) have matching unprocessed date sets. Abort with a clear error
  if they don't, rather than silently computing a wrong value from a partial
  backfill.

- Gold tables are two-tier: day-grain physical gold table(s) are merged first
  from the unprocessed rows, then only the affected higher-level periods
  (e.g. months) are re-rolled from the day-grain gold into the existing
  reporting-grain gold tables. Never recompute the full history when only a
  few days changed.

- History tables become pure append-only archives once the day-grain gold
  approach is in place — stamped and written to for traceability, but never
  read again for computation.

- Stamp bronze (`etl_processed_at`/`etl_run_id`) only as the last step, after
  every downstream write (history archive, day-grain gold, reporting-grain
  gold) has succeeded. This makes failure recovery automatic: an unstamped
  row is retried in full on the next run; a stamped row is never reprocessed.

- Provide a dedicated backload/reset helper procedure (e.g. `sp_backload_ga4`)
  that clears `etl_processed_at`/`etl_run_id` for a given date range across
  all the source streams that need to move together, so a manual
  reprocessing request never risks leaving one stream out of sync with
  another.

- History (archive) tables should also carry `source_filename` (copied from
  the ingestion tool's own per-extract filename column, e.g. Adverity's
  `dt_filename`) alongside `etl_run_id`/`etl_processed_at` — bronze's own
  filename column gets overwritten on the next truncate+reload of that
  partition, so only the history copy gives a permanent record of which
  source file produced each archived row.

- A `RAISE USING MESSAGE` (or equivalent early-exit statement) used for a
  validation check (e.g. a preflight check) must have the concurrency lock
  explicitly released immediately before it, in the same `IF` block —
  `RAISE` outside a `BEGIN...EXCEPTION` block skips the rest of the procedure
  entirely, including any lock-release statement at the very end, leaving
  the lock stuck until its timeout and blocking every subsequent run.
