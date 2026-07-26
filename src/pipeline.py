"""
Core pipeline orchestration for the ModularSnapshotETL data platform.

Execution flow:
    1. Validate file existence
    2. Derive snapshot month
    3. Load into raw_listings
    4. Archive source file
    5. Load stg_listings with dedupe + geo validation
    6. Upsert dimensions
    7. Load fact tables (excluding geo-flagged rows)
    8. Log execution metrics
"""
import logging
import os

from src import db, etl_logging
from src.ingestion import load_raw, load_staging, derive_snapshot_month, archive_file
from src.dimensions import (
    load_dim_date,
    load_dim_city,
    load_dim_neighbourhoods,
    load_dim_hosts,
    load_dim_listings,
    load_neighbourhood_reference_files,
    backfill_staging_neighbourhoods_from_dim_geo,
)
from src.facts import (
    load_fct_listing_monthly_snapshot,
    load_fct_neighbourhood_monthly_avg_price,
    load_fct_neighbourhood_monthly_top10_price_delta,
    load_fct_data_compliance_monthly,
)
from src.reconciliation import (
    reconcile_avg_price,
    reconcile_top10_price_delta,
    reconcile_reporting_views,
)
from src.validation import validate_file, validate_data_quality

logger = logging.getLogger(__name__)


def run(
    conn,
    dataset_path: str,
    city: str,
    triggered_by: str | None = None,
) -> dict:
    """Run the full pipeline for a single city/file.

    Args:
        conn: SQLite database connection.
        dataset_path: Path to the listings CSV (or .csv.gz) file.
        city: City name identifier.
        triggered_by: How the run was triggered (e.g. "dashboard", "cli").

    Returns:
        dict with execution metrics (row counts per layer).
    """
    # Create run record with file metadata (before validation so failures are logged)
    run_id = etl_logging.start_run(
        conn, city=city, source_file_path=dataset_path,
        triggered_by=triggered_by,
    )
    row_counts = {}

    try:
        # Step 1: Validate file
        validate_file(dataset_path)

        # Step 2: Derive snapshot month and update run record
        snapshot_month = derive_snapshot_month(dataset_path)
        logger.info("Snapshot month: %s", snapshot_month)
        conn.execute(
            "UPDATE pipeline_execution_log SET snapshot_month = ? WHERE run_id = ?",
            (snapshot_month, run_id),
        )
        conn.commit()

        # Step 3: Load raw layer
        raw_count = load_raw(conn, dataset_path, city, snapshot_month, run_id)
        row_counts["raw_listings"] = raw_count

        # Step 4: Archive source file after successful raw ingestion
        archived_path = archive_file(dataset_path, city, snapshot_month)
        etl_logging.update_archived_path(conn, run_id, archived_path)

        # Step 5: Load staging layer (clean + dedupe + geo flag)
        stg_count, invalid_price_count, geo_flagged_count = load_staging(
            conn, city, snapshot_month,
        )
        row_counts["stg_listings"] = stg_count
        row_counts["invalid_price_excluded"] = invalid_price_count
        row_counts["geo_out_of_city_count"] = geo_flagged_count

        if invalid_price_count > 0:
            etl_logging.log_error(
                conn, run_id, "INVALID_PRICE",
                f"{invalid_price_count} rows excluded: price was null or <= 0",
                "stg_listings",
            )

        if geo_flagged_count > 0:
            etl_logging.log_error(
                conn, run_id, "GEO_OUT_OF_CITY",
                f"{geo_flagged_count} rows flagged as outside city bounding box",
                "stg_listings",
            )

        # Step 6: Ensure core dimensions + neighbourhood reference files are loaded
        month_key = load_dim_date(conn, snapshot_month)
        city_key = load_dim_city(conn, city)

        city_dir = os.path.dirname(dataset_path)
        csv_loaded, geo_loaded = load_neighbourhood_reference_files(conn, city_key, city_dir)
        row_counts["dim_neighbourhood_reference_csv"] = csv_loaded
        row_counts["dim_neighbourhood_reference_geojson"] = geo_loaded

        # Step 6a: Backfill missing neighbourhoods in staging using dim geographies
        backfilled_count = backfill_staging_neighbourhoods_from_dim_geo(
            conn, city, snapshot_month, city_key
        )
        row_counts["stg_neighbourhood_geo_backfilled"] = backfilled_count

        # Step 6b: Read staging data for validation + dimension loading
        stg_df = db.read_sql(
            conn,
            "SELECT * FROM stg_listings WHERE city = ? AND snapshot_month = ?",
            (city, snapshot_month),
        )

        warnings = validate_data_quality(stg_df)
        if warnings:
            for w in warnings:
                etl_logging.log_error(
                    conn, run_id, "DATA_QUALITY_WARNING", w, "stg_listings"
                )

        # Step 7: Upsert dimensions
        neighbourhood_map = load_dim_neighbourhoods(conn, city_key, stg_df)
        host_map = load_dim_hosts(conn, stg_df, snapshot_month)
        listing_map = load_dim_listings(conn, stg_df, snapshot_month)

        row_counts["dim_neighbourhoods"] = len(neighbourhood_map)
        row_counts["dim_hosts"] = len(host_map)
        row_counts["dim_listings"] = len(listing_map)

        # Step 8: Load fact tables (geo-flagged rows excluded automatically)
        fct_count = load_fct_listing_monthly_snapshot(
            conn, city_key, month_key, neighbourhood_map, listing_map, host_map
        )
        row_counts["fct_listing_monthly_snapshot"] = fct_count

        avg_count = load_fct_neighbourhood_monthly_avg_price(conn, city_key, month_key)
        row_counts["fct_neighbourhood_monthly_avg_price"] = avg_count

        delta_count = load_fct_neighbourhood_monthly_top10_price_delta(
            conn, city_key, month_key
        )
        row_counts["fct_neighbourhood_monthly_top10_price_delta"] = delta_count

        compliance_count = load_fct_data_compliance_monthly(conn, city_key, month_key)
        row_counts["fct_data_compliance_monthly"] = compliance_count

        # Step 9: Reconciliation — compare staging vs fact tables vs views
        rec_avg = reconcile_avg_price(conn, city_key, month_key)
        row_counts["rec_avg_price_comparison"] = rec_avg

        rec_delta = reconcile_top10_price_delta(conn, city_key, month_key)
        row_counts["rec_top10_price_delta_comparison"] = rec_delta

        rec_views = reconcile_reporting_views(conn, city_key, month_key)
        row_counts["rec_reporting_view_comparison"] = rec_views

        # Step 10: Finalise
        etl_logging.finish_run(conn, run_id, "SUCCESS", row_counts)
        logger.info("Pipeline complete for %s. Row counts: %s", city, row_counts)
        return row_counts

    except Exception as e:
        # Roll back the failed statement's transaction first — on Postgres,
        # a connection stays unusable for further writes until this happens.
        conn.rollback()
        etl_logging.log_error(conn, run_id, type(e).__name__, str(e))
        etl_logging.finish_run(conn, run_id, "FAILED", row_counts, str(e))
        raise
