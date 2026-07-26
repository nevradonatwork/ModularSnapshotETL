-- ModularSnapshotETL: one-time migration to medallion schemas
-- (bronze/silver/gold/metadata).
--
-- WHY THIS IS SEPARATE FROM THE APP: the new schema.py creates tables with
-- "CREATE TABLE IF NOT EXISTS" inside bronze/silver/gold/metadata schemas.
-- That creates NEW, EMPTY tables -- it does NOT move your existing data
-- out of the flat "public" schema. Run this script FIRST, against the
-- live database, BEFORE deploying the code that expects the new schemas
-- to already contain your data. ALTER TABLE ... SET SCHEMA is a fast,
-- transactional, metadata-only operation in Postgres -- it does not copy
-- or rewrite any rows.
--
-- Safe to re-run: every step checks existence first and is a no-op if
-- already migrated (or if there was nothing to migrate, e.g. a brand new
-- database).
--
-- HOW TO RUN: paste this whole file into Neon's SQL Editor (or `psql -f`)
-- and execute it once.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

-- Bronze: raw layer
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='raw_listings')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='bronze' AND table_name='raw_listings') THEN
    ALTER TABLE public.raw_listings SET SCHEMA bronze;
  END IF;
END $$;

-- Silver: staging layer
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='stg_listings')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='silver' AND table_name='stg_listings') THEN
    ALTER TABLE public.stg_listings SET SCHEMA silver;
  END IF;
END $$;

-- Gold: dimension + fact tables
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'dim_date', 'dim_city', 'dim_neighbourhood', 'dim_host', 'dim_listing',
    'fct_listing_monthly_snapshot', 'fct_neighbourhood_monthly_avg_price',
    'fct_neighbourhood_monthly_top10_price_delta', 'fct_data_compliance_monthly'
  ]
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=t)
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='gold' AND table_name=t) THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA gold', t);
    END IF;
  END LOOP;
END $$;

-- Gold: reporting views
DO $$
DECLARE
  v text;
BEGIN
  FOREACH v IN ARRAY ARRAY[
    'vw_rep_monthly_neighbourhood_avg_price', 'vw_rep_monthly_top10_overpriced',
    'vw_rep_monthly_top10_underpriced', 'vw_rep_monthly_data_compliance'
  ]
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='public' AND table_name=v)
       AND NOT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='gold' AND table_name=v) THEN
      EXECUTE format('ALTER VIEW public.%I SET SCHEMA gold', v);
    END IF;
  END LOOP;
END $$;

-- Metadata: existing business-rule reconciliation tables move as-is
DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'rec_avg_price_comparison', 'rec_top10_price_delta_comparison', 'rec_reporting_view_comparison'
  ]
  LOOP
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=t)
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='metadata' AND table_name=t) THEN
      EXECUTE format('ALTER TABLE public.%I SET SCHEMA metadata', t);
    END IF;
  END LOOP;
END $$;

-- Metadata: etl_run_log -> pipeline_execution_log (rename + move)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='etl_run_log')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='metadata' AND table_name='pipeline_execution_log') THEN
    ALTER TABLE public.etl_run_log RENAME TO pipeline_execution_log;
    ALTER TABLE public.pipeline_execution_log SET SCHEMA metadata;
  END IF;
END $$;

-- Metadata: etl_error_log -> pipeline_error_log (rename + move)
DO $$ BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='etl_error_log')
     AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='metadata' AND table_name='pipeline_error_log') THEN
    ALTER TABLE public.etl_error_log RENAME TO pipeline_error_log;
    ALTER TABLE public.pipeline_error_log SET SCHEMA metadata;
  END IF;
END $$;

-- Verification: run this SEPARATELY afterwards to sanity-check row counts.
-- If a table below doesn't exist, it means there was nothing to migrate
-- for it (e.g. a fresh database) -- that's fine, the app will create it
-- empty on first connect.
--
-- SELECT 'bronze.raw_listings' AS tbl, count(*) FROM bronze.raw_listings
-- UNION ALL SELECT 'silver.stg_listings', count(*) FROM silver.stg_listings
-- UNION ALL SELECT 'gold.dim_city', count(*) FROM gold.dim_city
-- UNION ALL SELECT 'gold.fct_listing_monthly_snapshot', count(*) FROM gold.fct_listing_monthly_snapshot
-- UNION ALL SELECT 'metadata.pipeline_execution_log', count(*) FROM metadata.pipeline_execution_log;
