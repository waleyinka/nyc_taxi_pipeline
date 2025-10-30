/*===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
      Run this script to re-define the DDL structure of 'silver' Tables         
===============================================================================
*/





CREATE OR REPLACE PROCEDURE public.transform_to_silver_full_2024()
LANGUAGE plpgsql
AS $$
DECLARE
    v_row_count BIGINT := 0;
    v_sql TEXT := '';
    v_rec RECORD;
BEGIN
    -- metadata
    INSERT INTO public.load_metadata(file_name, layer, status, start_time)
    VALUES ('silver_transform_2024_full', 'silver', 'IN_PROGRESS', NOW())
    ON CONFLICT (file_name) DO UPDATE SET status='IN_PROGRESS', start_time=NOW();

    DROP TABLE IF EXISTS silver.yellow_tripdata_2024 CASCADE;

    -- build union from all bronze monthly tables
    v_sql := 'CREATE TABLE silver.yellow_tripdata_2024 AS SELECT * FROM (';
    FOR v_rec IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='bronze' AND table_name LIKE 'yellow_tripdata_2024_%'
        ORDER BY table_name
    LOOP
        v_sql := v_sql || format('SELECT * FROM bronze.%I UNION ALL ', v_rec.table_name);
    END LOOP;
    v_sql := left(v_sql, length(v_sql) - 11) || ') as unioned;';

    EXECUTE v_sql;

    -- standardize columns: add derived columns if missing
    ALTER TABLE silver.yellow_tripdata_2024
    ADD COLUMN IF NOT EXISTS trip_duration_minutes NUMERIC,
    ADD COLUMN IF NOT EXISTS avg_speed_mph NUMERIC,
    ADD COLUMN IF NOT EXISTS fare_per_mile NUMERIC,
    ADD COLUMN IF NOT EXISTS tip_pct NUMERIC,
    ADD COLUMN IF NOT EXISTS trip_hash TEXT;

    -- compute derived fields
    UPDATE silver.yellow_tripdata_2024
    SET
        trip_duration_minutes = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60.0,
        avg_speed_mph = CASE WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0
                             AND trip_distance > 0
                             THEN (trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600.0))
                             ELSE NULL END,
        fare_per_mile = CASE WHEN trip_distance > 0 THEN fare_amount / trip_distance ELSE NULL END,
        tip_pct = CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100.0 ELSE NULL END,
        trip_hash = md5(
            COALESCE(vendorid::TEXT,'') || '|' ||
            COALESCE(tpep_pickup_datetime::TEXT,'') || '|' ||
            COALESCE(tpep_dropoff_datetime::TEXT,'') || '|' ||
            COALESCE(pulocationid::TEXT,'') || '|' ||
            COALESCE(dolocationid::TEXT,'') || '|' ||
            COALESCE(passenger_count::TEXT,'') || '|' ||
            COALESCE(total_amount::TEXT,'')
        );

    -- unit checks: fail if too many null pickups or if table empty
    SELECT COUNT(*) FILTER (WHERE tpep_pickup_datetime IS NULL) INTO v_row_count FROM silver.yellow_tripdata_2024;
    IF v_row_count > 10 THEN
        UPDATE public.load_metadata SET status='FAILED', end_time=NOW(), remarks='Too many NULL pickup datetimes in silver' WHERE file_name='silver_transform_2024_full';
        RAISE EXCEPTION 'Unit check failed: % NULL pickup datetimes', v_row_count;
    END IF;

    SELECT COUNT(*) INTO v_row_count FROM silver.yellow_tripdata_2024;
    IF v_row_count = 0 THEN
        UPDATE public.load_metadata SET status='FAILED', end_time=NOW(), remarks='Silver table empty after transform' WHERE file_name='silver_transform_2024_full';
        RAISE EXCEPTION 'Silver transform produced zero rows';
    END IF;

    -- deduplicate using trip_hash
    CREATE TEMP TABLE silver._tmp_dedup AS
    SELECT DISTINCT ON (trip_hash) * FROM silver.yellow_tripdata_2024 ORDER BY trip_hash, tpep_pickup_datetime;

    DROP TABLE silver.yellow_tripdata_2024;
    CREATE TABLE silver.yellow_tripdata_2024 AS SELECT * FROM silver._tmp_dedup;
    DROP TABLE silver._tmp_dedup;

    -- basic data quality filters
    DELETE FROM silver.yellow_tripdata_2024 WHERE trip_distance <= 0 OR fare_amount <= 0 OR EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) <= 0;

    -- index
    CREATE INDEX IF NOT EXISTS idx_silver_pickup ON silver.yellow_tripdata_2024(tpep_pickup_datetime);
    CREATE INDEX IF NOT EXISTS idx_silver_trip_hash ON silver.yellow_tripdata_2024(trip_hash);

    SELECT COUNT(*) INTO v_row_count FROM silver.yellow_tripdata_2024;

    UPDATE public.load_metadata AS lm
       SET status='SUCCESS', end_time=NOW(), rows_loaded=v_row_count
     WHERE lm.file_name = 'silver_transform_2024_full';
EXCEPTION WHEN OTHERS THEN
    UPDATE public.load_metadata AS lm
       SET status='FAILED', end_time=NOW(), remarks=SQLERRM
     WHERE lm.file_name = 'silver_transform_2024_full';
    RAISE;
END;
$$;


CALL public.transform_to_silver_full_2024();






CREATE OR REPLACE PROCEDURE public.transform_to_silver_incremental_2024()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec RECORD;
    v_count BIGINT;
    v_inserted BIGINT := 0;
    v_total_after BIGINT := 0;
BEGIN
    INSERT INTO public.load_metadata(file_name, layer, status, start_time)
    VALUES ('silver_transform_2024_incremental', 'silver', 'IN_PROGRESS', NOW())
    ON CONFLICT (file_name) DO UPDATE SET status='IN_PROGRESS', start_time=NOW();

    -- ensure silver table exists (create empty with canonical schema if not)
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='silver' AND table_name='yellow_tripdata_2024') THEN
        CREATE TABLE silver.yellow_tripdata_2024 (
            vendorid INT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance NUMERIC(12,3),
            ratecodeid INT,
            store_and_fwd_flag TEXT,
            pulocationid INT,
            dolocationid INT,
            payment_type INT,
            fare_amount NUMERIC(12,2),
            extra NUMERIC(12,2),
            mta_tax NUMERIC(12,2),
            tip_amount NUMERIC(12,2),
            tolls_amount NUMERIC(12,2),
            improvement_surcharge NUMERIC(12,2),
            total_amount NUMERIC(12,2),
            congestion_surcharge NUMERIC(12,2),
            trip_duration_minutes NUMERIC,
            avg_speed_mph NUMERIC,
            fare_per_mile NUMERIC,
            tip_pct NUMERIC,
            trip_hash TEXT
        );
    END IF;

    FOR v_rec IN
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='bronze' AND table_name LIKE 'yellow_tripdata_2024_%'
        ORDER BY table_name
    LOOP
        -- skip if already processed
        PERFORM 1 FROM public.silver_backfill WHERE bronze_table = v_rec.table_name;
        IF FOUND THEN
            CONTINUE;
        END IF;

        -- insert data from bronze.<table> into silver
        EXECUTE format('INSERT INTO silver.yellow_tripdata_2024 (vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge) SELECT vendorid, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, ratecodeid, store_and_fwd_flag, pulocationid, dolocationid, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge FROM bronze.%I;', v_rec.table_name);

        -- compute derived fields for newly inserted rows by matching recent rows via trip_hash
        UPDATE silver.yellow_tripdata_2024
        SET
            trip_duration_minutes = EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/60.0,
            avg_speed_mph = CASE WHEN EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) > 0 AND trip_distance > 0 THEN (trip_distance / (EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime))/3600.0)) ELSE NULL END,
            fare_per_mile = CASE WHEN trip_distance > 0 THEN fare_amount / trip_distance ELSE NULL END,
            tip_pct = CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100.0 ELSE NULL END,
            trip_hash = md5(
                COALESCE(vendorid::TEXT,'') || '|' ||
                COALESCE(tpep_pickup_datetime::TEXT,'') || '|' ||
                COALESCE(tpep_dropoff_datetime::TEXT,'') || '|' ||
                COALESCE(pulocationid::TEXT,'') || '|' ||
                COALESCE(dolocationid::TEXT,'') || '|' ||
                COALESCE(passenger_count::TEXT,'') || '|' ||
                COALESCE(total_amount::TEXT,'')
            )
        WHERE trip_hash IS NULL;

        -- dedupe within silver: delete duplicates keeping earliest pickup
        CREATE TEMP TABLE silver._tmp_new AS
        SELECT DISTINCT ON (trip_hash) *
        FROM silver.yellow_tripdata_2024
        ORDER BY trip_hash, tpep_pickup_datetime;

        TRUNCATE silver.yellow_tripdata_2024;
        INSERT INTO silver.yellow_tripdata_2024 SELECT * FROM silver._tmp_new;
        DROP TABLE silver._tmp_new;

        -- basic checks for this bronze table's contribution
        EXECUTE format('SELECT COUNT(*) FROM bronze.%I;', v_rec.table_name) INTO v_count;
        IF v_count = 0 THEN
            UPDATE public.load_metadata SET status='FAILED', end_time=NOW(), remarks = format('Bronze table %s empty', v_rec.table_name) WHERE file_name='silver_transform_2024_incremental';
            RAISE EXCEPTION 'Bronze table %s is empty', v_rec.table_name;
        END IF;

        -- mark processed
        INSERT INTO public.silver_backfill(bronze_table, processed_at) VALUES (v_rec.table_name, NOW());

        v_inserted := v_inserted + v_count;
    END LOOP;

    SELECT COUNT(*) INTO v_total_after FROM silver.yellow_tripdata_2024;

    -- unit checks
    IF v_total_after = 0 THEN
        UPDATE public.load_metadata SET status='FAILED', end_time=NOW(), remarks='Silver empty after incremental load' WHERE file_name='silver_transform_2024_incremental';
        RAISE EXCEPTION 'Silver empty after incremental process';
    END IF;

    UPDATE public.load_metadata SET status='SUCCESS', end_time=NOW(), rows_loaded=v_total_after WHERE file_name='silver_transform_2024_incremental';
EXCEPTION WHEN OTHERS THEN
    UPDATE public.load_metadata SET status='FAILED', end_time=NOW(), remarks = SQLERRM WHERE file_name='silver_transform_2024_incremental';
    RAISE;
END;
$$;


CALL public.transform_to_silver_incremental_2024();





CREATE TABLE silver.yellow_taxi_trips_cleaned AS
SELECT
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    (tip_amount / NULLIF(fare_amount, 0)) AS tip_rate,
    payment_type
FROM bronze.yellow_taxi_trips
WHERE total_amount > 0
  AND passenger_count > 0
  AND trip_distance > 0;