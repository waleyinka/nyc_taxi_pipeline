/*================================================================================
DDL Script: Bronze Layer Setup and Incremental Load
===================================================================================
Script Purpose:
    This script sets up the 'bronze' schema and implements an incremental load
    procedure to load CSV files into bronze tables. It also creates a consolidated
    view for the full year 2024 data.
      Run this script to set up the Bronze layer and perform incremental loads <<
===================================================================================
*/



-- ===============================================================================
-- A. Create Metadata Table: To track pipeline activity and per-table lineage
-- ===============================================================================
CREATE TABLE IF NOT EXISTS public.load_metadata (
    id SERIAL PRIMARY KEY,
    file_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    layer TEXT NOT NULL DEFAULT 'bronze',      -- e.g. bronze, silver, gold
    status TEXT CHECK (status IN ('PENDING','IN_PROGRESS','SUCCESS','FAILED')),
    rows_loaded BIGINT DEFAULT 0,
    start_time TIMESTAMP DEFAULT NOW(),
    end_time TIMESTAMP,
    last_watermark TIMESTAMP,
    remarks TEXT,
    UNIQUE(file_name, layer)
);



-- ===============================================================================
-- B. Incremental load (load CSV from source → Bronze layer + record metadata),
-- ===============================================================================
CREATE OR REPLACE PROCEDURE public.load_bronze_data_incremental(
    p_file_name TEXT,
    p_file_path TEXT,
    p_force BOOLEAN DEFAULT NULL
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name TEXT := replace(p_file_name, '.csv', '');
    v_row_count BIGINT := 0;
    v_exists BOOLEAN;
    v_last_watermark TIMESTAMP;     -- dropoff time to track last load time
    v_new_max TIMESTAMP;       -- new max dropoff time after load
    v_load_mode TEXT;               -- 'FULL' or 'INCREMENTAL'
BEGIN
    -- check if table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='bronze' AND table_name = v_table_name
    ) INTO v_exists;

    -- fetch last successful watermark if exists
    SELECT last_watermark
    INTO v_last_watermark
    FROM public.load_metadata
    WHERE file_name = p_file_name AND layer='bronze' AND status='SUCCESS'
    ORDER BY end_time DESC
    LIMIT 1;

    -- determine load mode and log start
    IF p_force IS TRUE THEN
        v_load_mode := 'FULL';
    ELSIF p_force IS FALSE THEN
        v_load_mode := 'INCREMENTAL';
    ELSE
        IF NOT v_exists OR v_last_watermark IS NULL THEN
            v_load_mode := 'FULL';
        ELSE
            v_load_mode := 'INCREMENTAL';
        END IF;
    END IF;

    RAISE NOTICE '===================================================';
    RAISE NOTICE '>> Starting % load for table: %', v_load_mode, v_table_name;
    RAISE NOTICE '===================================================';

    -- log start of load in metadata table
    INSERT INTO public.load_metadata(file_name, layer, status, start_time)
    VALUES (p_file_name, 'bronze', 'IN_PROGRESS', NOW())
    ON CONFLICT (file_name, layer)
        DO UPDATE SET status='IN_PROGRESS', start_time=NOW();

    -- create table if not exists
    IF NOT v_exists THEN
        RAISE NOTICE '>> Table does not exist. Creating bronze.%...', v_table_name;
        EXECUTE format($fmt$
            CREATE TABLE bronze.%I (
                    vendor_id TEXT,
    				tpep_pickup_datetime TIMESTAMP,
    				tpep_dropoff_datetime TIMESTAMP,
    				passenger_count FLOAT,
    				trip_distance FLOAT,
    				rate_code_id FLOAT,
   					store_and_fwd_flag TEXT,
    				pu_location_id INTEGER,
    				do_location_id INTEGER,
    				payment_type INTEGER,
    				fare_amount FLOAT,
    				extra FLOAT,
    				mta_tax FLOAT,
    				tip_amount FLOAT,
    				tolls_amount FLOAT,
    				improvement_surcharge FLOAT,
    				total_amount FLOAT,
    				congestion_surcharge FLOAT,
					airport_fee FLOAT
            );
        $fmt$, v_table_name);
    END IF;

    -- determine load mode: FULL or INCREMENTAL
    IF v_load_mode LIKE 'FULL%' THEN
        RAISE NOTICE '>> Performing FULL load...';
        EXECUTE format('TRUNCATE TABLE bronze.%I;', v_table_name);
        EXECUTE format('COPY bronze.%I FROM %L DELIMITER '','' CSV HEADER;', v_table_name, p_file_path);

    ELSE
        RAISE NOTICE '>> Performing INCREMENTAL load using watermark: %', v_last_watermark;

        -- create a temporary table for the new data
        EXECUTE format('CREATE TEMP TABLE tmp_%I AS TABLE bronze.%I WITH NO DATA;', v_table_name, v_table_name);
        EXECUTE format('COPY tmp_%I FROM %L DELIMITER '','' CSV HEADER;', v_table_name, p_file_path);

        -- insert only new records beyond the last watermark
        EXECUTE format($fmt$
            INSERT INTO bronze.%I
            SELECT * FROM tmp_%I
            WHERE tpep_dropoff_datetime > %L;
        $fmt$, v_table_name, v_table_name, v_last_watermark);

        EXECUTE format('DROP TABLE tmp_%I;', v_table_name);
    END IF;

    -- count rows loaded and get new watermark
    EXECUTE format('SELECT COUNT(*) FROM bronze.%I;', v_table_name) INTO v_row_count;
    EXECUTE format('SELECT MAX(tpep_dropoff_datetime) FROM bronze.%I;', v_table_name) INTO v_las_watermark;

    -- update metadata log on success
    UPDATE public.load_metadata
       SET status='SUCCESS',
           end_time=NOW(),
           rows_loaded=v_row_count,
           last_watermark=v_last_watermark
     WHERE file_name = p_file_name
       AND layer='bronze';

    RAISE NOTICE '>> Load complete. Rows in table: %', v_row_count;
 
EXCEPTION WHEN OTHERS THEN
    -- update metadata log on failure
    UPDATE public.load_metadata
       SET status='FAILED', end_time=NOW(), remarks=SQLERRM
     WHERE file_name = p_file_name;
    RAISE;
END;
$$;


-- ====================================================================================
-- C. Call the incremental load procedure for each monthly CSV file for 2024 data
-- ====================================================================================
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-01.csv','/data_pipeline_files/yellow_tripdata_2024-01.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-02.csv','/data_pipeline_files/yellow_tripdata_2024-02.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-03.csv','/data_pipeline_files/yellow_tripdata_2024-03.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-04.csv','/data_pipeline_files/yellow_tripdata_2024-04.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-05.csv','/data_pipeline_files/yellow_tripdata_2024-05.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-06.csv','/data_pipeline_files/yellow_tripdata_2024-06.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-07.csv','/data_pipeline_files/yellow_tripdata_2024-07.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-08.csv','/data_pipeline_files/yellow_tripdata_2024-08.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-09.csv','/data_pipeline_files/yellow_tripdata_2024-09.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-10.csv','/data_pipeline_files/yellow_tripdata_2024-10.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-11.csv','/data_pipeline_files/yellow_tripdata_2024-11.csv',false);
CALL public.load_bronze_data_incremental('yellow_tripdata_2024-12.csv','/data_pipeline_files/yellow_tripdata_2024-12.csv',false);



-- ====================================================================================
-- D. Test: Validate data load in Bronze layer and metadata logging
-- ====================================================================================
-- Check row counts in individual monthly tables
SELECT COUNT(*) FROM bronze."yellow_tripdata_2024-01";
SELECT COUNT(*) FROM bronze."yellow_tripdata_2024-02";
SELECT COUNT(*) FROM bronze."yellow_tripdata_2024-03";

-- Sample data check
SELECT * FROM bronze."yellow_tripdata_2024-02"
LIMIT 100;

-- Check load metadata log
SELECT * FROM public.load_metadata;



-- ====================================================================================
-- E. Create a consolidated view that unions all monthly tables for the full year 2024 data in Bronze layer
-- ====================================================================================
CREATE OR REPLACE VIEW bronze.yellow_taxi_2024 AS
SELECT * FROM bronze."yellow_tripdata_2024-01"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-02"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-03"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-04"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-05"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-06"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-07"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-08"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-09"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-10"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-11"
UNION ALL
SELECT * FROM bronze."yellow_tripdata_2024-12";

-- Test
SELECT COUNT(*) FROM bronze.yellow_taxi_2024;

SELECT * FROM bronze.yellow_taxi_2024 LIMIT 100;

SELECT * FROM bronze.yellow_taxi_2024 
WHERE tpep_pickup_datetime >= '2024-12-01 00:00:00'
  AND tpep_pickup_datetime < '2024-12-02 00:00:00'
LIMIT 100;