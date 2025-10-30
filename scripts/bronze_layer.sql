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
-- A. Create Schemas
-- ===============================================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ===============================================================================
-- B. Create Metadata Table: To track pipeline activity and per-table lineage
-- ===============================================================================
CREATE TABLE IF NOT EXISTS public.load_metadata (
    id SERIAL PRIMARY KEY,
    file_name TEXT UNIQUE,
    layer TEXT,                         -- e.g. bronze, silver, gold
    status TEXT CHECK (status IN ('PENDING','IN_PROGRESS','SUCCESS','FAILED')),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_loaded BIGINT,
    remarks TEXT
);



-- ===============================================================================
-- C. Incremental load (load CSV from source → Bronze layer + record metadata),
-- ===============================================================================
CREATE OR REPLACE PROCEDURE public.load_bronze_data_incremental(p_file_name TEXT, p_file_path TEXT, p_force BOOLEAN DEFAULT FALSE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name TEXT := replace(p_file_name, '.csv', '');
    v_row_count BIGINT := 0;
    v_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='bronze' AND table_name = v_table_name
    ) INTO v_exists;

    INSERT INTO public.load_metadata(file_name, layer, status, start_time)
    VALUES (p_file_name, 'bronze', 'IN_PROGRESS', NOW())
    ON CONFLICT (file_name) DO UPDATE SET status='IN_PROGRESS', start_time=NOW();

    IF NOT v_exists THEN
        -- create table if missing
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
    ELSIF p_force THEN
        -- if force true, truncate then load
        EXECUTE format('TRUNCATE TABLE bronze.%I;', v_table_name);
    END IF;

    -- load data
    EXECUTE format('COPY bronze.%I FROM %L DELIMITER '','' CSV HEADER;', v_table_name, p_file_path);

    EXECUTE format('SELECT COUNT(*) FROM bronze.%I;', v_table_name) INTO v_row_count;

    UPDATE public.load_metadata AS lm
       SET status='SUCCESS', end_time=NOW(), rows_loaded=v_row_count
     WHERE lm.file_name = p_file_name;

EXCEPTION WHEN OTHERS THEN
    UPDATE public.load_metadata AS lm
       SET status='FAILED', end_time=NOW(), remarks=SQLERRM
     WHERE lm.file_name = p_file_name;
    RAISE;
END;
$$;



-- Call load_bronze_data_incremental
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



-- Test
SELECT COUNT(*) FROM bronze."yellow_tripdata_2024-01";
SELECT COUNT(*) FROM bronze."yellow_tripdata_2024-02";
SELECT COUNT(*) FROM bronze."yellow_tripdata_2024-03";

SELECT * FROM bronze."yellow_tripdata_2024-02"
LIMIT 100;

SELECT * FROM public.load_metadata;



-- ====================================================================================
-- D. Create a consolidated view that unions all monthly tables for the full year 2024 data in Bronze layer
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