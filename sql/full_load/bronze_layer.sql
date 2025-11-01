-- ===============================================================================
-- Create Bronze Layer Table for Yellow Taxi Data
-- ===============================================================================
CREATE TABLE bronze.yellow_taxi(
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


-- ===============================================================================
-- Create Load Metadata Table
-- ===============================================================================
CREATE TABLE IF NOT EXISTS public.load_metadata (
    id SERIAL PRIMARY KEY,
    file_name TEXT,
    layer TEXT,
    status TEXT,
    rows_loaded BIGINT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    remarks TEXT
);


-- ===============================================================================
-- Load data into Bronze layer from Source CSV files in Local Directory
-- ===============================================================================  
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-01.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-02.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-03.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-04.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files /yellow_tripdata_2024-05.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-06.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-07.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-08.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-09.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-10.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-11.csv' DELIMITER ',' CSV HEADER;
\copy bronze.yellow_taxi FROM '/data_pipeline_files/yellow_tripdata_2024-12.csv' DELIMITER ',' CSV HEADER;
-- Log successful load


-- ===============================================================================
-- Insert Load Metadata
-- ===============================================================================
INSERT INTO public.load_metadata (
    file_name,
    layer,
    status,
    rows_loaded,
    start_time,
    end_time,
    remarks
)
SELECT 
    table_name || '.csv' AS file_name,
    'bronze' AS layer,
    'SUCCESS' AS status,
    (SELECT COUNT(*) FROM bronze.yellow_taxi_2024) AS rows_loaded,
    NOW() AS start_time,
    NOW() AS end_time,
    'Manual load via \copy' AS remarks
FROM information_schema.tables
WHERE table_schema = 'bronze';



-- ====================================================================================
-- ALTERNATIVE: Using Stored Procedure for Full Load into Bronze Layer (Full Refresh)
-- ====================================================================================
/*
CREATE OR REPLACE PROCEDURE public.load_bronze_data_full(p_file_name TEXT, p_file_path TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_table_name TEXT := replace(p_file_name, '.csv', '');
    v_row_count BIGINT := 0;
BEGIN
    INSERT INTO public.load_metadata(file_name, layer, status, start_time)
    VALUES (p_file_name, 'bronze', 'IN_PROGRESS', NOW())
    ON CONFLICT (file_name) DO UPDATE SET status='IN_PROGRESS', start_time=NOW();

    -- drop and recreate (full load)
    EXECUTE format('DROP TABLE IF EXISTS bronze.%I CASCADE;', v_table_name);

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

    -- server-side COPY
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
*/