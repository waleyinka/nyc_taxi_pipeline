/*
===============================================================================
Create Schemas
===============================================================================
Schemas:
    - bronze → for raw datasets (untransformed).
    - silver → for cleaned and standardized datasets.
    - gold → for aggregated and analytics-ready datasets.
    - etl_metadata → for tracking metadata e.g load history, status, timestamps etc.
===============================================================================
*/

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;


-- ===============================================================================
-- Create Bronze layer and metadata tables
-- ===============================================================================

CREATE TABLE bronze.yellow_taxi_trips ( 
    VendorID BIGINT, 
	tpep_pickup_datetime TIMESTAMP, 
	tpep_dropoff_datetime TIMESTAMP,
	passenger_count FLOAT(53), 
	trip_distance FLOAT(53),
	RatecodeID FLOAT(53),
	store_and_fwd_flag TEXT,
	PULocationID BIGINT, 
	DOLocationID BIGINT,
	payment_type FLOAT(53),
	fare_amount FLOAT(53),
	extra FLOAT(53),
	mta_tax FLOAT(53),
	tip_amount FLOAT(53), 
	tolls_amount FLOAT(53),
	improvement_surcharge FLOAT(53), 
	total_amount FLOAT(53),
	congestion_surcharge FLOAT(53),
	airport_fee FLOAT(53)
);


CREATE TABLE metadata.pipeline_log (
    id SERIAL PRIMARY KEY,
    file_name TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    load_duration NUMERIC,
    row_count BIGINT,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

	


-- ===============================================================================
-- Stored Procedure: Load Bronze Layer (Source -> Bronze)
-- ===============================================================================


CREATE OR REPLACE PROCEDURE bronze.load_bronze(file_path TEXT, run_type TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    load_duration NUMERIC;
    rows_count BIGINT;
BEGIN
    start_time := NOW();

    RAISE NOTICE 'Loading file: %', file_path;
	
	-- Truncate table if FULL run
    IF run_type = 'FULL' THEN
        TRUNCATE TABLE bronze.yellow_taxi_trips;
    END IF;

    EXECUTE format(
        'COPY bronze.yellow_taxi_trips FROM %L WITH (FORMAT CSV, HEADER TRUE)',
        file_path
    );


    GET DIAGNOSTICS rows_count = ROW_COUNT;


    end_time := NOW();
    load_duration := EXTRACT(EPOCH FROM (end_time - start_time));


	-- Log success
    INSERT INTO metadata.pipeline_log (file_name, start_time, end_time, load_duration, row_count, status, error_message, created_at
    )
    VALUES (
		file_path,
		start_time,
		end_time,
		load_duration,
		rows_count,
		'SUCCESS',
		NULL,
		NOW()
	);

    RAISE NOTICE 'File % loaded successfully (% rows)', file_path, rows_count;

EXCEPTION WHEN OTHERS THEN
	
	--Log failure
	INSERT INTO metadata.pipeline_log (file_name, start_time, end_time, load_duration, row_count, status, error_message, created_at
    )
    VALUES (
		file_path,
		start_time,
		NOW(),
		NULL,
		NULL,
		'FAILED',
		SQLERRM,
		NOW()
	);
    
	RAISE NOTICE 'Error loading %: %', file_path, SQLERRM;
END;
$$;


-- Call load_bronze stored procedure
CALL bronze.load_bronze('C:\\Users\\Admin\\Downloads\\nyc_taxi_data_2024\\yellow_tripdata_2024-01.csv', 'FULL');


-- test
SELECT * FROM metadata.pipeline_log ORDER BY id DESC;

SELECT * FROM bronze.yellow_taxi_trips;