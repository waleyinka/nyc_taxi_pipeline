/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


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
