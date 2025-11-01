/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- ===============================================================================
-- Create Silver Layer Tables with Data Cleaning
-- ===============================================================================
CREATE TABLE silver.yellow_taxi_cleaned AS
SELECT
    CAST(vendor_id AS INTEGER) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC(10,2)) AS trip_distance,
    payment_type,
    CAST(fare_amount AS NUMERIC(10,2)) AS fare_amount,
    tip_amount,
    CAST(total_amount AS NUMERIC(10,2))AS total_amount
FROM bronze.yellow_taxi_2024
WHERE passenger_count > 0 AND trip_distance > 0;

-- ===============================================================================
-- Additional Silver Layer Transformations
-- Create additional cleaned tables or views as needed
-- ===============================================================================
CREATE TABLE silver.yellow_taxi_enriched AS
SELECT
    yt.*,
    (yt.tip_amount / NULLIF(yt.fare_amount, 0)) AS tip_rate,
    CASE
        WHEN yt.payment_type = 1 THEN 'Credit Card'
        WHEN yt.payment_type = 2 THEN 'Cash'
        WHEN yt.payment_type = 3 THEN 'No Charge'
        WHEN yt.payment_type = 4 THEN 'Dispute'
        WHEN yt.payment_type = 5 THEN 'Unknown'
        WHEN yt.payment_type = 6 THEN 'Voided Trip'
        ELSE 'Other'
    END AS payment_type_desc
FROM silver.yellow_taxi_cleaned yt;

-- ===============================================================================
-- Create Views for Easy Access
-- ===============================================================================
CREATE VIEW silver.vw_yellow_taxi_summary AS
SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance,
    AVG(tip_amount / NULLIF(fare_amount, 0)) AS avg_tip_rate
FROM silver.yellow_taxi_cleaned
GROUP BY 1;

-- ===============================================================================
-- End of Silver Layer Transformations
-- ===============================================================================