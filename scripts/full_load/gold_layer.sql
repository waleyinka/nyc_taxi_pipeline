/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data to produce a clean,
    enriched, and business-ready dataset by aggregating and summarizing
    data from the silver layer. 

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

--- Fact Tables
CREATE VIEW gold.fact_trips AS
SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_distance,
    SUM(total_amount) AS total_revenue
FROM silver.yellow_taxi_cleaned
GROUP BY 1;


--- Dimension Tables
CREATE VIEW gold.dim_vendor AS
SELECT DISTINCT vendor_id FROM silver.yellow_taxi_cleaned;


--- Additional Gold Layer Tables

CREATE VIEW gold.dim_payment_type AS
SELECT DISTINCT
    payment_type,
    CASE
        WHEN payment_type = 1 THEN 'Credit Card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No Charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided Trip'
        ELSE 'Other'
    END AS payment_type_desc
FROM silver.yellow_taxi_cleaned;


CREATE VIEW gold.daily_tip_summary AS
SELECT
    DATE_TRUNC('day', pickup_datetime) AS trip_date,
    SUM(tip_amount) AS total_tips,
    AVG(tip_amount) AS avg_tip_amount,
    AVG(tip_amount / NULLIF(fare_amount, 0)) AS avg_tip_rate
FROM silver.yellow_taxi_cleaned
GROUP BY 1;


CREATE VIEW gold.location_summary AS
SELECT
    pu_location_id,
    do_location_id,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance
FROM silver.yellow_taxi_cleaned
GROUP BY pu_location_id, do_location_id;





/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


CREATE TABLE gold.monthly_revenue_summary AS
SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance,
    AVG(tip_rate) AS avg_tip_rate
FROM silver.yellow_taxi_trips_cleaned
GROUP BY 1
ORDER BY 1;