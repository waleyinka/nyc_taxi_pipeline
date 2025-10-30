-- ===============================================================================
-- Create Gold Layer Tables with Aggregated Metrics
-- ===============================================================================


/*================================================================================
DDL Script: Create Gold Tables
===================================================================================
Script Purpose:
    This script creates tables in the 'gold' schema, dropping existing tables 
    if they already exist.
      Run this script to re-define the DDL structure of 'gold' Tables         
===================================================================================
*/
CREATE VIEW  gold.monthly_revenue_summary AS
SELECT
    DATE_TRUNC('month', tpep_pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_distance,
    AVG(tip_rate) AS avg_tip_rate
FROM silver.yellow_taxi_trips_cleaned
GROUP BY 1
ORDER BY 1;