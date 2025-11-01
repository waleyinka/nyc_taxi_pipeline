#DATA ENGINEERING CHALLENGE - NYC TAXI DATA PIPELINE

## Project Overview 

This project demonstrates how to design and implement a complete SQL-based data pipeline using PostgreSQL and NYC Taxi Data (2024).

The goal was to build an idempotent, metadata-driven, and incremental ETL pipeline — one that can:

 - Ingest raw monthly data (Jan–Dec 2024)

 - Transform it into cleaned, standardized tables

 - Aggregate it into business-ready metrics

 - Track lineage and load history dynamically

The result is a self-aware SQL pipeline that “remembers” where it left off, achieved through watermark-based incremental logic and metadata management.


## Arhitecture Overview

This pipeline follows the Medallion Architecture pattern to ensure clear data lineage and modular transformation.

``` bash
📦 Source (NYC Taxi CSVs)
      ↓
🥉 Bronze → Raw, unaltered ingestion
      ↓
🥈 Silver → Cleaned, validated, standardized
      ↓
🥇 Gold → Aggregated, analytics-ready metrics
```

Each layer represents a transformation stage:

| Layer      | Purpose                    | Key Processes                                              |
| ---------- | -------------------------- | ---------------------------------------------------------- |
| **Bronze** | Landing zone for raw data  | File ingestion, metadata logging, watermark tracking       |
| **Silver** | Cleansed and standardized  | Filtering invalid records, deduplication, schema alignment |
| **Gold**   | Aggregated, business-ready | Monthly revenue, trip count, fare trends, vendor insights  |


## Pipeline Flow

**Step 1: Data Source**

The dataset used is the Yellow Taxi Trip Data (2024) from the NYC TLC Open Data portal
.
Each month’s data file is available as a .csv or .parquet file.

Example filenames:

```bash
yellow_tripdata_2024-01.csv
yellow_tripdata_2024-02.csv
...
yellow_tripdata_2024-12.csv
```

**Step 2: Ingestion (Bronze Layer)**

All ingestion is handled through a single stored procedure —
`public.load_bronze_data_incremental(p_file_name, p_file_path, p_force)`

This procedure dynamically creates Bronze tables, loads new files, and tracks the progress in the metadata layer.

Key Features:

 - Handles both full and incremental loads

 - Auto-creates tables if missing

 - Tracks row counts, status, and watermarks

 - Supports re-runs without duplicates (idempotent)

Procedure Signature:

``` SQL

CALL public.load_bronze_data_incremental(
    'yellow_tripdata_2024-03.csv',
    '/data/yellow_tripdata_2024-03.csv'
);

```

Core Logic:

| Parameter     | Description                                                                        |
| ------------- | ---------------------------------------------------------------------------------- |
| `p_file_name` | Name of the CSV file (used for metadata tracking)                                  |
| `p_file_path` | Local path to the file                                                             |
| `p_force`     | Optional mode selector: `TRUE` = FULL, `FALSE` = INCREMENTAL, `NULL` = Auto-detect |

The procedure intelligently decides whether to perform a FULL or INCREMENTAL load based on the last successful run and the presence of a watermark.

**Step 2: Transformation (Silver Layer)**

The Silver layer is the cleansing stage. Data from Bronze is transformed using SQL scripts that:

 - Remove nulls, duplicates, and negative values

 - Enforce valid timestamps

 - Restrict trips to the year 2024

Example:

``` SQL

CREATE TABLE silver.yellow_taxi_2024 AS
SELECT *
FROM bronze.yellow_taxi_2024
WHERE passenger_count > 0
  AND fare_amount >= 0
  AND trip_distance > 0
  AND EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024;

```

Unit checks run after transformation to ensure:

 - No data loss between Bronze and Silver

 - No duplicate trip_ids

 - Consistent record counts

If any constraint fails, the load process halts automatically.


**Step 4: Aggregation (Gold Layer)**

The Gold layer delivers analytical summaries. This stage aggregates cleaned data into key business metrics such as:

 - Total trips per month

 - Average fare and tip rate

 - Monthly revenue patterns

 - Vendor performance trends

Example:

``` SQL

CREATE TABLE gold.yellow_taxi_metrics AS
SELECT
    DATE_TRUNC('month', pickup_datetime) AS month,
    COUNT(*) AS total_trips,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(tip_amount / fare_amount * 100), 2) AS avg_tip_rate
FROM silver.yellow_taxi_2024
GROUP BY month
ORDER BY month;

```

## Metadata Management

The metadata table (`public.load_metadata`) acts as the pipeline’s “memory.” Each load — full or incremental — is tracked in this table.

| Column           | Type      | Description                                         |
| ---------------- | --------- | --------------------------------------------------- |
| `file_name`      | TEXT      | Name of the ingested file                           |
| `layer`          | TEXT      | Target layer (`bronze`, `silver`, etc.)             |
| `status`         | TEXT      | Current status (`IN_PROGRESS`, `SUCCESS`, `FAILED`) |
| `start_time`     | TIMESTAMP | Load start time                                     |
| `end_time`       | TIMESTAMP | Load completion time                                |
| `rows_loaded`    | BIGINT    | Total rows inserted                                 |
| `last_watermark` | TIMESTAMP | Highest drop-off timestamp from the last run        |
| `remarks`        | TEXT      | Error details (if any)                              |

**Example Query:**

``` SQL

SELECT * FROM public.load_metadata
WHERE layer = 'bronze'
ORDER BY end_time DESC;

```

**Example Output:**

| file_name                   | layer  | status  | rows_loaded | last_watermark      |
| --------------------------- | ------ | ------- | ----------- | ------------------- |
| yellow_tripdata_2024-02.csv | bronze | SUCCESS | 1,234,567   | 2024-02-29 23:59:59 |
| yellow_tripdata_2024-01.csv | bronze | SUCCESS | 1,203,110   | 2024-01-31 23:59:59 |


## Testing & Validation

Each stage includes lightweight validation chceks:

| Layer  | Test                      | Description                                      |
| ------ | ------------------------- | ------------------------------------------------ |
| Bronze | Row count check           | Confirms number of rows loaded = file count      |
| Silver | Null and duplicate checks | Validates schema integrity                       |
| Gold   | Aggregation sanity checks | Ensures monthly metrics align with source totals |


## Folder Structure

nyc-taxi-pipeline/
├── data/                 # Local folder for downloaded CSVs
├── sql/                  # SQL scripts for transformations
│   ├── bronze/           # Ingestion logic
│   ├── silver/           # Cleaning & validation
│   └── gold/             # Aggregations
├── docs/                 # Architecture diagrams & documentation
├── scripts/              # Python automation for download & scheduling
├── README.md             # Project overview
└── requirements.txt      # Python dependencies


