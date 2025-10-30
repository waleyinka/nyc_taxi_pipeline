#DATA ENGINEERING CHALLENGE - NYC TAXI DATA PIPELINE

Currently Ongoing



## 3. Step-by-Step Breakdown

A. Data Acquisition (Python)

Write a simple Python script to loop through months January–December 2024.

For each month:

Construct the file URL dynamically (e.g., https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet)

Download and save to data/raw/yellow_tripdata_2024-01.parquet

Keep metadata (e.g., downloaded file names, sizes, timestamps) in a small metadata.json for audit trail.

💡 This script acts as the “ingestion layer.” It’s light and repeatable — could easily be cron-scheduled.


B. Database Setup

Create a PostgreSQL database, say `nyc_taxi_dw`.
Inside it, create three schemas:

```SQL
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

You can also create a metadata table:

```SQL
CREATE TABLE public.load_metadata (
    file_name TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT
);
```

C. Full Load — Bronze Layer

The Bronze layer is your raw landing zone — minimal intervention.

1. For each dataset:

    - Create a table like bronze.yellow_tripdata_2024_01

    - Use the same structure as the raw file (no transformations, just correct data types)

2. Load via COPY command or pgAdmin import, e.g.:

```SQL
COPY bronze.yellow_tripdata_2024_01
FROM '/path/to/data/raw/yellow_tripdata_2024-01.csv'
DELIMITER ','
CSV HEADER;
```


💡 Bronze tables mimic the original source exactly — this gives you traceability.

Once all 12 files are loaded, you can combine them into a single view:

```SQL
CREATE OR REPLACE VIEW bronze.yellow_tripdata_2024 AS
SELECT * FROM bronze.yellow_tripdata_2024_01
UNION ALL
SELECT * FROM bronze.yellow_tripdata_2024_02
-- etc...
```