#!/usr/bin/python
# coding: utf-8

import os
import requests
import pandas as pd

# Directory to store datasets downloaded from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
storage_dir = "/home/iamomowale/datawarehouse/data"
os.makedirs(storage_dir, exist_ok=True)

# URL for NYC Taxi data (2024-01) is https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
# Base URL for the datasets (to be able to download all datasets by looping through the months)
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

for month in range(1, 13):
    file_name = f"yellow_tripdata_2024-{month:02d}.parquet"
    file_url = f"{base_url}/{file_name}"
    file_path = f"{storage_dir}/{file_name}"

    print(f"Downloading {file_url} ...")
    with requests.get(file_url, stream=True) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    print(f"Saved: {file_path}")

    # Convert to CSV for Postgres ingestion
    df = pd.read_parquet(file_path)
    csv_path = file_path.replace(".parquet", ".csv")
    df.to_csv(csv_path, index=False)
    print(f"Converted to: {csv_path}")

    # To remove the parquet file after conversion
    os.remove(file_path)