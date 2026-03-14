import pandas as pd
import logging

def clean_taxi_data(**context):
    raw_path = context["ti"].xcom_pull(
        task_ids="ingest_taxi_data", key="raw_path"
    )
    df = pd.read_csv(raw_path, parse_dates=["tpep_pickup_datetime"])
    initial = len(df)
    logging.info(f"Starting cleaning with {initial:,} rows")
    
    # Remove anomalous fares
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    logging.info(f"After fare filter: {len(df):,} rows")
    
    # Remove invalid distances
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]
    logging.info(f"After distance filter: {len(df):,} rows")
    
    # NYC bounding box filter
    df = df[
        df["pickup_latitude"].between(40.4, 41.0) &
        df["pickup_longitude"].between(-74.3, -73.5)
    ]
    logging.info(f"After GPS filter: {len(df):,} rows")
    
    # Drop nulls in critical columns
    df = df.dropna(subset=["fare_amount", "trip_distance", "tpep_pickup_datetime"])
    
    removed = initial - len(df)
    logging.info(f"✓ Removed {removed:,} rows ({removed/initial*100:.1f}%). Final: {len(df):,} rows")
    
    output_path = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(output_path, index=False)
    context["ti"].xcom_push(key="clean_path", value=output_path)