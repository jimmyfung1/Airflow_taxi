import pandas as pd
import logging

def clean_taxi_data(**context):

    ti = context["ti"]

    # 1. Pull raw file path from XCom
    raw_path = ti.xcom_pull(
        key="raw_path",
        task_ids="ingest_taxi_data"
    )

    logging.info(f"Loading raw file: {raw_path}")

    # 2. Load only required columns (performance optimization)
    cols = [
        "fare_amount",
        "trip_distance",
        "tpep_pickup_datetime",
        "pickup_latitude",
        "pickup_longitude"
    ]

    df = pd.read_csv(raw_path, usecols=cols, low_memory=False)

    original_rows = len(df)
    logging.info(f"Initial rows: {original_rows}")

    # 3. Parse datetime first
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce"
    )

    # 4. Create filtering masks (vectorized)
    fare_mask = (df["fare_amount"] > 0) & (df["fare_amount"] <= 500)
    distance_mask = (df["trip_distance"] > 0) & (df["trip_distance"] <= 100)

    coord_mask = (
        df["pickup_latitude"].between(40.4, 41.0) &
        df["pickup_longitude"].between(-74.3, -73.5)
    )

    not_null_mask = df[
        ["fare_amount", "trip_distance", "tpep_pickup_datetime"]
    ].notnull().all(axis=1)

    # Combine masks
    valid_mask = fare_mask & distance_mask & coord_mask & not_null_mask

    cleaned_df = df[valid_mask]

    removed_rows = original_rows - len(cleaned_df)

    logging.info(f"Rows removed during cleaning: {removed_rows}")
    logging.info(f"Final cleaned rows: {len(cleaned_df)}")

    # 5. Save cleaned data
    clean_path = "/tmp/nyc_taxi_clean.csv"
    cleaned_df.to_csv(clean_path, index=False)

    logging.info(f"Saved cleaned data to {clean_path}")

    # 6. Push cleaned file path to XCom
    ti.xcom_push(
        key="clean_path",
        value=clean_path
    )