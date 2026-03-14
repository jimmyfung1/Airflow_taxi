import pandas as pd
import logging

def transform_taxi_data(**context):

    ti = context["ti"]

    # 1. Pull cleaned CSV path from XCom
    clean_path = ti.xcom_pull(
        key="clean_path",
        task_ids="clean_taxi_data"
    )

    logging.info(f"Loading cleaned data from: {clean_path}")

    # 2. Load data
    df = pd.read_csv(clean_path, low_memory=False)

    # Ensure datetime columns are parsed
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce"
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], errors="coerce"
    )

    # Derived features
    df["trip_duration_minutes"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60
    )

    df["speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60)

    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"]

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour

    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek

    df["is_weekend"] = df["pickup_day_of_week"] >= 5

    # 3. Filter unrealistic trips
    before = len(df)

    df = df[
        (df["speed_mph"] <= 80) &
        (df["trip_duration_minutes"] >= 1)
    ]

    removed_rows = before - len(df)

    logging.info(f"Removed {removed_rows} rows due to unrealistic speed or duration")
    logging.info(f"Remaining rows after transform: {len(df)}")

    # 4. Save transformed data
    transformed_path = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(transformed_path, index=False)

    logging.info(f"Saved transformed data to {transformed_path}")

    # 5. Log descriptive statistics
    stats_cols = [
        "trip_duration_minutes",
        "speed_mph",
        "fare_per_mile",
        "pickup_hour"
    ]

    logging.info("Descriptive statistics for new features:")
    logging.info(df[stats_cols].describe().to_string())

    # Push transformed path to XCom for downstream tasks
    ti.xcom_push(
        key="transformed_path",
        value=transformed_path
    )