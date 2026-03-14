from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import requests
import logging
import csv
import pandas as pd
from sqlalchemy import create_engine

URL = "https://data.cityofnewyork.us/resource/t29m-gskq.csv"
FILE_PATH = "/tmp/nyc_taxi_raw.csv"


# -----------------------------
# DOWNLOAD FUNCTION
# -----------------------------
def download_taxi_data(**context):

    retries = 5

    for attempt in range(retries):

        try:
            logging.info(f"Download attempt {attempt+1}")

            response = requests.get(
                URL,
                stream=True,
                timeout=120
            )

            response.raise_for_status()

            with open(FILE_PATH, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            with open(FILE_PATH, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                row_count = sum(1 for row in reader)

            logging.info(f"Downloaded {row_count} rows")

            context["ti"].xcom_push(
                key="file_path",
                value=FILE_PATH
            )

            return FILE_PATH

        except requests.exceptions.RequestException as e:

            logging.warning(f"Attempt {attempt+1} failed: {e}")

            if attempt == retries - 1:
                raise


# -----------------------------
# CLEAN FUNCTION
# -----------------------------
def clean_taxi_data(**context):

    ti = context["ti"]

    raw_path = ti.xcom_pull(
        key="file_path",
        task_ids="download_taxi_data"
    )

    logging.info(f"Loading file {raw_path}")

    df = pd.read_csv(raw_path)

    original_rows = len(df)
    logging.info(f"Original rows: {original_rows}")

    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]

    df = df.dropna(subset=[
        "fare_amount",
        "trip_distance",
        "tpep_pickup_datetime"
    ])

    clean_path = "/tmp/nyc_taxi_clean.csv"

    df.to_csv(clean_path, index=False)

    logging.info(f"Saved cleaned file to {clean_path}")

    ti.xcom_push(
        key="clean_path",
        value=clean_path
    )


# -----------------------------
# TRANSFORM FUNCTION
# -----------------------------
def transform_taxi_data(**context):

    ti = context["ti"]

    clean_path = ti.xcom_pull(
        key="clean_path",
        task_ids="clean_taxi_data"
    )

    logging.info(f"Loading cleaned file {clean_path}")

    df = pd.read_csv(clean_path)

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    df["speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60)

    df["fare_per_mile"] = df["fare_amount"] / df["trip_distance"]

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["is_weekend"] = df["pickup_day_of_week"] >= 5

    df = df[
        (df["speed_mph"] <= 80) &
        (df["trip_duration_minutes"] >= 1)
    ]

    output_path = "/tmp/nyc_taxi_transformed.csv"

    df.to_csv(output_path, index=False)

    logging.info(f"Saved transformed file to {output_path}")

    stats = df[
        ["trip_duration_minutes", "speed_mph", "fare_per_mile"]
    ].describe()

    logging.info(f"\nStatistics:\n{stats}")

    ti.xcom_push(
        key="transform_path",
        value=output_path
    )


# -----------------------------
# LOAD STAR SCHEMA FUNCTION
# -----------------------------
def load_taxi_model(**context):

    ti = context["ti"]

    transform_path = ti.xcom_pull(
        key="transform_path",
        task_ids="transform_taxi_data"
    )

    df = pd.read_csv(transform_path)

    host = Variable.get("MYSQL_HOST")
    user = Variable.get("MYSQL_USER")
    password = Variable.get("MYSQL_PASS")
    db = Variable.get("MYSQL_DB")

    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@{host}/{db}"
    )

    logging.info("Connected to MySQL")

    # DIM TIME
    dim_time = df[[
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend"
    ]].drop_duplicates().reset_index(drop=True)

    dim_time["time_id"] = dim_time.index + 1

    # DIM PAYMENT
    dim_payment = df[["payment_type"]].drop_duplicates().reset_index(drop=True)
    dim_payment["payment_id"] = dim_payment.index + 1

    # FACT TABLE
    fact = df.merge(
        dim_time,
        on=["pickup_hour", "pickup_day_of_week", "is_weekend"]
    )

    fact = fact.merge(
        dim_payment,
        on="payment_type"
    )

    fact_trips = fact[[
        "time_id",
        "payment_id",
        "fare_amount",
        "trip_distance",
        "trip_duration_minutes",
        "speed_mph",
        "fare_per_mile",
        "passenger_count"
    ]]

    dim_time.to_sql(
        "dim_time",
        engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    dim_payment.to_sql(
        "dim_payment",
        engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    fact_trips.to_sql(
        "fact_trips",
        engine,
        if_exists="replace",
        index=False,
        chunksize=1000
    )

    logging.info(f"dim_time rows: {len(dim_time)}")
    logging.info(f"dim_payment rows: {len(dim_payment)}")
    logging.info(f"fact_trips rows: {len(fact_trips)}")


# -----------------------------
# DAG SETTINGS
# -----------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}


with DAG(
    dag_id="download_nyc_taxi",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id="download_taxi_data",
        python_callable=download_taxi_data
    )

    clean_task = PythonOperator(
        task_id="clean_taxi_data",
        python_callable=clean_taxi_data
    )

    transform_task = PythonOperator(
        task_id="transform_taxi_data",
        python_callable=transform_taxi_data
    )

    load_task = PythonOperator(
        task_id="load_taxi_model",
        python_callable=load_taxi_model
    )

    download_task >> clean_task >> transform_task >> load_task