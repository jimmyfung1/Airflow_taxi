from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

from clean_taxi_data import clean_taxi_data
from transform_taxi_data import transform_taxi_data
from load_taxi_model import load_taxi_model

import pandas as pd
import requests
import logging
import csv

from sqlalchemy import create_engine, text
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024,1,1)
}

with DAG(
    dag_id="taxi_ingest_dag",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data
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

    ingest_task >> clean_task >> transform_task >> load_task

# ---------------------------------
# GLOBAL SETTINGS
# ---------------------------------

URL = "https://data.cityofnewyork.us/resource/t29m-gskq.csv?$limit=5000"

RAW_PATH = "/tmp/nyc_taxi_raw.csv"
CLEAN_PATH = "/tmp/nyc_taxi_clean.csv"
TRANSFORM_PATH = "/tmp/nyc_taxi_transformed.csv"


# ---------------------------------
# 1 INGEST DATA
# ---------------------------------

def ingest_taxi_data(**context):

    retries = 3

    for attempt in range(retries):

        try:

            response = requests.get(URL, stream=True, timeout=120)
            response.raise_for_status()

            with open(RAW_PATH, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            with open(RAW_PATH, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                row_count = sum(1 for row in reader)

            logging.info(f"Downloaded rows: {row_count}")

            context["ti"].xcom_push(
                key="raw_path",
                value=RAW_PATH
            )

            break

        except requests.exceptions.RequestException as e:

            logging.warning(f"Attempt {attempt+1} failed: {e}")

            if attempt == retries - 1:
                raise


# ---------------------------------
# 2 CLEAN DATA
# ---------------------------------

def clean_taxi_data(**context):

    ti = context["ti"]

    raw_path = ti.xcom_pull(
        key="raw_path",
        task_ids="ingest_taxi_data"
    )

    logging.info(f"Loading raw file {raw_path}")

    df = pd.read_csv(raw_path, low_memory=False)

    original_rows = len(df)

    # Fare cleaning
    before = len(df)
    df = df[(df["fare_amount"] > 0) & (df["fare_amount"] <= 500)]
    logging.info(f"Removed {before-len(df)} rows (fare filter)")

    # Distance cleaning
    before = len(df)
    df = df[(df["trip_distance"] > 0) & (df["trip_distance"] <= 100)]
    logging.info(f"Removed {before-len(df)} rows (distance filter)")

    # Drop nulls
    before = len(df)
    df = df.dropna(subset=[
        "fare_amount",
        "trip_distance",
        "tpep_pickup_datetime"
    ])
    logging.info(f"Removed {before-len(df)} rows (null filter)")

    # Parse datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce"
    )

    df.to_csv(CLEAN_PATH, index=False)

    logging.info(f"Cleaned rows: {len(df)}")

    ti.xcom_push(
        key="clean_path",
        value=CLEAN_PATH
    )


# ---------------------------------
# 3 TRANSFORM DATA
# ---------------------------------

def transform_taxi_data(**context):

    ti = context["ti"]

    clean_path = ti.xcom_pull(
        key="clean_path",
        task_ids="clean_taxi_data"
    )

    logging.info(f"Loading cleaned file {clean_path}")

    df = pd.read_csv(clean_path, low_memory=False)

    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"],
        errors="coerce"
    )

    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"],
        errors="coerce"
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

    # Filter unrealistic trips
    before = len(df)

    df = df[
        (df["speed_mph"] <= 80) &
        (df["trip_duration_minutes"] >= 1)
    ]

    logging.info(f"Removed {before-len(df)} unrealistic trips")

    df.to_csv(TRANSFORM_PATH, index=False)

    logging.info(f"Transformed rows: {len(df)}")

    logging.info(df[[
        "trip_duration_minutes",
        "speed_mph",
        "fare_per_mile"
    ]].describe().to_string())

    ti.xcom_push(
        key="transformed_path",
        value=TRANSFORM_PATH
    )


# ---------------------------------
# 4 LOAD DATA TO MYSQL
# ---------------------------------

def load_taxi_model(**context):

    ti = context["ti"]

    path = ti.xcom_pull(
        key="transformed_path",
        task_ids="transform_taxi_data"
    )

    df = pd.read_csv(path)

    mysql_host = Variable.get("MYSQL_HOST")
    mysql_user = Variable.get("MYSQL_USER")
    mysql_pass = Variable.get("MYSQL_PASS")
    mysql_db = Variable.get("MYSQL_DB")

    conn_str = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"

    engine = create_engine(conn_str)

    logging.info("Connected to MySQL")

    # DIM TIME
    dim_time = df[[
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend"
    ]].drop_duplicates().reset_index(drop=True)

    dim_time["time_id"] = dim_time.index + 1

    # DIM PAYMENT
    if "payment_type" in df.columns:
        dim_payment = df[["payment_type"]].drop_duplicates()
    else:
        dim_payment = pd.DataFrame({"payment_type": ["unknown"]})

    dim_payment["payment_id"] = dim_payment.index + 1

    # FACT TABLE
    fact = df.merge(
        dim_time,
        on=["pickup_hour", "pickup_day_of_week", "is_weekend"],
        how="left"
    )

    if "payment_type" in fact.columns:
        fact = fact.merge(dim_payment, on="payment_type", how="left")
    else:
        fact["payment_id"] = 1

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

    # WRITE TABLES
    dim_time.to_sql("dim_time", engine, if_exists="replace", index=False, chunksize=1000)

    dim_payment.to_sql("dim_payment", engine, if_exists="replace", index=False, chunksize=1000)

    fact_trips.to_sql("fact_trips", engine, if_exists="replace", index=False, chunksize=1000)

    logging.info("Tables loaded")

    with engine.connect() as conn:

        t1 = conn.execute(text("SELECT COUNT(*) FROM dim_time")).scalar()
        t2 = conn.execute(text("SELECT COUNT(*) FROM dim_payment")).scalar()
        t3 = conn.execute(text("SELECT COUNT(*) FROM fact_trips")).scalar()

        logging.info(f"dim_time rows: {t1}")
        logging.info(f"dim_payment rows: {t2}")
        logging.info(f"fact_trips rows: {t3}")


# ---------------------------------
# DAG
# ---------------------------------

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024,1,1)
}

with DAG(
    dag_id="taxi_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_taxi_data",
        python_callable=ingest_taxi_data
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

    ingest_task >> clean_task >> transform_task >> load_task