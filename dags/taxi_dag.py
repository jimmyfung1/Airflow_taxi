from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable
from datetime import datetime
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine

log = logging.getLogger(__name__)

def ingest_taxi_data(**kwargs):
    ti = kwargs['ti']
    URL = "https://data.cityofnewyork.us/resource/2upf-qytp.csv?$limit=5000"
    OUTPUT_PATH = "/tmp/nyc_taxi_raw.csv"
    MAX_RETRIES = 3

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            log.info(f"Attempt {attempt}/{MAX_RETRIES}: Downloading from {URL}")
            response = requests.get(URL, stream=True, timeout=60)
            response.raise_for_status()

            with open(OUTPUT_PATH, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            log.info(f"File saved to {OUTPUT_PATH}")
            break

        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error on attempt {attempt}: {e}")
            if attempt == MAX_RETRIES:
                raise

    df = pd.read_csv(OUTPUT_PATH, on_bad_lines='skip')
    row_count = len(df)
    log.info(f"Downloaded {row_count} rows")

    if row_count < 1000:
        raise ValueError(f"Validation failed: only {row_count} rows (expected >= 1000)")

    ti.xcom_push(key='taxi_file_path', value=OUTPUT_PATH)
    log.info(f"Pushed {OUTPUT_PATH} to XCom")


def clean_taxi_data(**context):
    ti = context['ti']
    raw_path = ti.xcom_pull(key='taxi_file_path', task_ids='ingest_taxi_data')
    log.info(f"Reading file from: {raw_path}")

    df = pd.read_csv(raw_path, on_bad_lines='skip', low_memory=False)
    original_count = len(df)
    log.info(f"Loaded {original_count} rows")

    # แปลง type ก่อน filter
    df['fare_amount'] = pd.to_numeric(df['fare_amount'], errors='coerce')
    df['trip_distance'] = pd.to_numeric(df['trip_distance'], errors='coerce')

    before = len(df)
    df = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 500)]
    log.info(f"Removed {before - len(df)} rows by fare_amount")

    before = len(df)
    df = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 100)]
    log.info(f"Removed {before - len(df)} rows by trip_distance")

    coord_cols = ['pickup_latitude', 'dropoff_latitude', 'pickup_longitude', 'dropoff_longitude']
    if all(col in df.columns for col in coord_cols):
        before = len(df)
        df = df[
            (df['pickup_latitude'].between(40.4, 41.0)) &
            (df['dropoff_latitude'].between(40.4, 41.0)) &
            (df['pickup_longitude'].between(-74.3, -73.5)) &
            (df['dropoff_longitude'].between(-74.3, -73.5))
        ]
        log.info(f"Removed {before - len(df)} rows by coordinates")
    else:
        log.warning(f"Coordinate columns not found, skipping. Available: {list(df.columns)}")

    before = len(df)
    df = df.dropna(subset=['fare_amount', 'trip_distance', 'tpep_pickup_datetime'])
    log.info(f"Removed {before - len(df)} rows by null values")

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format='mixed')
    log.info("Parsed tpep_pickup_datetime as datetime")

    OUTPUT_PATH = "/tmp/nyc_taxi_clean.csv"
    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Saved {len(df)} clean rows to {OUTPUT_PATH}")
    log.info(f"Total removed: {original_count - len(df)} rows")

    ti.xcom_push(key='clean_file_path', value=OUTPUT_PATH)
    log.info(f"Pushed {OUTPUT_PATH} to XCom")


def transform_taxi_data(**context):
    ti = context['ti']
    clean_path = ti.xcom_pull(key='clean_file_path', task_ids='clean_taxi_data')
    log.info(f"Reading file from: {clean_path}")

    df = pd.read_csv(clean_path, low_memory=False)
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], format='mixed', errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], format='mixed', errors='coerce')

    before = len(df)
    df = df.dropna(subset=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
    log.info(f"Removed {before - len(df)} rows with invalid datetimes")

    df['trip_duration_minutes'] = (
        df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
    ).dt.total_seconds() / 60

    df['speed_mph'] = df['trip_distance'] / (df['trip_duration_minutes'] / 60)
    df['fare_per_mile'] = df['fare_amount'] / df['trip_distance']
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
    df['pickup_day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek
    df['is_weekend'] = df['pickup_day_of_week'] >= 5

    log.info("Computed all derived features")

    before = len(df)
    df = df[(df['speed_mph'] <= 80) & (df['trip_duration_minutes'] >= 1)]
    log.info(f"Removed {before - len(df)} rows by speed/duration filter")

    OUTPUT_PATH = "/tmp/nyc_taxi_transformed.csv"
    df.to_csv(OUTPUT_PATH, index=False)
    log.info(f"Saved {len(df)} rows to {OUTPUT_PATH}")

    for col in ['trip_duration_minutes', 'speed_mph', 'fare_per_mile', 'pickup_hour', 'pickup_day_of_week']:
        log.info(f"{col} → mean={df[col].mean():.2f}, min={df[col].min():.2f}, max={df[col].max():.2f}")

    ti.xcom_push(key='transformed_file_path', value=OUTPUT_PATH)
    log.info(f"Pushed {OUTPUT_PATH} to XCom")


def load_taxi_model(**context):
    ti = context['ti']
    transformed_path = ti.xcom_pull(key='transformed_file_path', task_ids='transform_taxi_data')
    log.info(f"Reading file from: {transformed_path}")

    df = pd.read_csv(transformed_path, low_memory=False)
    log.info(f"Loaded {len(df)} rows")

    host = Variable.get("MYSQL_HOST")
    user = Variable.get("MYSQL_USER")
    password = Variable.get("MYSQL_PASS")
    database = Variable.get("MYSQL_DB")

    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
    log.info("Connected to MySQL")

    dim_time = df[['pickup_hour', 'pickup_day_of_week', 'is_weekend']].drop_duplicates().reset_index(drop=True)
    dim_time.index.name = 'time_id'
    dim_time = dim_time.reset_index()
    dim_time.to_sql('dim_time', con=engine, if_exists='replace', index=False, chunksize=1000)
    log.info(f"dim_time loaded: {len(dim_time)} rows")

    dim_payment = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    dim_payment.index.name = 'payment_id'
    dim_payment = dim_payment.reset_index()
    dim_payment.to_sql('dim_payment', con=engine, if_exists='replace', index=False, chunksize=1000)
    log.info(f"dim_payment loaded: {len(dim_payment)} rows")

    df = df.merge(dim_time, on=['pickup_hour', 'pickup_day_of_week', 'is_weekend'], how='left')
    df = df.merge(dim_payment, on='payment_type', how='left')

    fact_trips = df[[
        'time_id', 'payment_id',
        'fare_amount', 'trip_distance',
        'trip_duration_minutes', 'speed_mph',
        'fare_per_mile', 'passenger_count'
    ]]
    fact_trips.to_sql('fact_trips', con=engine, if_exists='replace', index=False, chunksize=1000)
    log.info(f"fact_trips loaded: {len(fact_trips)} rows")

    engine.dispose()
    log.info("Done loading all tables to MySQL")


with DAG(
    dag_id='nyc_taxi_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_taxi_data',
        python_callable=ingest_taxi_data,
    )

    clean_task = PythonOperator(
        task_id='clean_taxi_data',
        python_callable=clean_taxi_data,
    )

    transform_task = PythonOperator(
        task_id='transform_taxi_data',
        python_callable=transform_taxi_data,
    )

    load_task = PythonOperator(
        task_id='load_taxi_model',
        python_callable=load_taxi_model,
    )

    ingest_task >> clean_task >> transform_task >> load_task