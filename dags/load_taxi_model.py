import pandas as pd
import logging
from sqlalchemy import create_engine, text
from airflow.models import Variable

def load_taxi_model(**context):

    ti = context["ti"]

    # 1. Pull transformed CSV path from XCom
    transformed_path = ti.xcom_pull(
        key="transformed_path",
        task_ids="transform_taxi_data"
    )

    logging.info(f"Loading transformed data from: {transformed_path}")

    # 2. Load data with pandas
    df = pd.read_csv(transformed_path, low_memory=False)

    # Ensure datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")

    # 3. Connect to MySQL using SQLAlchemy and Airflow Variables
    mysql_host = Variable.get("MYSQL_HOST")
    mysql_user = Variable.get("MYSQL_USER")
    mysql_pass = Variable.get("MYSQL_PASS")
    mysql_db   = Variable.get("MYSQL_DB")

    conn_str = f"mysql+pymysql://{mysql_user}:{mysql_pass}@{mysql_host}/{mysql_db}"
    engine = create_engine(conn_str)

    logging.info("Connected to MySQL")

    # 4. Create star schema tables

    # ---------- dim_time ----------
    dim_time = df[[
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend"
    ]].drop_duplicates().reset_index(drop=True)

    dim_time["time_id"] = dim_time.index + 1

    # ---------- dim_payment ----------
    if "payment_type" in df.columns:
        dim_payment = df[["payment_type"]].drop_duplicates().reset_index(drop=True)
    else:
        dim_payment = pd.DataFrame({"payment_type": ["unknown"]})

    dim_payment["payment_id"] = dim_payment.index + 1

    # ---------- fact_trips ----------
    fact = df.copy()

    # join dim_time
    fact = fact.merge(
        dim_time,
        on=["pickup_hour", "pickup_day_of_week", "is_weekend"],
        how="left"
    )

    # join dim_payment
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

    # 5. Write tables with chunking
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

    logging.info("Tables written to MySQL")

    # 6. Log row counts
    with engine.connect() as conn:

        dim_time_count = conn.execute(text("SELECT COUNT(*) FROM dim_time")).scalar()
        dim_payment_count = conn.execute(text("SELECT COUNT(*) FROM dim_payment")).scalar()
        fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_trips")).scalar()

        logging.info(f"dim_time rows: {dim_time_count}")
        logging.info(f"dim_payment rows: {dim_payment_count}")
        logging.info(f"fact_trips rows: {fact_count}")