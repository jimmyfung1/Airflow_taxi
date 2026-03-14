# 🚕 NYC Taxi Data Pipeline — Apache Airflow

**Course:** DE241 | **Date:** 14 Mar, 2026

---

## 📋 Project Overview

Pipeline สำหรับดึง ทำความสะอาด แปลง และโหลดข้อมูล NYC Yellow Taxi Trip Records เข้า MySQL โดยใช้ Apache Airflow บน Docker

---

## 🏗️ Architecture

```
ingest_taxi_data → clean_taxi_data → transform_taxi_data → load_taxi_model
```

| Task | คำอธิบาย |
|------|----------|
| `ingest_taxi_data` | ดาวน์โหลด CSV จาก NYC Open Data API |
| `clean_taxi_data` | ทำความสะอาดข้อมูล เช่น ลบ outliers, null values |
| `transform_taxi_data` | คำนวณ derived features เช่น speed_mph, fare_per_mile |
| `load_taxi_model` | โหลดข้อมูลเข้า MySQL แบบ Star Schema |

---

## 🛠️ Tech Stack

- **Apache Airflow 3.x** — Workflow orchestration
- **Docker + Docker Compose** — Container runtime
- **Python 3.12** — ภาษาหลัก
- **Pandas** — Data processing
- **SQLAlchemy + PyMySQL** — Database connection
- **MySQL 8** — Data warehouse
- **GitHub** — Version control

---

## 📁 Project Structure

```
lab8/
├── airflow/
│   ├── dags/
│   │   └── taxi_dag.py        # DAG หลัก
│   ├── logs/                  # Airflow logs
│   ├── plugins/               # Airflow plugins
│   ├── docker-compose.yaml    # Docker Compose config
│   └── .env                   # Environment variables
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Docker Desktop
- Git

### 1. Clone Repository

```bash
git clone https://github.com/Nampu2539/nyc-taxi-airflow.git
cd nyc-taxi-airflow
```

### 2. ตั้งค่า Environment

```bash
cd airflow
echo AIRFLOW_UID=50000 > .env
```

### 3. รัน Airflow

```bash
docker compose up -d
```

รอประมาณ 2-3 นาที แล้วเปิด browser ที่ `http://localhost:8080`

- **Username:** `airflow`
- **Password:** `airflow`

### 4. รัน MySQL

```bash
docker run -d \
  --name mysql-taxi \
  --network airflow_default \
  -e MYSQL_ROOT_PASSWORD=root1234 \
  -e MYSQL_DATABASE=taxi_db \
  -p 3306:3306 \
  mysql:8
```

### 5. ตั้งค่า Airflow Variables

ไปที่ **Admin → Variables** ใน Airflow UI แล้วเพิ่ม

| Key | Value |
|-----|-------|
| `MYSQL_HOST` | `mysql-taxi` |
| `MYSQL_USER` | `root` |
| `MYSQL_PASS` | `root1234` |
| `MYSQL_DB` | `taxi_db` |

### 6. Trigger DAG

ใน Airflow UI ค้นหา `nyc_taxi_pipeline` แล้วกด ▶️ Trigger

---

## 📊 Data Source

**NYC Yellow Taxi Trip Records 2019**
- URL: `https://data.cityofnewyork.us/resource/2upf-qytp.csv`
- ดึงข้อมูล 5,000 rows ต่อครั้ง (สามารถปรับ `$limit` ได้)

---

## 🔧 Task Details

### 1. `ingest_taxi_data`

- ดาวน์โหลด CSV ด้วย `requests` แบบ stream
- บันทึกไฟล์ที่ `/tmp/nyc_taxi_raw.csv`
- Retry 3 ครั้งถ้า connection error
- Validate ว่ามีอย่างน้อย 1,000 rows
- Push file path ไปยัง XCom

### 2. `clean_taxi_data`

- Pull file path จาก XCom
- Cleaning rules:
  - ลบ `fare_amount <= 0` หรือ `> 500`
  - ลบ `trip_distance <= 0` หรือ `> 100`
  - ลบ rows ที่มี null ใน `fare_amount`, `trip_distance`, `tpep_pickup_datetime`
  - Parse `tpep_pickup_datetime` เป็น datetime
- บันทึกที่ `/tmp/nyc_taxi_clean.csv`

### 3. `transform_taxi_data`

- คำนวณ derived features:
  - `trip_duration_minutes` — ระยะเวลาเดินทาง (นาที)
  - `speed_mph` — ความเร็วเฉลี่ย (ไมล์/ชั่วโมง)
  - `fare_per_mile` — ค่าโดยสารต่อไมล์
  - `pickup_hour` — ชั่วโมงที่รับผู้โดยสาร
  - `pickup_day_of_week` — วันในสัปดาห์ (0=Monday)
  - `is_weekend` — True ถ้าเป็นวันเสาร์-อาทิตย์
- Filter: ลบ trips ที่ `speed_mph > 80` หรือ `trip_duration_minutes < 1`
- บันทึกที่ `/tmp/nyc_taxi_transformed.csv`

### 4. `load_taxi_model`

- โหลดข้อมูลเข้า MySQL แบบ Star Schema
- **dim_time** — ข้อมูลเวลา (hour, day_of_week, is_weekend)
- **dim_payment** — ประเภทการชำระเงิน
- **fact_trips** — ข้อมูล trip หลัก พร้อม FK ไปยัง dimension tables
- ใช้ `chunksize=1000` สำหรับ chunked writes

---

## 🗄️ Star Schema

```
        dim_time
           |
fact_trips-+
           |
        dim_payment
```

### dim_time
| Column | Type |
|--------|------|
| time_id | INT (PK) |
| pickup_hour | INT |
| pickup_day_of_week | INT |
| is_weekend | BOOL |

### dim_payment
| Column | Type |
|--------|------|
| payment_id | INT (PK) |
| payment_type | INT |

### fact_trips
| Column | Type |
|--------|------|
| time_id | INT (FK) |
| payment_id | INT (FK) |
| fare_amount | FLOAT |
| trip_distance | FLOAT |
| trip_duration_minutes | FLOAT |
| speed_mph | FLOAT |
| fare_per_mile | FLOAT |
| passenger_count | INT |

---

## ⚠️ Known Issues & Fixes

| ปัญหา | วิธีแก้ |
|-------|---------|
| `404 Not Found` สำหรับ URL เดิม | เปลี่ยนเป็น `/resource/` endpoint |
| `pickup_latitude` ไม่มีในข้อมูลปี 2019 | ข้ามขั้นตอน coordinate filter |
| datetime format ไม่ตรง | ใช้ `format='mixed'` |
| column เป็น string แทน numeric | ใช้ `pd.to_numeric(..., errors='coerce')` |

---

## 📝 License

MIT