# 🏗️ Data Warehouse Project: Sparkify ETL Pipeline

## 📌 Overview

This project implements an ETL pipeline for Sparkify, a fictional music streaming startup. The goal is to build a cloud-based data warehouse using **Amazon Redshift** and populate it with user activity logs and song metadata from **Amazon S3**.

The final data model is designed in a **star schema**, enabling business analysts to perform efficient analytical queries like identifying most played songs, user behavior, or artist popularity.

---

## 🧱 Project Components

| File | Description |
|------|-------------|
| `create_tables.py` | Creates all **staging** and **analytics** tables in Redshift. Drops old ones if they exist. |
| `etl.py` | Runs the full ETL pipeline — copies data from S3 → staging tables → inserts into final tables. |
| `sql_queries.py` | Contains all SQL `CREATE`, `DROP`, `COPY`, and `INSERT` statements used by the other scripts. |
| `dwh.cfg` | Configuration file containing AWS Redshift cluster connection details and S3 paths. |

---

## ⭐️ Data Model (Star Schema)

### 🎯 Fact Table
- `songplays`: Records song play events, including user, time, song, artist, and session metadata.

### 🧩 Dimension Tables
- `users`: User information (name, gender, level).
- `songs`: Song information (title, year, duration).
- `artists`: Artist data (name, location).
- `time`: Timestamps split into hour, day, week, month, year, weekday.

---

## 🔁 ETL Pipeline Process

### Step 1: **Extract**
- Copy raw JSON data from:
  - `s3://udacity-dend/song_data`
  - `s3://udacity-dend/log_data`
- Loaded into:
  - `staging_songs`
  - `staging_events`

### Step 2: **Transform**
- Parse JSON logs using `log_json_path.json`
- Filter relevant events like `page = 'NextSong'`
- Join `staging_events` with `staging_songs` to build enriched records

### Step 3: **Load**
- Final data is inserted into:
  - `songplays`, `users`, `songs`, `artists`, `time`

---

## 🧪 How to Run

> Ensure you’ve created your Redshift cluster, IAM Role, and updated `dwh.cfg` with the correct values.

```bash
# Step 1: Create tables
python create_tables.py

# Step 2: Run the ETL pipeline
python etl.py
