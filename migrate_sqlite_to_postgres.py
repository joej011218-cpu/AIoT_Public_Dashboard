
"""
Optional one-time migration:
local SQLite aqi_data.db -> Render PostgreSQL.

Run this on your Mac from the folder containing:
- aqi_data.db
- this script

Before running:
    export DATABASE_URL='YOUR_RENDER_POSTGRES_URL'

Then:
    python3 migrate_sqlite_to_postgres.py
"""

import os
import sqlite3
from datetime import datetime

import psycopg2


LOCAL_DB = os.environ.get(
    "LOCAL_AQI_DB",
    "aqi_data.db"
)

DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or
    os.environ.get("CPCB_DATABASE_URL")
)


if not DATABASE_URL:
    raise SystemExit(
        "DATABASE_URL is not set."
    )


def normalize_bool(value):

    if value is None:
        return None

    return bool(value)


src = sqlite3.connect(
    LOCAL_DB
)

src.row_factory = sqlite3.Row

dst = psycopg2.connect(
    DATABASE_URL
)


with dst.cursor() as cur:

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sensor_readings (

            id BIGSERIAL PRIMARY KEY,
            node_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            window_minutes INTEGER,
            gas_samples INTEGER,
            dht_samples INTEGER,
            co_mg_m3 DOUBLE PRECISION,
            co_ppm DOUBLE PRECISION,
            co_raw DOUBLE PRECISION,
            co_voltage DOUBLE PRECISION,
            co_ratio DOUBLE PRECISION,
            co_relative DOUBLE PRECISION,
            co_range_valid BOOLEAN,
            nh3_ug_m3 DOUBLE PRECISION,
            nh3_raw DOUBLE PRECISION,
            nh3_voltage DOUBLE PRECISION,
            nh3_ratio DOUBLE PRECISION,
            nh3_relative DOUBLE PRECISION,
            temp DOUBLE PRECISION,
            humidity DOUBLE PRECISION
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS current_aqi_history (

            id BIGSERIAL PRIMARY KEY,
            node_id TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            cpcb_timestamp TEXT,
            aqi DOUBLE PRECISION,
            category TEXT,
            dominant_pollutant TEXT,
            pm25 DOUBLE PRECISION,
            pm10 DOUBLE PRECISION,
            co_mg_m3 DOUBLE PRECISION,
            nh3_ug_m3 DOUBLE PRECISION,
            pm25_subindex DOUBLE PRECISION,
            pm10_subindex DOUBLE PRECISION,
            co_subindex DOUBLE PRECISION,
            nh3_subindex DOUBLE PRECISION,
            UNIQUE (node_id, timestamp)
        )
        """
    )

dst.commit()


readings = src.execute(
    """
    SELECT *
    FROM readings
    ORDER BY id ASC
    """
).fetchall()


with dst.cursor() as cur:

    for row in readings:

        cur.execute(
            """
            INSERT INTO sensor_readings (

                node_id,
                timestamp,
                window_minutes,
                gas_samples,
                dht_samples,

                co_mg_m3,
                co_ppm,
                co_raw,
                co_voltage,
                co_ratio,
                co_relative,
                co_range_valid,

                nh3_ug_m3,
                nh3_raw,
                nh3_voltage,
                nh3_ratio,
                nh3_relative,

                temp,
                humidity
            )

            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            """,
            (
                row["node_id"] or "node1",
                row["timestamp"],
                row["window_minutes"],
                row["gas_samples"],
                row["dht_samples"],

                row["co_mg_m3"],
                row["co_ppm"],
                row["co_raw"],
                row["co_voltage"],
                row["co_ratio"],
                row["co_relative"],
                normalize_bool(
                    row["co_range_valid"]
                ),

                row["nh3_ug_m3"],
                row["nh3_raw"],
                row["nh3_voltage"],
                row["nh3_ratio"],
                row["nh3_relative"],

                row["temp"],
                row["humidity"],
            )
        )

dst.commit()


table_exists = src.execute(
    """
    SELECT name
    FROM sqlite_master
    WHERE
        type='table'
        AND
        name='current_aqi_history'
    """
).fetchone()


aqi_count = 0

if table_exists:

    aqi_rows = src.execute(
        """
        SELECT *
        FROM current_aqi_history
        ORDER BY id ASC
        """
    ).fetchall()

    with dst.cursor() as cur:

        for row in aqi_rows:

            cur.execute(
                """
                INSERT INTO current_aqi_history (

                    node_id,
                    timestamp,
                    cpcb_timestamp,

                    aqi,
                    category,
                    dominant_pollutant,

                    pm25,
                    pm10,
                    co_mg_m3,
                    nh3_ug_m3,

                    pm25_subindex,
                    pm10_subindex,
                    co_subindex,
                    nh3_subindex
                )

                VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )

                ON CONFLICT (
                    node_id,
                    timestamp
                )
                DO NOTHING
                """,
                (
                    row["node_id"],
                    row["timestamp"],
                    row["cpcb_timestamp"],

                    row["aqi"],
                    row["category"],
                    row["dominant_pollutant"],

                    row["pm25"],
                    row["pm10"],
                    row["co_mg_m3"],
                    row["nh3_ug_m3"],

                    row["pm25_subindex"],
                    row["pm10_subindex"],
                    row["co_subindex"],
                    row["nh3_subindex"],
                )
            )

            aqi_count += 1

    dst.commit()


src.close()
dst.close()

print(
    "Migration complete."
)

print(
    "Sensor readings copied:",
    len(readings)
)

print(
    "Current AQI rows attempted:",
    aqi_count
)
