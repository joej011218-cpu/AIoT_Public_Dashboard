
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

DASHBOARD_DIR = os.path.join(
    BASE_DIR,
    "dashboard"
)

DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or
    os.environ.get("CPCB_DATABASE_URL")
    or
    ""
)

CPCB_CITY = os.environ.get(
    "CPCB_CITY",
    "Vadodara"
)

MQTT_BROKER = os.environ.get(
    "MQTT_BROKER",
    "broker.hivemq.com"
)

MQTT_PORT = int(
    os.environ.get(
        "MQTT_PORT",
        "1883"
    )
)

MQTT_TOPIC = os.environ.get(
    "MQTT_TOPIC",
    "aqi/node1/readings"
)

START_MQTT = os.environ.get(
    "START_MQTT",
    "1"
) == "1"


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)
CORS(app)


# ============================================================
# POSTGRES
# ============================================================

def get_db():

    if not DATABASE_URL:

        raise RuntimeError(
            "DATABASE_URL is not configured."
        )

    return psycopg2.connect(
        DATABASE_URL
    )


def json_value(value):

    if isinstance(
        value,
        datetime
    ):

        return value.isoformat()

    if isinstance(
        value,
        Decimal
    ):

        return float(value)

    return value


def row_to_dict(row):

    if row is None:
        return None

    return {
        key: json_value(value)
        for key, value
        in dict(row).items()
    }


def init_db():

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute(
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

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_sensor_readings_node_time
                ON sensor_readings (
                    node_id,
                    timestamp DESC
                )
                """
            )

            cursor.execute(
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

                    UNIQUE (
                        node_id,
                        timestamp
                    )
                )
                """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS
                idx_current_aqi_node_time
                ON current_aqi_history (
                    node_id,
                    timestamp DESC
                )
                """
            )

        conn.commit()

    finally:

        conn.close()

    print(
        "PostgreSQL sensor tables ready."
    )


# ============================================================
# CPCB
# ============================================================

def get_latest_cpcb():

    conn = None

    try:

        conn = get_db()

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT
                    timestamp,
                    pm25,
                    pm10

                FROM cpcb_hourly

                WHERE
                    pm25 IS NOT NULL
                    OR pm10 IS NOT NULL

                ORDER BY
                    timestamp::timestamp DESC

                LIMIT 1
                """
            )

            row = cursor.fetchone()

        if not row:
            return None

        row = row_to_dict(row)

        return {
            "timestamp":
                row.get("timestamp"),

            "pm25_ug_m3":
                row.get("pm25"),

            "pm10_ug_m3":
                row.get("pm10"),

            "city":
                CPCB_CITY,

            "source":
                "Render PostgreSQL"
        }

    except Exception as error:

        print(
            "CPCB read error:",
            error
        )

        return None

    finally:

        if conn is not None:
            conn.close()


# ============================================================
# AQI
# ============================================================

AQI_PM25_BREAKPOINTS = [
    (0, 30, 0, 50),
    (31, 60, 51, 100),
    (61, 90, 101, 200),
    (91, 120, 201, 300),
    (121, 250, 301, 400),
    (251, 380, 401, 500),
]

AQI_PM10_BREAKPOINTS = [
    (0, 50, 0, 50),
    (51, 100, 51, 100),
    (101, 250, 101, 200),
    (251, 350, 201, 300),
    (351, 430, 301, 400),
    (431, 510, 401, 500),
]

AQI_NH3_BREAKPOINTS = [
    (0, 200, 0, 50),
    (201, 400, 51, 100),
    (401, 800, 101, 200),
    (801, 1200, 201, 300),
    (1201, 1800, 301, 400),
    (1801, 2400, 401, 500),
]

AQI_CO_BREAKPOINTS = [
    (0.0, 1.0, 0, 50),
    (1.1, 2.0, 51, 100),
    (2.1, 10.0, 101, 200),
    (10.1, 17.0, 201, 300),
    (17.1, 34.0, 301, 400),
    (34.1, 50.0, 401, 500),
]


def aqi_category(aqi):

    if aqi is None:
        return None

    if aqi <= 50:
        return "Good"

    if aqi <= 100:
        return "Satisfactory"

    if aqi <= 200:
        return "Moderate"

    if aqi <= 300:
        return "Poor"

    if aqi <= 400:
        return "Very Poor"

    return "Severe"


def calculate_sub_index(
    concentration,
    breakpoints,
    decimals=0
):

    if concentration is None:
        return None

    try:
        concentration = float(
            concentration
        )
    except (TypeError, ValueError):
        return None

    if concentration < 0:
        return None

    concentration = round(
        concentration,
        decimals
    )

    if concentration > breakpoints[-1][1]:
        return 500

    for (
        c_low,
        c_high,
        i_low,
        i_high
    ) in breakpoints:

        if (
            c_low
            <= concentration
            <= c_high
        ):

            if c_high == c_low:
                return float(i_high)

            value = (
                (i_high - i_low)
                /
                (c_high - c_low)
                *
                (concentration - c_low)
                +
                i_low
            )

            return round(
                value
            )

    # Handle small decimal gaps between CPCB CO bands,
    # e.g. 1.0 < value < 1.1, by selecting the nearest band edge.
    for index in range(
        len(breakpoints) - 1
    ):

        left = breakpoints[index]
        right = breakpoints[index + 1]

        if (
            left[1]
            < concentration
            < right[0]
        ):

            midpoint = (
                left[1] + right[0]
            ) / 2

            if concentration <= midpoint:
                return float(left[3])

            return float(right[2])

    return None


def calculate_current_aqi(
    pm25,
    pm10,
    co_mg_m3,
    nh3_ug_m3
):

    concentrations = {
        "pm25": pm25,
        "pm10": pm10,
        "co": co_mg_m3,
        "nh3": nh3_ug_m3,
    }

    if not all(
        value is not None
        for value
        in concentrations.values()
    ):

        available = [
            key
            for key, value
            in concentrations.items()
            if value is not None
        ]

        return {
            "label":
                "Current AQI",

            "ready":
                False,

            "aqi":
                None,

            "category":
                None,

            "dominant_pollutant":
                None,

            "pollutants_used":
                available,

            "sub_indices":
                {},

            "concentrations":
                concentrations,

            "note":
                "Waiting for current PM2.5, PM10, CO and NH3 values."
        }

    sub_indices = {
        "pm25":
            calculate_sub_index(
                pm25,
                AQI_PM25_BREAKPOINTS,
                0
            ),

        "pm10":
            calculate_sub_index(
                pm10,
                AQI_PM10_BREAKPOINTS,
                0
            ),

        "co":
            calculate_sub_index(
                co_mg_m3,
                AQI_CO_BREAKPOINTS,
                1
            ),

        "nh3":
            calculate_sub_index(
                nh3_ug_m3,
                AQI_NH3_BREAKPOINTS,
                0
            ),
    }

    sub_indices = {
        key: value
        for key, value
        in sub_indices.items()
        if value is not None
    }

    if len(sub_indices) != 4:

        return {
            "label":
                "Current AQI",

            "ready":
                False,

            "aqi":
                None,

            "category":
                None,

            "dominant_pollutant":
                None,

            "pollutants_used":
                list(sub_indices.keys()),

            "sub_indices":
                sub_indices,

            "concentrations":
                concentrations,

            "note":
                "One or more values could not be converted to an AQI sub-index."
        }

    dominant = max(
        sub_indices,
        key=sub_indices.get
    )

    aqi = sub_indices[
        dominant
    ]

    return {
        "label":
            "Current AQI",

        "ready":
            True,

        "aqi":
            aqi,

        "category":
            aqi_category(aqi),

        "dominant_pollutant":
            dominant,

        "pollutants_used":
            [
                "pm25",
                "pm10",
                "co",
                "nh3"
            ],

        "sub_indices":
            sub_indices,

        "concentrations":
            concentrations,

        "note":
            (
                "Current project AQI calculated from the latest "
                "PM2.5, PM10, CO and NH3 values."
            )
    }


# ============================================================
# SENSOR DB HELPERS
# ============================================================

def save_sensor_reading(
    data,
    timestamp
):

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute(
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
                    %s, %s,
                    %s,
                    %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s
                )

                RETURNING id
                """,
                (
                    data.get(
                        "node_id",
                        "node1"
                    ),

                    timestamp,

                    data.get(
                        "window_minutes"
                    ),

                    data.get(
                        "gas_samples"
                    ),

                    data.get(
                        "dht_samples"
                    ),

                    data.get(
                        "co_mg_m3"
                    ),

                    data.get(
                        "co_ppm"
                    ),

                    data.get(
                        "co_raw"
                    ),

                    data.get(
                        "co_voltage"
                    ),

                    data.get(
                        "co_ratio"
                    ),

                    data.get(
                        "co_relative"
                    ),

                    bool(
                        data.get(
                            "co_range_valid",
                            False
                        )
                    ),

                    data.get(
                        "nh3_ug_m3"
                    ),

                    data.get(
                        "nh3_raw"
                    ),

                    data.get(
                        "nh3_voltage"
                    ),

                    data.get(
                        "nh3_ratio"
                    ),

                    data.get(
                        "nh3_relative"
                    ),

                    data.get(
                        "temp"
                    ),

                    data.get(
                        "humidity"
                    ),
                )
            )

            row_id = cursor.fetchone()[0]

        conn.commit()

        return row_id

    finally:

        conn.close()


def save_current_aqi_snapshot(
    current_aqi,
    node_id,
    sensor_timestamp,
    cpcb_timestamp=None
):

    if (
        not current_aqi
        or
        not current_aqi.get("ready")
        or
        current_aqi.get("aqi") is None
    ):

        return False

    concentrations = current_aqi.get(
        "concentrations",
        {}
    )

    sub_indices = current_aqi.get(
        "sub_indices",
        {}
    )

    conn = get_db()

    try:

        with conn.cursor() as cursor:

            cursor.execute(
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

                DO UPDATE SET

                    cpcb_timestamp =
                        EXCLUDED.cpcb_timestamp,

                    aqi =
                        EXCLUDED.aqi,

                    category =
                        EXCLUDED.category,

                    dominant_pollutant =
                        EXCLUDED.dominant_pollutant,

                    pm25 =
                        EXCLUDED.pm25,

                    pm10 =
                        EXCLUDED.pm10,

                    co_mg_m3 =
                        EXCLUDED.co_mg_m3,

                    nh3_ug_m3 =
                        EXCLUDED.nh3_ug_m3,

                    pm25_subindex =
                        EXCLUDED.pm25_subindex,

                    pm10_subindex =
                        EXCLUDED.pm10_subindex,

                    co_subindex =
                        EXCLUDED.co_subindex,

                    nh3_subindex =
                        EXCLUDED.nh3_subindex
                """,
                (
                    node_id,
                    sensor_timestamp,
                    cpcb_timestamp,

                    current_aqi.get("aqi"),
                    current_aqi.get("category"),
                    current_aqi.get(
                        "dominant_pollutant"
                    ),

                    concentrations.get(
                        "pm25"
                    ),

                    concentrations.get(
                        "pm10"
                    ),

                    concentrations.get(
                        "co"
                    ),

                    concentrations.get(
                        "nh3"
                    ),

                    sub_indices.get(
                        "pm25"
                    ),

                    sub_indices.get(
                        "pm10"
                    ),

                    sub_indices.get(
                        "co"
                    ),

                    sub_indices.get(
                        "nh3"
                    ),
                )
            )

        conn.commit()

        return True

    finally:

        conn.close()


def get_latest_sensor(
    node_id="node1"
):

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT
                    *

                FROM sensor_readings

                WHERE node_id = %s

                ORDER BY
                    timestamp DESC,
                    id DESC

                LIMIT 1
                """,
                (
                    node_id,
                )
            )

            row = cursor.fetchone()

        return row_to_dict(row)

    finally:

        conn.close()


# ============================================================
# MQTT
# ============================================================

mqtt_client = None
mqtt_lock = threading.Lock()
mqtt_started = False


def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties
):

    print(
        "MQTT connected:",
        reason_code
    )

    client.subscribe(
        MQTT_TOPIC
    )

    print(
        "Subscribed:",
        MQTT_TOPIC
    )


def on_message(
    client,
    userdata,
    msg
):

    try:

        if msg.retain:

            print(
                "Ignored retained MQTT message."
            )

            return

        data = json.loads(
            msg.payload.decode(
                "utf-8"
            )
        )

        if not any(
            key in data
            for key in (
                "co_mg_m3",
                "nh3_ug_m3",
                "temp",
                "humidity"
            )
        ):

            return

        timestamp = datetime.now(
            timezone.utc
        )

        row_id = save_sensor_reading(
            data,
            timestamp
        )

        cpcb = get_latest_cpcb()

        if cpcb:

            current_aqi = calculate_current_aqi(
                cpcb.get(
                    "pm25_ug_m3"
                ),
                cpcb.get(
                    "pm10_ug_m3"
                ),
                data.get(
                    "co_mg_m3"
                ),
                data.get(
                    "nh3_ug_m3"
                )
            )

            save_current_aqi_snapshot(
                current_aqi,
                data.get(
                    "node_id",
                    "node1"
                ),
                timestamp,
                cpcb.get(
                    "timestamp"
                )
            )

        print()
        print(
            "15-MINUTE SENSOR READING SAVED"
        )
        print(
            "PostgreSQL row:",
            row_id
        )
        print(
            "Node:",
            data.get(
                "node_id",
                "node1"
            )
        )
        print(
            "CO:",
            data.get(
                "co_mg_m3"
            ),
            "mg/m3"
        )
        print(
            "NH3:",
            data.get(
                "nh3_ug_m3"
            ),
            "ug/m3"
        )

    except Exception as error:

        print(
            "MQTT message error:",
            error
        )


def mqtt_loop():

    global mqtt_client

    while True:

        try:

            client = mqtt.Client(
                callback_api_version=
                    mqtt.CallbackAPIVersion.VERSION2,

                client_id=
                    f"aiot-render-{os.getpid()}"
            )

            client.on_connect = on_connect
            client.on_message = on_message

            client.connect(
                MQTT_BROKER,
                MQTT_PORT,
                keepalive=60
            )

            mqtt_client = client

            client.loop_forever(
                retry_first_connection=True
            )

        except Exception as error:

            print(
                "MQTT connection error:",
                error
            )

            time.sleep(10)


def start_mqtt_once():

    global mqtt_started

    if not START_MQTT:
        return

    with mqtt_lock:

        if mqtt_started:
            return

        mqtt_started = True

        thread = threading.Thread(
            target=mqtt_loop,
            daemon=True
        )

        thread.start()

        print(
            "MQTT background thread started."
        )


# ============================================================
# API - LATEST
# ============================================================

@app.route(
    "/api/latest"
)
def api_latest():

    node_id = request.args.get(
        "node_id",
        "node1"
    )

    sensor = get_latest_sensor(
        node_id
    )

    if not sensor:

        return jsonify(
            {
                "error":
                    "no sensor data yet"
            }
        ), 404

    result = dict(sensor)

    cpcb = get_latest_cpcb()

    if cpcb:

        result["pm25_ug_m3"] = (
            cpcb.get(
                "pm25_ug_m3"
            )
        )

        result["pm10_ug_m3"] = (
            cpcb.get(
                "pm10_ug_m3"
            )
        )

        result["cpcb_timestamp"] = (
            cpcb.get(
                "timestamp"
            )
        )

        result["pm25_timestamp"] = (
            cpcb.get(
                "timestamp"
            )
        )

        result["pm10_timestamp"] = (
            cpcb.get(
                "timestamp"
            )
        )

    else:

        result["pm25_ug_m3"] = None
        result["pm10_ug_m3"] = None
        result["cpcb_timestamp"] = None
        result["pm25_timestamp"] = None
        result["pm10_timestamp"] = None

    result["cpcb_city"] = CPCB_CITY

    result["sources"] = {
        "pm25":
            "CPCB database",

        "pm10":
            "CPCB database",

        "co":
            "ESP32 MQ-7",

        "nh3":
            "ESP32 MQ-135",

        "temperature":
            "ESP32 DHT11",

        "humidity":
            "ESP32 DHT11"
    }

    result["current_aqi"] = (
        calculate_current_aqi(
            result.get(
                "pm25_ug_m3"
            ),
            result.get(
                "pm10_ug_m3"
            ),
            result.get(
                "co_mg_m3"
            ),
            result.get(
                "nh3_ug_m3"
            )
        )
    )

    if (
        result.get("timestamp")
        and
        result["current_aqi"].get(
            "ready"
        )
    ):

        save_current_aqi_snapshot(
            result["current_aqi"],
            node_id,
            result.get(
                "timestamp"
            ),
            result.get(
                "cpcb_timestamp"
            )
        )

    return jsonify(
        result
    )


# ============================================================
# API - CPCB
# ============================================================

@app.route(
    "/api/cpcb/latest"
)
@app.route(
    "/api/cpcb/refresh"
)
def api_cpcb_latest():

    cpcb = get_latest_cpcb()

    if not cpcb:

        return jsonify(
            {
                "error":
                    "CPCB data unavailable"
            }
        ), 503

    return jsonify(
        cpcb
    )


# ============================================================
# API - SENSOR HISTORY
# ============================================================

@app.route(
    "/api/history"
)
def api_history():

    node_id = request.args.get(
        "node_id",
        "node1"
    )

    try:
        hours = int(
            request.args.get(
                "hours",
                24
            )
        )
    except ValueError:
        hours = 24

    hours = max(
        1,
        min(
            hours,
            24 * 365
        )
    )

    since = datetime.now(
        timezone.utc
    ) - timedelta(
        hours=hours
    )

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT
                    *

                FROM sensor_readings

                WHERE
                    node_id = %s
                    AND
                    timestamp >= %s

                ORDER BY
                    timestamp ASC
                """,
                (
                    node_id,
                    since
                )
            )

            rows = cursor.fetchall()

        return jsonify(
            [
                row_to_dict(row)
                for row in rows
            ]
        )

    finally:

        conn.close()


# ============================================================
# API - ANALYTICS
# ============================================================

@app.route(
    "/api/analytics/history"
)
def api_analytics_history():

    node_id = request.args.get(
        "node_id",
        "node1"
    )

    try:
        hours = int(
            request.args.get(
                "hours",
                24
            )
        )
    except ValueError:
        hours = 24

    hours = max(
        1,
        min(
            hours,
            24 * 365
        )
    )

    sensor_since = datetime.now(
        timezone.utc
    ) - timedelta(
        hours=hours
    )

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT

                    id,
                    node_id,
                    timestamp,

                    co_mg_m3,
                    nh3_ug_m3,
                    temp,
                    humidity

                FROM sensor_readings

                WHERE
                    node_id = %s
                    AND
                    timestamp >= %s
                    AND (
                        co_mg_m3 IS NOT NULL
                        OR nh3_ug_m3 IS NOT NULL
                        OR temp IS NOT NULL
                        OR humidity IS NOT NULL
                    )

                ORDER BY
                    timestamp ASC
                """,
                (
                    node_id,
                    sensor_since
                )
            )

            sensor_rows = cursor.fetchall()

            cursor.execute(
                """
                SELECT

                    id,
                    node_id,
                    timestamp,

                    aqi,
                    category,
                    dominant_pollutant,

                    pm25_subindex,
                    pm10_subindex,
                    nh3_subindex,
                    co_subindex

                FROM current_aqi_history

                WHERE
                    node_id = %s
                    AND
                    timestamp >= %s
                    AND
                    aqi IS NOT NULL

                ORDER BY
                    timestamp ASC
                """,
                (
                    node_id,
                    sensor_since
                )
            )

            aqi_rows = cursor.fetchall()

    finally:

        conn.close()

    india_now = datetime.now(
        ZoneInfo(
            "Asia/Kolkata"
        )
    )

    cpcb_since = (
        india_now
        -
        timedelta(
            hours=hours
        )
    ).replace(
        tzinfo=None
    )

    cpcb_rows = []
    cpcb_error = None

    conn = None

    try:

        conn = get_db()

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT
                    timestamp,
                    pm25,
                    pm10

                FROM cpcb_hourly

                WHERE
                    timestamp::timestamp
                    >= %s::timestamp
                    AND (
                        pm25 IS NOT NULL
                        OR pm10 IS NOT NULL
                    )

                ORDER BY
                    timestamp::timestamp ASC
                """,
                (
                    cpcb_since,
                )
            )

            rows = cursor.fetchall()

        cpcb_rows = [
            row_to_dict(row)
            for row in rows
        ]

    except Exception as error:

        cpcb_error = str(
            error
        )

    finally:

        if conn is not None:
            conn.close()

    return jsonify(
        {
            "node_id":
                node_id,

            "hours":
                hours,

            "sensor":
                [
                    row_to_dict(row)
                    for row in sensor_rows
                ],

            "cpcb":
                cpcb_rows,

            "aqi":
                [
                    row_to_dict(row)
                    for row in aqi_rows
                ],

            "sources": {
                "pm25":
                    "CPCB database",

                "pm10":
                    "CPCB database",

                "co":
                    "ESP32 MQ-7",

                "nh3":
                    "ESP32 MQ-135",

                "aqi":
                    "Current 4-pollutant AQI"
            },

            "cpcb_error":
                cpcb_error
        }
    )


# ============================================================
# API - ALERTS
# ============================================================

@app.route(
    "/api/alerts"
)
def api_alerts():

    node_id = request.args.get(
        "node_id",
        "node1"
    )

    try:
        limit = int(
            request.args.get(
                "limit",
                200
            )
        )
    except ValueError:
        limit = 200

    limit = max(
        1,
        min(
            limit,
            1000
        )
    )

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT

                    id,
                    node_id,
                    timestamp,

                    aqi,
                    category,
                    dominant_pollutant,

                    pm25_subindex,
                    pm10_subindex,
                    nh3_subindex,
                    co_subindex

                FROM current_aqi_history

                WHERE
                    node_id = %s
                    AND
                    aqi IS NOT NULL
                    AND
                    aqi >= 201

                ORDER BY
                    timestamp DESC

                LIMIT %s
                """,
                (
                    node_id,
                    limit
                )
            )

            rows = cursor.fetchall()

        return jsonify(
            [
                row_to_dict(row)
                for row in rows
            ]
        )

    finally:

        conn.close()


# ============================================================
# API - CURRENT AQI HISTORY
# ============================================================

@app.route(
    "/api/current-aqi/history"
)
def api_current_aqi_history():

    node_id = request.args.get(
        "node_id",
        "node1"
    )

    try:
        hours = int(
            request.args.get(
                "hours",
                24
            )
        )
    except ValueError:
        hours = 24

    since = datetime.now(
        timezone.utc
    ) - timedelta(
        hours=hours
    )

    conn = get_db()

    try:

        with conn.cursor(
            cursor_factory=RealDictCursor
        ) as cursor:

            cursor.execute(
                """
                SELECT

                    id,
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

                FROM current_aqi_history

                WHERE
                    node_id = %s
                    AND
                    timestamp >= %s

                ORDER BY
                    timestamp ASC
                """,
                (
                    node_id,
                    since
                )
            )

            rows = cursor.fetchall()

        return jsonify(
            [
                row_to_dict(row)
                for row in rows
            ]
        )

    finally:

        conn.close()


# ============================================================
# API - STATUS / HEALTH
# ============================================================

@app.route(
    "/api/status"
)
def api_status():

    cpcb = get_latest_cpcb()

    sensor = None

    try:
        sensor = get_latest_sensor(
            "node1"
        )
    except Exception:
        pass

    return jsonify(
        {
            "backend":
                "running",

            "database":
                "Render PostgreSQL",

            "database_configured":
                bool(DATABASE_URL),

            "mqtt_broker":
                MQTT_BROKER,

            "mqtt_topic":
                MQTT_TOPIC,

            "mqtt_enabled":
                START_MQTT,

            "cpcb_city":
                CPCB_CITY,

            "latest_cpcb":
                cpcb,

            "latest_sensor_timestamp":
                (
                    sensor.get(
                        "timestamp"
                    )
                    if sensor
                    else None
                )
        }
    )


@app.route(
    "/healthz"
)
def healthz():

    return jsonify(
        {
            "status":
                "ok"
        }
    )


# ============================================================
# DASHBOARD
# ============================================================

@app.route("/")
def dashboard_home():

    return send_from_directory(
        DASHBOARD_DIR,
        "index.html"
    )


@app.route(
    "/<path:path>"
)
def dashboard_files(path):

    full_path = os.path.join(
        DASHBOARD_DIR,
        path
    )

    if os.path.isfile(
        full_path
    ):

        return send_from_directory(
            DASHBOARD_DIR,
            path
        )

    return send_from_directory(
        DASHBOARD_DIR,
        "index.html"
    )


# ============================================================
# STARTUP
# ============================================================

def startup():

    # A short retry helps during Render deploys if Postgres
    # needs a moment before accepting connections.
    last_error = None

    for attempt in range(1, 6):

        try:

            init_db()

            last_error = None

            break

        except Exception as error:

            last_error = error

            print(
                f"Database init attempt {attempt} failed:",
                error
            )

            time.sleep(3)

    if last_error is not None:

        raise last_error

    start_mqtt_once()


startup()


if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "5000"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False
    )
