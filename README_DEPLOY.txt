AIoT PUBLIC DASHBOARD - RENDER DEPLOYMENT

This package hosts both:
1. Flask backend
2. Live/Analytics/Alerts/About dashboard

PUBLIC ARCHITECTURE
ESP32 -> HiveMQ -> Render Web Service -> Render PostgreSQL -> Public Dashboard

DATABASE
Use your existing Render PostgreSQL database.
The app will keep your existing cpcb_hourly table and automatically create:
- sensor_readings
- current_aqi_history

RENDER SETTINGS

Build Command:
    pip install -r requirements.txt

Start Command:
    gunicorn --workers 1 --threads 4 --timeout 120 app:app

Health Check:
    /healthz

Environment variables:
    DATABASE_URL = your Render PostgreSQL connection URL
    MQTT_BROKER = broker.hivemq.com
    MQTT_PORT = 1883
    MQTT_TOPIC = aqi/node1/readings
    CPCB_CITY = Vadodara
    START_MQTT = 1

IMPORTANT
Use exactly ONE Gunicorn worker.
Otherwise multiple workers could all subscribe to MQTT and duplicate sensor records.

For Render-to-Render database access, prefer the Internal Database URL
when the web service and database are in the same Render region.

The dashboard already links CPCB Prediction to:
https://joej011218-cpu.github.io/CPCB_Prediction/prediction.html

OPTIONAL LOCAL HISTORY MIGRATION
To copy your current local aqi_data.db to PostgreSQL:
1. Put migrate_sqlite_to_postgres.py beside aqi_data.db.
2. Set DATABASE_URL to your database URL.
3. Run:
       python3 migrate_sqlite_to_postgres.py

FILES
app.py
requirements.txt
render.yaml
migrate_sqlite_to_postgres.py
dashboard/index.html
dashboard/analytics.html
dashboard/alerts.html
dashboard/about.html
