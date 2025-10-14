"""
Fetches and compiles historical hourly PurpleAir sensor data for the past 4 years
across the defined bounding box around the Hilo region of Hawai‘i.

This script:
- Queries all outdoor sensors within the geographic bounding box
- Iteratively requests hourly historical data (in 30-day chunks per sensor)
- Appends results incrementally to data/raw/purpleair/purpleair_all.csv
- Includes environmental fields (humidity, temperature, pressure)

"""

import os
import time
import math
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Setup
load_dotenv()
key = os.getenv("PURPLEAIR_KEY")
if not key:
    raise RuntimeError("Missing PURPLEAIR_KEY environment variable. Check .env file.")

headers = {"X-API-Key": key}

ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "data" / "raw" / "purpleair" / "purpleair_all.csv"
os.makedirs(RAW_PATH.parent, exist_ok=True)

# Define region
BBOX = dict(nwlng=-155.5, nwlat=20.2, selng=-154.7, selat=19.3)

# Define time window (4 years ago to now)
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=365 * 4)
print(f"Fetching PurpleAir data from {start_time.date()} to {end_time.date()}...\n")


# Get all sensors in the region
sensor_url = "https://api.purpleair.com/v1/sensors"
sensor_params = {
    "fields": "sensor_index,name,latitude,longitude",
    "location_type": 0,  # outdoor only
    **BBOX,
}

resp = requests.get(sensor_url, headers=headers, params=sensor_params, timeout=30)
if resp.status_code != 200:
    raise RuntimeError(f"Error fetching sensors: {resp.text}")

sensors = pd.DataFrame(resp.json()["data"], columns=resp.json()["fields"])
print(f"✅ Found {len(sensors)} sensors in bounding box.\n")


# Function to fetch and append data per sensor
def fetch_sensor_history(sensor_id: int, start: datetime, end: datetime):
    """Fetch hourly historical data for a single sensor in 30-day chunks."""
    url = f"https://api.purpleair.com/v1/sensors/{sensor_id}/history"
    all_rows = []

    chunk_days = 30
    total_days = (end - start).days
    chunks = math.ceil(total_days / chunk_days)

    for i in range(chunks):
        chunk_start = start + timedelta(days=i * chunk_days)
        chunk_end = min(chunk_start + timedelta(days=chunk_days), end)

        params = dict(
            fields="pm2.5_atm,pm2.5_cf_1,humidity,temperature,pressure",
            start_timestamp=int(chunk_start.timestamp()),
            end_timestamp=int(chunk_end.timestamp()),
            average=60,  # hourly
        )

        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            if r.status_code == 200:
                js = r.json()
                if js.get("data"):
                    df_chunk = pd.DataFrame(js["data"], columns=js["fields"])
                    df_chunk["sensor_index"] = sensor_id
                    df_chunk["datetime_utc"] = pd.to_datetime(df_chunk["time_stamp"], unit="s", utc=True)
                    all_rows.append(df_chunk)
            else:
                print(f"⚠️ Sensor {sensor_id} chunk {i+1}/{chunks}: {r.status_code} - {r.text[:80]}")
        except Exception as e:
            print(f"⚠️ Sensor {sensor_id} chunk {i+1}/{chunks} failed: {e}")

        time.sleep(2.5)  # polite delay

    if not all_rows:
        return None

    df_all = pd.concat(all_rows, ignore_index=True)
    df_all["latitude"] = sensors.loc[sensors["sensor_index"] == sensor_id, "latitude"].values[0]
    df_all["longitude"] = sensors.loc[sensors["sensor_index"] == sensor_id, "longitude"].values[0]
    df_all["sensor_name"] = sensors.loc[sensors["sensor_index"] == sensor_id, "name"].values[0]
    return df_all



# Loop through all sensors and append results
for sid in sensors["sensor_index"]:
    print(f"Fetching data for sensor {sid}...")
    df_sensor = fetch_sensor_history(sid, start_time, end_time)
    if df_sensor is not None and not df_sensor.empty:
        write_header = not RAW_PATH.exists()
        df_sensor.to_csv(RAW_PATH, mode="a", index=False, header=write_header)
        print(f"✅ Sensor {sid}: {len(df_sensor)} rows appended.\n")
    else:
        print(f"⚠️ Sensor {sid}: No data returned.\n")

print(f"💾 Completed PurpleAir historical backfill. Output file: {RAW_PATH}")
