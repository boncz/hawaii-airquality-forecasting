"""
purpleair_daily.py
------------------
Daily automated ingestion of PurpleAir sensor data for the past 24 hours
across the defined bounding box around Hilo, Hawai‘i.

This script:
- Fetches all outdoor sensors in the bounding box
- Pulls the most recent 24 hours of hourly data for each
- Appends the results to data/raw/purpleair/purpleair_all.csv

Author: Ailene Johnston
Project: Hawai‘i Air Quality Forecasting
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
    raise RuntimeError("Missing PURPLEAIR_KEY environment variable.")

headers = {"X-API-Key": key}

ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "data" / "raw" / "purpleair" / "purpleair_all.csv"
os.makedirs(RAW_PATH.parent, exist_ok=True)

BBOX = dict(nwlng=-155.5, nwlat=20.2, selng=-154.7, selat=19.3)

# Define time window (past 24 hours)
end_time = datetime.now(timezone.utc)
start_time = end_time - timedelta(days=1)
print(f"Fetching PurpleAir data for {start_time.date()} to {end_time.date()}...\n")

# Get all sensors in region
sensor_url = "https://api.purpleair.com/v1/sensors"
sensor_params = {
    "fields": "sensor_index,name,latitude,longitude",
    "location_type": 0,
    **BBOX,
}

r = requests.get(sensor_url, headers=headers, params=sensor_params, timeout=30)
if r.status_code != 200:
    raise RuntimeError(f"Error fetching sensors: {r.text}")

sensors = pd.DataFrame(r.json()["data"], columns=r.json()["fields"])
print(f"✅ Found {len(sensors)} sensors.\n")

# Fetch and append hourly data for each
for sid in sensors["sensor_index"]:
    url = f"https://api.purpleair.com/v1/sensors/{sid}/history"
    params = dict(
        fields="pm2.5_atm,pm2.5_cf_1,humidity,temperature,pressure",
        start_timestamp=int(start_time.timestamp()),
        end_timestamp=int(end_time.timestamp()),
        average=60,
    )

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 200:
            js = resp.json()
            if js.get("data"):
                df = pd.DataFrame(js["data"], columns=js["fields"])
                df["datetime_utc"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
                df["sensor_index"] = sid
                df["latitude"] = sensors.loc[sensors["sensor_index"] == sid, "latitude"].values[0]
                df["longitude"] = sensors.loc[sensors["sensor_index"] == sid, "longitude"].values[0]
                df["sensor_name"] = sensors.loc[sensors["sensor_index"] == sid, "name"].values[0]

                write_header = not RAW_PATH.exists()
                df.to_csv(RAW_PATH, mode="a", index=False, header=write_header)
                print(f"✅ Appended {len(df)} rows for sensor {sid}.")
            else:
                print(f"⚠️ Sensor {sid}: no new data.")
        else:
            print(f"⚠️ Sensor {sid}: {resp.status_code} - {resp.text[:100]}")
    except Exception as e:
        print(f"⚠️ Sensor {sid} failed: {e}")

    time.sleep(1.5)  # polite delay

print(f"\n💾 Daily PurpleAir update complete. Data appended to: {RAW_PATH}")
