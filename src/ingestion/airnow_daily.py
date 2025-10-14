# src/ingestion/airnow_daily.py
import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

load_dotenv()

# Load API key
key = os.getenv("AIRNOW_KEY")
if not key:
    raise RuntimeError("Missing AIRNOW_KEY environment variable. Did you load your .env file?")

# Parameters
LAT, LON, DIST = 19.7297, -155.09, 10
ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "data" / "raw" / "airnow" / "airnow_aqi_all.csv"

# Create directory if needed
os.makedirs(RAW_PATH.parent, exist_ok=True)

# Define the day to pull (yesterday)
end_date = date.today() - timedelta(days=2)  # AirNow delay safety buffer
start_date = end_date
print(f"📆 Fetching AirNow hourly data for {start_date}...\n")

# Requests session with retries & backoff
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def _append_day(rows: list[dict]):
    if not rows:
        return 0
    df_day = pd.DataFrame(rows)
    write_header = not RAW_PATH.exists()
    df_day.to_csv(RAW_PATH, mode="a", index=False, header=write_header)
    return len(df_day)

day_str = start_date.strftime("%Y-%m-%d")
daily_records = []

for hour in range(24):
    url = (
        "https://www.airnowapi.org/aq/observation/latLong/historical/"
        f"?format=application/json&latitude={LAT}&longitude={LON}"
        f"&date={day_str}T{hour:02d}-1000"
        f"&distance={DIST}&API_KEY={key}"
    )

    try:
        r = session.get(url, timeout=60)
        if r.ok and r.content:
            data = r.json() or []
            for rec in data:
                rec["RequestedDateLocal"] = day_str
                rec["RequestedHourLocal"] = hour
                if "HourObserved" not in rec or not rec["HourObserved"]:
                    rec["HourObserved"] = hour
                if "DateObserved" not in rec or not rec["DateObserved"]:
                    rec["DateObserved"] = day_str
            daily_records.extend(data)
    except requests.exceptions.RequestException as e:
        print(f"⏳ retry/skip: {day_str} {hour:02d}h → {e}")
        time.sleep(2)

    time.sleep(0.5)

if daily_records:
    n = _append_day(daily_records)
    print(f"✅ {day_str}: {n} rows appended")
else:
    print(f"⚠️ {day_str}: no data returned")

print(f"\n💾 Daily update complete. Data accumulated at: {RAW_PATH}")
