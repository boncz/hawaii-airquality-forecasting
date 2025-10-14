"""
Fetches 4 years of historical hourly AirNow data for Hilo, Hawai‘i
using the AirNow API (https://www.airnowapi.org/).

The script retrieves PM2.5 and AQI observations within a 10 km radius
of the specified latitude and longitude, one day at a time, and appends
each day's results to data/raw/airnow/airnow_aqi_all.csv.

Includes retry logic, polite rate-limiting, and checkpointed writes
to safely build a complete multi-year archive for model training.

This script is intended for one-time or occasional use to build
or refresh the historical AirNow dataset. For routine updates,
see airnow_daily.py.
"""


# src/ingestion/airnow.py
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
ROOT = Path(__file__).resolve().parents[2]   # go up to project root
RAW_PATH = ROOT / "data" / "raw" / "airnow" / "airnow_aqi_all.csv"

# Define backfill range (4 years up to 2 days ago to avoid lag)
end_date = date.today() - timedelta(days=2)
start_date = end_date - timedelta(days=365 * 4)
dates = pd.date_range(start=start_date, end=end_date)
start_date = pd.Timestamp(start_date)  # align types for arithmetic

# Requests session with retries & backoff
session = requests.Session()
retries = Retry(
    total=5,                # up to 5 tries
    backoff_factor=1.5,     # 0s, 1.5s, 3s, 4.5s, 6s...
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retries))

os.makedirs(RAW_PATH.parent, exist_ok=True)
print(f"Fetching AirNow hourly data from {start_date.date()} to {end_date}...\n")

# Helper to write/append daily results immediately (checkpoint)
def _append_day(rows: list[dict]):
    if not rows:
        return 0
    df_day = pd.DataFrame(rows)
    write_header = not RAW_PATH.exists()
    df_day.to_csv(RAW_PATH, mode="a", index=False, header=write_header)
    return len(df_day)

for d in dates:
    day_str = d.strftime("%Y-%m-%d")
    daily_records = []

    for hour in range(24):
        url = (
            "https://www.airnowapi.org/aq/observation/latLong/historical/"
            f"?format=application/json&latitude={LAT}&longitude={LON}"
            f"&date={day_str}T{hour:02d}-1000"
            f"&distance={DIST}&API_KEY={key}"
        )

        try:
            r = session.get(url, timeout=60)  # longer timeout
            if r.ok and r.content:
                data = r.json() or []
                # ✅ ensure each record includes correct date/hour
                for rec in data:
                    rec["RequestedDateLocal"] = day_str
                    rec["RequestedHourLocal"] = hour
                    if "HourObserved" not in rec or not rec["HourObserved"]:
                        rec["HourObserved"] = hour
                    if "DateObserved" not in rec or not rec["DateObserved"]:
                        rec["DateObserved"] = day_str
                daily_records.extend(data)
        except requests.exceptions.RequestException as e:
            # Log and continue to next hour
            print(f"⏳ retry/skip: {day_str} {hour:02d}h → {e}")
            time.sleep(2)

        # polite delay between hour calls
        time.sleep(0.5)

    # checkpoint after each day
    if daily_records:
        n = _append_day(daily_records)
        print(f"✅ {day_str}: {n} rows appended")
    else:
        print(f"⚠️ {day_str}: no data returned")

    # longer pause every 50 days to avoid throttling waves
    if (d - start_date).days % 50 == 0 and d != start_date:
        print("⏸️ Pausing briefly to avoid rate limit...")
        time.sleep(10)

print(f"\n💾 Backfill complete. Data accumulating at: {RAW_PATH}")
