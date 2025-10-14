# src/ingestion/aqs.py
"""
Fetches 4 years of historical PM2.5 data (parameter 88101)
from the EPA AQS API for the state of Hawaii (state code 15).
Appends results to data/raw/aqs/aqs_all.csv, one year at a time.

This script is intended for one-time or occasional use
to build the historical dataset (AQS is slow and not real-time).
"""

import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from pathlib import Path
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Load credentials from .env
load_dotenv()
email = os.getenv("AQS_EMAIL")
key = os.getenv("AQS_KEY")
if not (email and key):
    raise RuntimeError("Missing AQS_EMAIL or AQS_KEY in .env file")

# Constants
BASE_URL = "https://aqs.epa.gov/data/api/sampleData/byState"
STATE_CODE = "15"   # Hawaii
PARAM_CODE = "88101"  # PM2.5 - Local Conditions
YEARS_BACK = 4
DELAY_BETWEEN_CALLS = 10  # seconds between API calls to be polite

# Paths
ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "data" / "raw" / "aqs" / "aqs_all.csv"
os.makedirs(RAW_PATH.parent, exist_ok=True)

# Dates
end_date = date.today() - timedelta(days=2)
start_date = end_date - timedelta(days=365 * YEARS_BACK)
print(f"📆 Fetching AQS hourly data from {start_date} to {end_date}...\n")

# Session with retry logic
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def fetch_chunk(bdate: str, edate: str) -> list[dict]:
    """Fetch data for one time window."""
    url = (
        f"{BASE_URL}?email={email}&key={key}"
        f"&param={PARAM_CODE}&bdate={bdate}&edate={edate}&state={STATE_CODE}"
    )
    try:
        r = session.get(url, timeout=120)
        if r.ok and r.content:
            data = r.json()
            if data.get("Data"):
                return data["Data"]
            else:
                print(f"⚠️ No data returned for {bdate}–{edate}.")
                return []
        else:
            print(f"❌ API error {r.status_code} for {bdate}–{edate}.")
            return []
    except requests.exceptions.RequestException as e:
        print(f"⏳ Request failed ({bdate}–{edate}): {e}")
        return []

def append_to_csv(rows: list[dict]):
    """Append retrieved rows to the CSV file."""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    write_header = not RAW_PATH.exists()
    df.to_csv(RAW_PATH, mode="a", index=False, header=write_header)
    return len(df)

# Main loop – fetch by year to avoid API range limits
years = pd.date_range(start=start_date, end=end_date, freq="YS")
if years[-1].date() != end_date:
    years = years.append(pd.DatetimeIndex([end_date]))

for i in range(len(years) - 1):
    bdate = years[i].strftime("%Y%m%d")
    edate = (years[i + 1] - pd.DateOffset(days=1)).strftime("%Y%m%d")
    print(f"📅 Fetching {bdate} → {edate}...")

    rows = fetch_chunk(bdate, edate)
    if rows:
        n = append_to_csv(rows)
        print(f"✅ Appended {n} rows for {bdate}–{edate}.")
    else:
        print(f"⚠️ No records found for {bdate}–{edate}.")

    print(f"⏸️ Sleeping {DELAY_BETWEEN_CALLS}s before next request...\n")
    time.sleep(DELAY_BETWEEN_CALLS)

print(f"\n💾 Backfill complete. Data saved to: {RAW_PATH}")
