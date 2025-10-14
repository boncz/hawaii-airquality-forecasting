# src/ingestion/aqs_monthly.py

"""
Incremental monthly updater for AQS PM2.5 (parameter 88101) data.

This script retrieves data for the past 120 days (4 months) from the EPA AQS API
and appends any new or updated rows to data/raw/aqs/aqs_all.csv.

Because AQS data submissions lag by several weeks to months,
a 4-month overlap window ensures newly available records are captured
without refetching the full historical dataset.

Intended to be scheduled monthly via GitHub Actions.
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

load_dotenv()

email = os.getenv("AQS_EMAIL")
key = os.getenv("AQS_KEY")
if not (email and key):
    raise RuntimeError("Missing AQS_EMAIL or AQS_KEY in .env file")

BASE_URL = "https://aqs.epa.gov/data/api/sampleData/byState"
STATE_CODE = "15"       # Hawaii
PARAM_CODE = "88101"    # PM2.5 - Local Conditions
OVERLAP_DAYS = 120      # re-fetch 4 months back each run
CHUNK_DAYS = 30         # fetch in monthly chunks to avoid range limits
PAUSE_SEC = 5           # between API calls

ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "data" / "raw" / "aqs" / "aqs_all.csv"
os.makedirs(RAW_PATH.parent, exist_ok=True)

end_date = date.today() - timedelta(days=2)
if RAW_PATH.exists():
    df = pd.read_csv(RAW_PATH, parse_dates=["date_local"])
    latest = df["date_local"].max().date()
    start_date = latest - timedelta(days=OVERLAP_DAYS)
    print(f"🔍 Existing data through {latest}.")
else:
    start_date = end_date - timedelta(days=365)
    print("📂 No existing AQS file found. Starting with 1-year window.")

if start_date > end_date:
    print("✅ Data already up to date.")
    exit()

print(f"📆 Fetching AQS data from {start_date} to {end_date}...\n")

session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def fetch_chunk(bdate: str, edate: str) -> list[dict]:
    url = (
        f"{BASE_URL}?email={email}&key={key}"
        f"&param={PARAM_CODE}&bdate={bdate}&edate={edate}&state={STATE_CODE}"
    )
    try:
        r = session.get(url, timeout=120)
        if r.ok and r.content:
            data = r.json()
            return data.get("Data", [])
        else:
            print(f"❌ API error {r.status_code} for {bdate}–{edate}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"⏳ Request failed {bdate}–{edate}: {e}")
        return []

def append_unique(rows: list[dict]):
    if not rows:
        return 0
    df_new = pd.DataFrame(rows)
    if RAW_PATH.exists():
        df_exist = pd.read_csv(RAW_PATH, parse_dates=["date_local"])
        df_comb = pd.concat([df_exist, df_new], ignore_index=True)
        df_comb.drop_duplicates(
            subset=["state_code", "county_code", "site_number",
                    "date_local", "time_local", "parameter_code"],
            inplace=True
        )
        df_comb.to_csv(RAW_PATH, index=False)
        return len(df_comb) - len(df_exist)
    else:
        df_new.to_csv(RAW_PATH, index=False)
        return len(df_new)

current = start_date
while current <= end_date:
    chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), end_date)
    bdate = current.strftime("%Y%m%d")
    edate = chunk_end.strftime("%Y%m%d")
    print(f"📅 Fetching {bdate} → {edate}...")
    rows = fetch_chunk(bdate, edate)
    if rows:
        n = append_unique(rows)
        print(f"✅ Added {n} new/updated rows for {bdate}–{edate}.")
    else:
        print(f"⚠️ No records for {bdate}–{edate}.")
    time.sleep(PAUSE_SEC)
    current = chunk_end + timedelta(days=1)

print(f"\n💾 Monthly update complete. Data saved to {RAW_PATH}")
