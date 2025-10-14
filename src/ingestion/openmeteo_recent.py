"""
Incremental updater for hourly ERA5 weather data (Hilo, Hawaii).

 - Checks the last timestamp in openmeteo_hilo_hourly.csv
 - Pulls any missing hourly data since that date
 - Accounts for ERA5's ~5-day data lag
 - Appends new rows and re-saves the full file
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import openmeteo_requests
from retry_requests import retry
import requests


# Setup
BASE_URL = "https://archive-api.open-meteo.com/v1/era5"
DATA_DIR = "data/raw/openmeteo"
INFILE = os.path.join(DATA_DIR, "openmeteo_hilo_hourly.csv")
OUTFILE = INFILE

LAT, LON = 19.7297, -155.09
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m"
]


# Friendly Timing
session = requests.Session()
retry_session = retry(session, retries=5, backoff_factor=0.3)
openmeteo = openmeteo_requests.Client(session=retry_session)


def fetch_chunk(start, end):
    """Fetch one chunk of hourly weather data."""
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start.strftime("%Y-%m-%d"),
        "end_date": end.strftime("%Y-%m-%d"),
        "hourly": HOURLY_VARS,
        "timezone": "Pacific/Honolulu"
    }

    try:
        response = openmeteo.weather_api(BASE_URL, params=params)[0]
    except Exception as e:
        print(f"⚠️  Failed for {start.date()} → {end.date()}: {e}")
        return None

    hourly = response.Hourly()
    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    )

    data = {"datetime_utc": times}
    for i, var in enumerate(HOURLY_VARS):
        data[var] = hourly.Variables(i).ValuesAsNumpy()

    df = pd.DataFrame(data)
    df["latitude"] = LAT
    df["longitude"] = LON
    df["elevation_m"] = response.Elevation()
    return df


def main():
    print("🔄 Updating Open-Meteo data for Hilo...")

    if not os.path.exists(INFILE):
        print("❌ Historical file not found. Run pull_openmeteo.py first.")
        return

    existing = pd.read_csv(INFILE, parse_dates=["datetime_utc"])
    last_date = existing["datetime_utc"].max()
    print(f"📅 Last data point: {last_date}")

    # Start from the next hour after last record
    start_date = (last_date + pd.Timedelta(hours=1)).tz_localize(None)

    # ERA5 reanalysis lags ~5 days behind current date
    end_date = (datetime.utcnow() - timedelta(days=5))
    if start_date >= end_date:
        print("✅ No new data available yet (ERA5 lag ~5 days).")
        return

    print(f"→ Fetching {start_date.date()} → {end_date.date()}")

    # Pull by 31-day chunks to stay safe
    new_data = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + pd.DateOffset(days=31), end_date)
        df = fetch_chunk(current, chunk_end)
        if df is not None:
            new_data.append(df)
        current = chunk_end

    if not new_data:
        print("⚠️ No new data returned.")
        return

    new_df = pd.concat(new_data, ignore_index=True)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined.drop_duplicates(subset=["datetime_utc"], inplace=True)
    combined.sort_values("datetime_utc", inplace=True)
    combined.reset_index(drop=True, inplace=True)

    combined.to_csv(OUTFILE, index=False)
    print(f"✅ Updated file saved — now contains {len(combined):,} total rows.")


if __name__ == "__main__":
    main()
