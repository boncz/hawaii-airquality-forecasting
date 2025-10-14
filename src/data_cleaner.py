"""
This module contains functions to clean raw environmental datasets
(AirNow, AQS, etc.) into standardized, analysis-ready formats.

All cleaning functions:
- Convert local or GMT times into a standardized UTC column: 'datetime_utc'
- Remove unnecessary or redundant fields
- Handle nulls and duplicates consistently
- Output data suitable for merging into the final processed dataset
"""

import pandas as pd


# AIRNOW CLEANER
def clean_airnow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean AirNow data:
    - Add proper headers if missing
    - Convert date/time columns to UTC
    - Drop redundant columns and duplicates
    - Standardize column names
    """
    print("🔹 Cleaning AirNow data...")

    # If no headers (common in AirNow flat files)
    if list(df.columns)[0] == df.iloc[0, 0]:
        print("Detected missing headers — adding manually.")
        df.columns = [
            "DateObserved", "HourObserved", "LocalTimeZone", "ReportingArea",
            "StateCode", "Latitude", "Longitude", "ParameterName", "AQI",
            "Category", "DateLocal", "HourLocal"
        ]

    # Convert date/hour to UTC datetime
    df["datetime_utc"] = pd.to_datetime(
        df["DateObserved"] + " " + df["HourObserved"].astype(str),
        errors="coerce",
        utc=True
    )

    # Drop unused columns
    df = df.drop(
        columns=[
            "DateObserved", "HourObserved", "DateLocal",
            "HourLocal", "LocalTimeZone"
        ],
        errors="ignore"
    )

    # Drop duplicates
    df = df.drop_duplicates(subset=["ReportingArea", "ParameterName", "datetime_utc"])

    # Sort chronologically
    df = df.sort_values("datetime_utc").reset_index(drop=True)

    print(f"✅ AirNow cleaned: {df.shape[0]} rows, {df.shape[1]} columns")
    return df.reset_index(drop=True)



# AQS CLEANER
def clean_aqs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean AQS EPA data:
    - Standardize datetime to UTC
    - Drop redundant or sparse fields
    - Handle nulls and qualifiers
    - Deduplicate and sort
    """
    print("🔹 Cleaning AQS data...")

    # Combine GMT date/time into UTC datetime
    df["datetime_utc"] = pd.to_datetime(
        df["date_gmt"] + " " + df["time_gmt"],
        errors="coerce",
        utc=True
    )

    # Drop redundant or low-value columns
    df = df.drop(
        columns=[
            "date_local", "time_local", "date_gmt", "time_gmt",
            "uncertainty", "datum", "units_of_measure_code", "sample_duration_code"
        ],
        errors="ignore"
    )

    # Fill missing qualifiers
    df["qualifier"] = df["qualifier"].fillna("None")

    # Deduplicate and sort
    df = df.drop_duplicates(
        subset=["state_code", "county_code", "site_number", "datetime_utc", "parameter_code"]
    ).sort_values("datetime_utc").reset_index(drop=True)

    # Ensure hourly consistency
    df["datetime_utc"] = df["datetime_utc"].dt.floor("h")

    print(f"✅ AQS cleaned: {df.shape[0]} rows, {df.shape[1]} columns")
    return df.reset_index(drop=True)

# PURPLEAIR CLEANER
def clean_purpleair(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean PurpleAir sensor data:
    - Converts epoch timestamps to UTC datetime
    - Removes duplicates and sorts chronologically
    - Keeps all fields provided by the API
    - Ensures 'datetime_utc' exists for merging consistency
    - Floors timestamps to nearest hour
    """
    print("🔹 Cleaning PurpleAir data...")

    # Convert timestamp(s) to UTC datetime
    if "time_stamp" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["time_stamp"], unit="s", utc=True)
    elif "last_seen" in df.columns:
        df["datetime_utc"] = pd.to_datetime(df["last_seen"], unit="s", utc=True)
    else:
        raise ValueError("❌ Expected a 'time_stamp' or 'last_seen' column in the data.")

    # Floor timestamps to the nearest hour for consistent merging
    df["datetime_utc"] = df["datetime_utc"].dt.floor("h")

    # Drop redundant time fields
    df = df.drop(columns=["time_stamp", "last_seen"], errors="ignore")

    # Deduplicate by sensor + timestamp
    df = df.drop_duplicates(subset=["sensor_index", "datetime_utc"])

    # Sort chronologically
    df = df.sort_values("datetime_utc").reset_index(drop=True)

    print(f"✅ PurpleAir cleaned: {df.shape[0]} rows, {df.shape[1]} columns")
    return df.reset_index(drop=True)

# HVO CLEANER
def clean_hvo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean HVO volcano status data:
    - Converts timestamps to UTC datetimes
    - Ensures consistent datetime_utc column
    - Deduplicates by noticeId
    - Flags changes in alert level or color code
    """
    print("🔹 Cleaning HVO data...")

    # Convert relevant time fields to datetime (they’re already UTC)
    for col in ["timestamp_utc", "alertDate", "colorDate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Use timestamp_utc as canonical UTC time
    df["datetime_utc"] = df["timestamp_utc"]
    df = df.drop(columns=["timestamp_utc"], errors="ignore")

    # Deduplicate by noticeId and sort chronologically
    df = df.drop_duplicates(subset=["noticeId"]).sort_values("datetime_utc").reset_index(drop=True)

    # Flag alert/color changes
    df["alert_change"] = df["alertLevel"].ne(df["alertLevel"].shift())
    df["color_change"] = df["colorCode"].ne(df["colorCode"].shift())

    print(f"✅ HVO cleaned: {df.shape[0]} rows, {df.shape[1]} columns")
    return df.reset_index(drop=True)

# OPENMETEO CLEANER
def clean_openmeteo(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Open-Meteo weather data:
    - Confirms datetime_utc is proper UTC datetime
    - Enforces numeric types for meteorological variables
    - Deduplicates and sorts chronologically
    - Drops rows missing all key weather measurements
    """
    print("🔹 Cleaning Open-Meteo data...")

    # Ensure datetime_utc is proper datetime type
    if "datetime_utc" not in df.columns:
        raise ValueError("❌ Expected 'datetime_utc' column in Open-Meteo data.")
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce", utc=True)

    # Convert numeric columns
    num_cols = [
        "temperature_2m", "relative_humidity_2m", "precipitation", "rain",
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
        "latitude", "longitude", "elevation_m"
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where all key weather variables are missing
    core_weather = [
        "temperature_2m", "relative_humidity_2m",
        "precipitation", "wind_speed_10m"
    ]
    df = df.dropna(subset=[c for c in core_weather if c in df.columns], how="all")

    # Remove duplicates and sort chronologically
    df = df.drop_duplicates(subset=["datetime_utc"]).sort_values("datetime_utc").reset_index(drop=True)

    print(f"✅ Open-Meteo cleaned: {df.shape[0]} rows, {df.shape[1]} columns")
    return df.reset_index(drop=True)