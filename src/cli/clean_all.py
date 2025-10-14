"""
CLI script to clean all raw datasets and save cleaned versions into /data/interim.

Usage:
    python cli/clean_all.py

This script imports and applies all cleaning functions from src.ingestion.data_cleaner.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.data_cleaner import (
    clean_airnow,
    clean_aqs,
    clean_purpleair,
    clean_hvo,
    clean_openmeteo
)

# Determine project root (the parent of src/)
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"

# Ensure interim directory exists
INTERIM_DIR.mkdir(parents=True, exist_ok=True)


# Helper to clean + save
def process_dataset(name: str, raw_path: Path, cleaner_func, output_name: str):
    """Load a CSV, clean it, and save to /data/interim."""
    print(f"\n🚀 Cleaning {name} dataset...")
    try:
        df = pd.read_csv(raw_path)
        cleaned = cleaner_func(df)
        out_path = INTERIM_DIR / output_name
        cleaned.to_csv(out_path, index=False)
        print(f"✅ Saved cleaned {name} data → {out_path}")
    except Exception as e:
        print(f"❌ Error processing {name}: {e}")


# Main cleaning sequence
def main():
    print("🔧 Starting full data cleaning pipeline...")

    process_dataset(
        name="AirNow",
        raw_path=RAW_DIR / "airnow" / "airnow_aqi_all.csv",
        cleaner_func=clean_airnow,
        output_name="airnow_clean.csv"
    )

    process_dataset(
        name="AQS",
        raw_path=RAW_DIR / "aqs" / "aqs_all.csv",
        cleaner_func=clean_aqs,
        output_name="aqs_clean.csv"
    )

    process_dataset(
        name="PurpleAir",
        raw_path=RAW_DIR / "purpleair" / "purpleair_all.csv",
        cleaner_func=clean_purpleair,
        output_name="purpleair_clean.csv"
    )

    process_dataset(
        name="HVO",
        raw_path=RAW_DIR / "hvo" / "hvo_status.csv",
        cleaner_func=clean_hvo,
        output_name="hvo_clean.csv"
    )

    process_dataset(
        name="Open-Meteo",
        raw_path=RAW_DIR / "openmeteo" / "openmeteo_hilo_hourly.csv",
        cleaner_func=clean_openmeteo,
        output_name="openmeteo_clean.csv"
    )

    print("\n🎉 All datasets cleaned successfully!")


if __name__ == "__main__":
    main()
