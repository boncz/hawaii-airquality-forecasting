"""
Manual trigger for all automated ingestion scripts.
Run this locally to update raw data across all sources at once.

Includes:
    • AirNow (daily)
    • AQS (monthly)
    • HVO (hourly status)
    • Open-Meteo (recent)
    • PurpleAir (daily)

Each script appends new records to its respective raw CSV file under /data/raw/.
Use this to quickly refresh the local dataset without waiting for scheduled
GitHub Actions.

Author: Ailene Johnston
Project: Hawai‘i Air Quality Forecasting
"""

import subprocess
import sys
from pathlib import Path


# Define paths
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "ingestion"

# List of ingestion scripts (executed in order)
SCRIPTS = [
    SRC / "airnow_daily.py",
    SRC / "aqs_monthly.py",
    SRC / "hvo.py",
    SRC / "openmeteo_recent.py",
    SRC / "purpleair_daily.py",
]

# Helper function
def run_script(script_path: Path):
    """Run a Python ingestion script and print its output live."""
    print(f"\n{'='*60}")
    print(f"▶ Running {script_path.name}...")
    print(f"{'='*60}")
    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
        print(f"✅ {script_path.name} completed successfully.\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ {script_path.name} failed with error code {e.returncode}.\n")

# Main routine
def main():
    print("\n📡 Starting manual data update for all ingestion scripts...\n")
    for script in SCRIPTS:
        if script.exists():
            run_script(script)
        else:
            print(f"⚠️ Skipping missing script: {script.name}")
    print("\n🌺 All ingestion scripts finished.\n")

if __name__ == "__main__":
    main()
