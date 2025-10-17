# src/utils/append_hvo.py
import pandas as pd
from datetime import datetime
import os

# File paths
DATA_DIR = "data/hvo/"
RAW_FILE = os.path.join(DATA_DIR, "hvo_latest.csv")
ARCHIVE_FILE = os.path.join(DATA_DIR, "hvo_full_history.csv")

def append_hvo_data():
    """Append the latest HVO data to the full historical dataset."""
    if not os.path.exists(RAW_FILE):
        print(f"No new file found at {RAW_FILE}")
        return

    # Read new data
    new_df = pd.read_csv(RAW_FILE)

    # Read existing file (if it exists)
    if os.path.exists(ARCHIVE_FILE):
        existing_df = pd.read_csv(ARCHIVE_FILE)
        combined = pd.concat([existing_df, new_df]).drop_duplicates()
    else:
        combined = new_df

    # Save combined file
    combined.to_csv(ARCHIVE_FILE, index=False)
    print(f"✅ HVO data appended successfully at {datetime.now()}")

if __name__ == "__main__":
    append_hvo_data()
