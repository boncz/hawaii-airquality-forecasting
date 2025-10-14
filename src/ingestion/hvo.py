"""
Fetches the current volcano status for Kīlauea (USGS HVO)
and logs it hourly for historical tracking.

Because the HVO endpoint only provides current status, this script
builds an hourly archive by recording the pull time (timestamp_utc)
along with HVO’s internal alert timestamp (alertDate).

Each entry includes a flag (new_notice) indicating whether the
noticeId differs from the previous record, making it easy to
identify actual status changes.
"""

import os
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

# Parameters
VOLCANO_ID = "332010"  # Kilauea
ROOT = Path(__file__).resolve().parents[2]
RAW_PATH = ROOT / "data" / "raw" / "hvo" / "hvo_status.csv"

os.makedirs(RAW_PATH.parent, exist_ok=True)
URL = f"https://volcanoes.usgs.gov/vsc/api/volcanoApi/vhpstatus/{VOLCANO_ID}"

# Fetch current status
r = requests.get(URL, timeout=20)
if not r.ok:
    raise RuntimeError(f"Request failed: {r.status_code}")
data = r.json()

# Prepare record
timestamp_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

record = {
    "timestamp_utc": timestamp_utc,
    "alertDate": data.get("alertDate"),
    "colorDate": data.get("colorDate"),
    "alertLevel": data.get("alertLevel"),
    "colorCode": data.get("colorCode"),
    "noticeSynopsis": data.get("noticeSynopsis"),
    "noticeId": data.get("noticeId"),
    "noticeUrl": data.get("noticeUrl"),
    "vName": data.get("vName"),
    "nvewsThreat": data.get("nvewsThreat"),
}

# Load existing CSV (if any)
if RAW_PATH.exists():
    df = pd.read_csv(RAW_PATH)
    last_notice = df["noticeId"].iloc[-1] if not df.empty else None
    record["new_notice"] = record["noticeId"] != last_notice
else:
    record["new_notice"] = True  # first run

# Append to CSV
df_new = pd.DataFrame([record])
write_header = not RAW_PATH.exists()
df_new.to_csv(RAW_PATH, mode="a", index=False, header=write_header)

# Optional console print for logs
status = "🟢 NEW NOTICE" if record["new_notice"] else "🟡 No change"
print(f"{status} at {timestamp_utc} — Alert: {record['alertLevel']} ({record['colorCode']})")
