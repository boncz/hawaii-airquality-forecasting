from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import numpy as np


@dataclass
class DataPaths:
    root: Path

    @property
    def data(self) -> Path:
        return self.root / "data"

    @property
    def interim(self) -> Path:
        return self.data / "interim"

    @property
    def processed(self) -> Path:
        return self.data / "processed"

    # cleaned inputs (produced by clean_all.py)
    @property
    def airnow(self) -> Path:
        return self.interim / "airnow_clean.csv"

    @property
    def aqs(self) -> Path:
        return self.interim / "aqs_clean.csv"

    @property
    def purpleair(self) -> Path:
        return self.interim / "purpleair_clean.csv"

    @property
    def openmeteo(self) -> Path:
        return self.interim / "openmeteo_clean.csv"

    @property
    def hvo(self) -> Path:
        return self.interim / "hvo_clean.csv"

    # output
    @property
    def merged(self) -> Path:
        return self.processed / "merged_all.csv"


class DataMerger:
    """
    Merge all cleaned sources (hourly, UTC) into a single dataframe.
    - AQS: pm25_aqs (target)
    - AirNow: aqi_airnow (PM2.5 only)
    - PurpleAir: hourly median pm2.5_atm per hour + sensor count
    - Open-Meteo: weather features
    - HVO: encoded alert flags
    """

    def __init__(self, project_root: Path | None = None):
        self.paths = DataPaths(project_root or Path(__file__).resolve().parents[1])
        self.paths.processed.mkdir(parents=True, exist_ok=True)

    # ---------- loaders ----------
    def _read(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            print(f"!! Missing: {path}")
            return pd.DataFrame()
        df = pd.read_csv(path)
        if "datetime_utc" in df.columns:
            df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
        return df

    def load_all(self) -> dict[str, pd.DataFrame]:
        return {
            "aqs": self._read(self.paths.aqs),
            "airnow": self._read(self.paths.airnow),
            "purpleair": self._read(self.paths.purpleair),
            "openmeteo": self._read(self.paths.openmeteo),
            "hvo": self._read(self.paths.hvo),
        }

    # ---------- per-source shaping ----------
    def prep_aqs(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Only PM2.5 (AQS param 88101 is typical; but keep any PM2.5 rows already filtered in cleaner)
        # Expect 'sample_measurement' as the value column.
        out = df.loc[:, ["datetime_utc", "sample_measurement"]].rename(columns={
            "sample_measurement": "pm25_aqs"
        })
        return out

    def prep_airnow(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Keep the relevant columns
        keep_cols = ["datetime_utc", "AQI"]
        out = df.loc[:, keep_cols].rename(columns={"AQI": "aqi_airnow"})
        return out

    def prep_purpleair(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Keep key variables for bias analysis and modeling
        keep_cols = [
            "datetime_utc",
            "pm2.5_atm",
            "humidity",
            "temperature",
            "pressure"
    ]
        out = df.loc[:, keep_cols].rename(columns={"pm2.5_atm": "pm25_purpleair"})
        return out

    def prep_openmeteo(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Keep only relevant meteorological variables (doesn't include location data)
        keep_cols = [
            "datetime_utc",
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "rain",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m"
        ]
        out = df.loc[:, keep_cols]
        return out

    def prep_hvo(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        keep_cols = [
        "datetime_utc",
        "alertLevel",
        "colorCode",
        "alert_change",
        "color_change"
        ]
        out = df.loc[:,keep_cols]
        return out

    # ---------- merge orchestration ----------
    def build_hourly_index(self, frames: list[pd.DataFrame]) -> pd.DatetimeIndex:
         """Construct a continuous hourly UTC time index across all datasets."""
         
         # Combine min/max times from each dataframe
         start = min(df["datetime_utc"].min() for df in frames)
         end = max(df["datetime_utc"].max() for df in frames)

         # Build an hourly range from start to end (inclusive)
         return pd.date_range(start=start, end=end, freq="H", tz="UTC")

    def merge_all(self) -> pd.DataFrame:
        raw = self.load_all()

        aqs = self.prep_aqs(raw["aqs"])
        airnow = self.prep_airnow(raw["airnow"])
        purpleair = self.prep_purpleair(raw["purpleair"])
        openmeteo = self.prep_openmeteo(raw["openmeteo"])
        hvo = self.prep_hvo(raw["hvo"])

        frames = [aqs, airnow, purpleair, openmeteo, hvo]
        idx = self.build_hourly_index(frames)
        base = pd.DataFrame({"datetime_utc": idx})

        def left_join(base_df: pd.DataFrame, add_df: pd.DataFrame) -> pd.DataFrame:
            if add_df.empty:
                return base_df
            return base_df.merge(add_df, on="datetime_utc", how="left")

        merged = base.pipe(left_join, aqs)\
                     .pipe(left_join, airnow)\
                     .pipe(left_join, purpleair)\
                     .pipe(left_join, openmeteo)\
                     .pipe(left_join, hvo)

        # final sort & de-dup (paranoia)
        merged = (merged
                  .drop_duplicates(subset=["datetime_utc"])
                  .sort_values("datetime_utc")
                  .reset_index(drop=True))

        return merged

    def save(self, df: pd.DataFrame) -> Path:
        self.paths.processed.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.paths.merged, index=False)
        return self.paths.merged


if __name__ == "__main__":
    merger = DataMerger()
    out = merger.merge_all()
    path = merger.save(out)
    print(f"✅ Merged dataset: {out.shape[0]} rows, {out.shape[1]} columns")
    print(f"💾 Saved to: {path}")
    # quick coverage check
    coverage = out.notna().mean().sort_values(ascending=False)
    print("\nNon-null coverage (fraction):")
    print(coverage.to_string())

