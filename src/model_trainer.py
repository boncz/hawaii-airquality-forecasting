"""
ModelTrainer
------------
This module contains the ModelTrainer class used to train, evaluate, and compare
machine learning models for short-term PM2.5 forecasting in Hawaiʻi.

Goals:
- Predict regulatory PM2.5 (AQS) from combined meteorological and sensor data
- Assess PurpleAir bias and correction under volcanic and humid conditions
- Compare model performance across multiple ML algorithms
"""

from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
import joblib


class ModelTrainer:
    """Handles data loading, feature preparation, model training, and evaluation."""

    def __init__(self, data_path: Path | None = None):
        # Default to processed dataset
        self.root = Path(__file__).resolve().parents[1]  # project/
        self.data_path = data_path or (self.root / "data" / "processed" / "merged_all.csv")
        self.data_path = self.data_path.resolve()
        self.models_dir = self.root / "models"
        self.models_dir.mkdir(parents=True, exist_ok=True)

        # Load data immediately
        self.df = self.load_data()

    # ---------------------------------------------------------
    # 1. Load and Prepare Data
    # ---------------------------------------------------------
    def load_data(self) -> pd.DataFrame:
        """Loads the merged dataset and prepares features and target."""
        print(f"Loading data from: {self.data_path}")
        df = pd.read_csv(self.data_path, parse_dates=["datetime_utc"])
        df = df.dropna(subset=["pm25_aqs"])  # Keep only rows with known AQS PM2.5 (target)
        df = df.ffill(limit=5)               # Fill short gaps for meteorological and sensor data
        return df

    def prepare_features(self) -> tuple[pd.DataFrame, pd.Series]:
        """Defines features (X) and target (y) for hourly modeling with engineered predictors."""
        base_cols = [
            "pm25_purpleair", "humidity", "temperature", "pressure",
            "temperature_2m", "relative_humidity_2m",
            "precipitation", "rain", "wind_speed_10m",
            "wind_direction_10m", "wind_gusts_10m"
        ]

        dfX = self.df[base_cols].copy()

        # Basic sanity clips (keeps trees stable)
        if "humidity" in dfX.columns:
            dfX["humidity"] = dfX["humidity"].clip(0, 100)
        if "relative_humidity_2m" in dfX.columns:
            dfX["relative_humidity_2m"] = dfX["relative_humidity_2m"].clip(0, 100)
        for col in [c for c in dfX.columns if "pm25" in c]:
            dfX[col] = dfX[col].clip(lower=0)

        # Wind components + cyclic time
        dfX = self._encode_wind_components(dfX)
        dfX = self._encode_time_cycles(dfX)

        # Lags and rolling windows (past only)
        dfX = self._add_lags_rollups(dfX)

        # Target
        y = self.df["pm25_aqs"].clip(lower=0).copy()

        # Drop rows with NaNs introduced by lags/rollings
        before = len(dfX)
        valid_idx = dfX.dropna().index.intersection(y.dropna().index)
        dfX = dfX.loc[valid_idx]
        y = y.loc[valid_idx]
        print(f"Feature engineering dropped {before - len(dfX)} rows due to lags/rollups.")

        return dfX, y

    # ---------------- Feature engineering helpers ----------------
    def _encode_wind_components(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert wind direction (deg) + speed to Cartesian components u, v."""
        if "wind_direction_10m" in df.columns and "wind_speed_10m" in df.columns:
            theta = np.deg2rad(df["wind_direction_10m"].astype(float))
            speed = df["wind_speed_10m"].astype(float)
            df["wind_u10m"] = speed * np.cos(theta)
            df["wind_v10m"] = speed * np.sin(theta)
        return df

    def _encode_time_cycles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add cyclic hour-of-day and month features from datetime_utc."""
        dt = pd.to_datetime(self.df.loc[df.index, "datetime_utc"])
        hour = dt.dt.hour
        month = dt.dt.month
        df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
        df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
        df["month_sin"] = np.sin(2 * np.pi * month / 12)
        df["month_cos"] = np.cos(2 * np.pi * month / 12)
        return df

    def _add_lags_rollups(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add past lags and rolling stats (uses only past info to avoid leakage)."""
        base_cols = [
            "pm25_purpleair",
            "relative_humidity_2m",
            "temperature_2m",
            "wind_speed_10m",
            "precipitation"
        ]
        for c in base_cols:
            if c not in df.columns:
                continue
            for k in [1, 2, 3, 6]:
                df[f"{c}_lag{k}h"] = df[c].shift(k)
            for w in [3, 6, 12]:
                df[f"{c}_roll{w}h_mean"] = df[c].rolling(window=w, min_periods=w).mean().shift(1)
                df[f"{c}_roll{w}h_std"] = df[c].rolling(window=w, min_periods=w).std().shift(1)
        return df

    # --- Published correction models ---
    def apply_epa_correction(self, X: pd.DataFrame) -> pd.Series:
        """Applies the EPA’s humidity correction formula for PurpleAir."""
        if "pm25_purpleair" not in X.columns or "humidity" not in X.columns:
            raise ValueError("Required columns missing for EPA correction.")
        return 0.524 * X["pm25_purpleair"] - 0.0862 * X["humidity"] + 5.75

    def apply_lrpa_correction(self, X: pd.DataFrame) -> pd.Series:
        """Applies the LRAPA correction model."""
        if "pm25_purpleair" not in X.columns:
            raise ValueError("Required column missing for LRAPA correction.")
        return 0.5 * X["pm25_purpleair"] - 0.66

    # ---------------------------------------------------------
    # 2. Split Train/Test
    # ---------------------------------------------------------
    def train_test_split(self, X, y, test_size=0.2):
        """Splits data into train and test sets (chronologically)."""
        n = len(X)
        split_idx = int(n * (1 - test_size))
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

        print(f"Train size: {len(X_train)}, Test size: {len(X_test)}")
        return X_train, X_test, y_train, y_test

    # ---------------------------------------------------------
    # 3. Model Training
    # ---------------------------------------------------------
    def train_model(self, model_name: str, model, X_train, y_train):
        """Fits a model and saves it to the models directory."""
        print(f"Training model: {model_name}")
        model.fit(X_train, y_train)
        model_path = self.models_dir / f"{model_name}.joblib"
        joblib.dump(model, model_path)
        print(f"Saved model: {model_path}")
        return model

    def train_gbr_tuned(self, X_train, y_train):
        """Time-aware CV + small grid for GBR tuned to MAE."""
        from sklearn.model_selection import TimeSeriesSplit, GridSearchCV

        gbr = GradientBoostingRegressor(random_state=42)
        param_grid = {
            "n_estimators": [300, 600, 900],
            "learning_rate": [0.03, 0.05, 0.1],
            "max_depth": [2, 3, 4],
            "subsample": [0.7, 0.9, 1.0],
            "max_features": ["sqrt", 0.5, None],
        }
        tscv = TimeSeriesSplit(n_splits=5)
        gs = GridSearchCV(
            gbr, param_grid,
            scoring="neg_mean_absolute_error",
            cv=tscv,
            n_jobs=-1,
            verbose=0
        )
        print("Tuning GradientBoostingRegressor with TimeSeriesSplit...")
        gs.fit(X_train, y_train)
        print(f"Best GBR params: {gs.best_params_}")
        return gs.best_estimator_

    # ---------------------------------------------------------
    # 4. Evaluation
    # ---------------------------------------------------------
    def evaluate_model(self, model, X_test, y_test) -> dict:
        """Evaluates the model and returns key metrics."""
        y_pred = model.predict(X_test)
        metrics = {
            "MAE": mean_absolute_error(y_test, y_pred),
            "RMSE": np.sqrt(mean_squared_error(y_test, y_pred)),
            "R2": r2_score(y_test, y_pred),
        }
        print(f"Evaluation: MAE={metrics['MAE']:.3f}, RMSE={metrics['RMSE']:.3f}, R²={metrics['R2']:.3f}")
        return metrics

    def evaluate_static_model(self, y_true: pd.Series, y_pred: pd.Series) -> dict:
        """Evaluate a model that doesn’t train — e.g., published correction formula."""
        mae = mean_absolute_error(y_true, y_pred)
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)
        r2 = r2_score(y_true, y_pred)
        return {"R2": r2, "MAE": mae, "RMSE": rmse}

    # ---------------------------------------------------------
    # 5. Full Pipeline
    # ---------------------------------------------------------
    def run_all_models(self):
        """Runs all selected models, compares performance, and saves results."""
        X, y = self.prepare_features()
        X_train, X_test, y_train, y_test = self.train_test_split(X, y)

        # --- Directories ---
        results_path = self.root / "reports" / "tables"
        preds_dir = self.root / "reports" / "predictions"
        results_path.mkdir(parents=True, exist_ok=True)
        preds_dir.mkdir(parents=True, exist_ok=True)

        # --- Machine learning models ---
        models = {
            "linear_regression": LinearRegression(),
            "random_forest": RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
        }

        results = {}

        # --- Baseline ML models ---
        for name, model in models.items():
            fitted = self.train_model(name, model, X_train, y_train)
            results[name] = self.evaluate_model(fitted, X_test, y_test)
            y_pred = fitted.predict(X_test)
            pd.DataFrame({"y_true": y_test, "y_pred": y_pred}).to_csv(
                preds_dir / f"predictions_{name}.csv", index=False
            )

        # --- Tuned Gradient Boosting ---
        gbr_best = self.train_gbr_tuned(X_train, y_train)
        joblib.dump(gbr_best, self.models_dir / "gradient_boosting_tuned.joblib")
        results["gradient_boosting_tuned"] = self.evaluate_model(gbr_best, X_test, y_test)
        y_pred_gbr = gbr_best.predict(X_test)
        pd.DataFrame({"y_true": y_test, "y_pred": y_pred_gbr}).to_csv(
            preds_dir / "predictions_gradient_boosting_tuned.csv", index=False
        )

        # --- Published correction models ---
        print("\nEvaluating published correction models...")
        y_pred_epa = self.apply_epa_correction(X_test)
        y_pred_lrpa = self.apply_lrpa_correction(X_test)
        results["epa_correction"] = self.evaluate_static_model(y_test, y_pred_epa)
        results["lrpa_correction"] = self.evaluate_static_model(y_test, y_pred_lrpa)

        for name, y_pred in {
            "epa_correction": y_pred_epa,
            "lrpa_correction": y_pred_lrpa
        }.items():
            pd.DataFrame({"y_true": y_test, "y_pred": y_pred}).to_csv(
                preds_dir / f"predictions_{name}.csv", index=False
            )

        # --- Save results summary ---
        results_df = pd.DataFrame(results).T
        results_df.to_csv(results_path / "model_performance.csv")
        print(f"\nModel comparison:\n{results_df}")
        print(f"Results saved to: {results_path / 'model_performance.csv'}")

        return results_df
