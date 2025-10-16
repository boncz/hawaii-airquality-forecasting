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

        # Keep only rows with known AQS PM2.5 (target)
        df = df.dropna(subset=["pm25_aqs"])

        # Fill short gaps for meteorological and sensor data
        df = df.ffill(limit=5)

        return df

    def prepare_features(self) -> tuple[pd.DataFrame, pd.Series]:
        """Defines features (X) and target (y) for modeling."""
        feature_cols = [
            # PurpleAir + Environment
            "pm25_purpleair", "humidity", "temperature", "pressure",
            # Meteorology
            "temperature_2m", "relative_humidity_2m",
            "precipitation", "rain", "wind_speed_10m",
            "wind_direction_10m", "wind_gusts_10m"
        ]

        X = self.df[feature_cols].copy()
        y = self.df["pm25_aqs"].copy()

        # Drop rows where features still have NaN after forward-fill
        missing_before = len(X) - X.dropna().shape[0]
        X = X.dropna()
        y = y.loc[X.index]

        print(f"Dropped {missing_before} rows with remaining NaNs after forward-fill.")
        print(f"Prepared features: {len(feature_cols)} columns, {len(X)} rows")
        return X, y

    
        # --- Published correction models ---
    def apply_epa_correction(self, X: pd.DataFrame) -> pd.Series:
        """
        Applies the EPA’s humidity correction formula for PurpleAir.
        Formula reference: 0.524 * PM2.5_raw - 0.0862 * RH + 5.75
        """
        if "pm25_purpleair" not in X.columns or "humidity" not in X.columns:
            raise ValueError("Required columns missing for EPA correction.")
        return 0.524 * X["pm25_purpleair"] - 0.0862 * X["humidity"] + 5.75

    def apply_lrpa_correction(self, X: pd.DataFrame) -> pd.Series:
        """
        Applies the LRAPA correction model (used in Oregon and elsewhere).
        Formula reference: 0.5 * PM2.5_raw - 0.66
        """
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

        #  --- Machine learning models ---
        models = {
            "linear_regression": LinearRegression(),
            "random_forest": RandomForestRegressor(n_estimators=100, random_state=42),
            "gradient_boosting": GradientBoostingRegressor(random_state=42),
        }

        results = {}

        # --- Trained ML models ---
        for name, model in models.items():
            fitted = self.train_model(name, model, X_train, y_train)
            results[name] = self.evaluate_model(fitted, X_test, y_test)

        # --- Published correction models ---
        print("\nEvaluating published correction models...")

        y_pred_epa = self.apply_epa_correction(X_test)
        y_pred_lrpa = self.apply_lrpa_correction(X_test)

        results["epa_correction"] = self.evaluate_static_model(y_test, y_pred_epa)
        results["lrpa_correction"] = self.evaluate_static_model(y_test, y_pred_lrpa)

         # --- Save results ---
        results_df = pd.DataFrame(results).T
        results_path = self.root / "reports" / "tables"
        results_path.mkdir(parents=True, exist_ok=True)
        results_df.to_csv(results_path / "model_performance.csv")

        print(f"\nModel comparison:\n{results_df}")
        print(f"Results saved to: {results_path / 'model_performance.csv'}")

        return results_df

