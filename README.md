# Hawaiʻi Air Quality Forecasting

This project builds a fully automated data pipeline and forecasting system for fine particulate matter (PM₂.₅) in Hawaiʻi, where air quality is influenced by both meteorological conditions and volcanic emissions.
It integrates regulatory (EPA AQS), near real-time (AirNow, PurpleAir), meteorological (Open-Meteo), and volcanic (USGS HVO) data sources to generate a unified dataset for hourly air quality prediction and model evaluation.

Beyond forecasting, the project evaluates correction models for low-cost sensors (PurpleAir) under Hawaiʻi’s unique meteorological and volcanic conditions, aiming to improve local awareness and public-health communication.

## Objectives

- Build a reproducible end-to-end data pipeline for ingesting, cleaning, merging, and modeling environmental data.
- Develop machine learning models including Random Forest, Gradient Boosting, and XGBoost for short-term PM₂.₅ forecasting.
- Assess sensor bias and correction reliability for PurpleAir sensors compared to AQS data.
- Quantify how volcanic activity (via HVO alerts) and weather patterns drive PM₂.₅ concentrations across the island.
- Create visualizations and reports for community-facing tools and environmental health outreach.

## Data Sources

| Source | Endpoint / Description | Purpose |
|:--|:--|:--|
| **EPA AQS** | [`sampleData/byState`](https://aqs.epa.gov/aqsweb/documents/data_api.html) | Historical, regulatory-grade PM₂.₅ measurements (ground truth for model training and validation). |
| **AirNow** | [`observation/latLong/historical`](https://docs.airnowapi.org/) | Near-real-time hourly PM₂.₅ data for validation and temporal alignment with forecasts. |
| **Open-Meteo** | [`archive`](https://open-meteo.com/) | Meteorological variables including temperature, humidity, precipitation, wind, and gusts used as model predictors. |
| **USGS HVO** | [`vhpstatus`](https://volcanoes.usgs.gov/hans-public/api/volcano/) | Hourly volcanic activity reports and alert levels for Kīlauea, used to flag eruption episodes and emissions events. |
| **PurpleAir** | [`sensors` and `sensors/{id}/history`](https://community.purpleair.com/t/api-overview/421) | Hourly PM₂.₅ and environmental readings from local low-cost sensors to enhance spatial coverage and evaluate correction models. |

## Project Structure

```
hawaii-airquality-forecasting/
│
├── data/
│   ├── raw/          ← Unprocessed API data (AirNow, AQS, PurpleAir, etc.)
│   ├── interim/      ← Cleaned, standardized datasets
│   └── processed/    ← Fully merged, analysis-ready dataset
│
├── src/
│   ├── ingestion/          ← Individual API ingestion scripts (daily + historical)
│   ├── cli/                ← Command-line orchestrators
│   │   ├── manual_data_update.py
│   │   ├── clean_all.py
│   │   ├── merge_all.py
│   │   ├── train_model.py
│   │   └── rebuild_all.py   ← Full end-to-end pipeline runner
│   │
│   ├── data_cleaner.py      ← Cleaning and standardization functions
│   ├── data_merger.py       ← Combines all sources by datetime_utc
│   ├── model_trainer.py     ← ML training and evaluation logic
│   ├── visualizations.py    ← Plotting and interpretability
│   └── utils/
│       
│
├── reports/
│   ├── figures/             ← Generated plots and visual outputs for analysis and presentation
│   └── literature_review.pdf ← Background research and context for the forecasting project
│
├── presentation.ipynb       ← Final report and visualization notebook combining results, figures, and narrative interpretation
├── .github/workflows/       ← Automated daily and monthly data pulls
├── Makefile                 ← Run tasks like `make rebuild` or `make train`
├── .env.example             ← Template for required API keys and environment variables
└── README.md
```

## Automated Data Pipeline

Data ingestion and updates are handled via GitHub Actions and manual CLI scripts:

| Task | Script | Frequency |
|------|---------|-----------|
| Historical backfill (4 yrs) | `airnow.py`, `aqs.py`, `openmeteo.py`, `purpleair.py` | One-time full pull |
| Incremental updates | `airnow_daily.py`, `aqs_monthly.py`, `openmeteo_recent.py`, `purpleair_daily.py`, `hvo_hourly.py` | Daily or monthly |
| Cleaning | `src/cli/clean_all.py` | Standardizes and de-duplicates |
| Merging | `src/cli/merge_all.py` | Combines all interim datasets |
| Modeling | `src/cli/train_model.py` | Trains and evaluates ML models |
| Full rebuild | `src/cli/rebuild_all.py` | Runs entire pipeline from scratch |

Each workflow writes to `/data` and logs to the Actions console for reproducibility.

## Machine Learning Pipeline

1. **Feature Engineering:** Merge all datasets by `datetime_utc` and align features (meteorology, volcanism, sensor data).
2. **Model Training:** Random Forest, Gradient Boosting, and XGBoost regressors trained on PM₂.₅.
3. **Evaluation:** MAE, RMSE, R², and comparison with persistence and climatological baselines.
4. **Explainability:** SHAP feature importance and volcano-event attribution analysis.
5. **Visualization:** Interactive plots of predictions vs observations and volcanic episode overlays.

## Makefile Commands

| Command | Description |
|:--|:--|
| `make update` | Run all daily API updates |
| `make clean` | Clean and standardize raw data |
| `make merge` | Merge cleaned datasets |
| `make train` | Train and evaluate ML models |
| `make rebuild` | Full pipeline run (historical + incremental) |

## Outputs

- `/data/processed/merged_all.csv` — master dataset with aligned hourly records
- `/models/` — serialized trained models and evaluation metrics
- `/reports/figures/` — visualization outputs for notebooks and presentations

## Reproducibility and Environment

- Requires Python 3.10+
- Dependencies managed in `requirements.txt`
- Environment variables (API keys) stored in `.env`
- Full rebuild reproducible via:
  ```bash
  make rebuild
  ```

## Citation

If used in research, please cite:
> Johnston, A. (2025). *Hawaiʻi Air Quality Forecasting: Integrating Meteorological, Volcanic, and Sensor Data for PM₂.₅ Prediction.*

_Disclaimer: Portions of this README were refined with the assistance of ChatGPT (OpenAI) for clarity and formatting. It will continue to evolve as modeling and visualization features are added._
