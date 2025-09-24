# Hawaiʻi Air Quality Forecasting  

This project explores methods for forecasting fine particulate matter (PM₂.₅) in Hawaiʻi, where local conditions are shaped by both meteorology and volcanic activity. The goal is to integrate regulatory monitoring (EPA AQS), near real-time feeds (AirNow, PurpleAir), and weather forecasts (Open-Meteo) into a unified system for short-term air quality prediction. In addition to forecasting, the project evaluates existing correction models for PurpleAir sensors — widely used but often biased — to assess their accuracy under Hawaiʻi’s unique volcanic and meteorological conditions. Together, these efforts aim to improve local air quality awareness and provide a foundation for community-facing tools.  
 

## Data Sources   
- [EPA AQS](https://aqs.epa.gov/aqsweb/documents/data_api.html) — historical air quality data (PM₂.₅ and related pollutants)  
- [AirNow](https://docs.airnowapi.org/) — near real-time PM₂.₅ observations  
- [Open-Meteo](https://open-meteo.com/) — meteorological forecasts (wind, humidity, boundary layer height, etc.)  
- [USGS Volcano API](https://volcanoes.usgs.gov/hans-public/api/volcano/) — eruption and alert status for Kīlauea and Mauna Loa  
- [PurpleAir](https://community.purpleair.com/t/api-overview/421) — local low-cost sensor data  
 


## Data Requirements by API — Hawai‘i Air Quality Forecasting

| API / Source | Endpoint / Notes | Variables to Pull | Purpose in Analysis / Role |
|---|---|---|---|
| **AQS (EPA Air Quality System)** | `sampleData/byCounty` or `byState` | `date_local`, `time_local`, `sample_measurement` (µg/m³), `parameter_code`, `state_code`, `county_code`, `site_number`, `latitude`, `longitude`, `method_type` / `method_name` | **Ground truth PM₂.₅** — clean, regulatory standard data used to train & evaluate models and to validate PurpleAir corrections. |
| **AirNow (US EPA / AirNow API)** | `observation/latLong/current` or historical equivalents | `DateObserved`, `HourObserved`, `ParameterName`, `AQI`, `Category.Name`, `StationCode`, `ReportingArea`, `Latitude`, `Longitude` | **Real-time observation / validation** — gives near-current AQ readings to compare forecasts and help validate PurpleAir readings in near real time. |
| **Open-Meteo (Weather / Meteorology API)** | `forecast` or `archive` endpoints | `time`, `temperature_2m`, `relative_humidity_2m`, `precipitation`, `wind_speed_10m`, `wind_direction_10m`, `boundary_layer_height` (if available), plus any others you request (e.g. wind at multiple heights) | **Predictor (feature) variables** — meteorological drivers needed to explain dispersion, accumulation, boundary layer dynamics, humidity effects, etc. |
| **USGS / Volcanoes (HANS / Volcano API / USGS Volcano API)** | `newestForVolcano/{code}`, `getElevatedVolcanoes`, `vhpstatus` endpoints | `volcanoName`, `alertLevel`, `colorCode`, `noticeTime` / `issuedTime`, possibly `onTime` | **Volcanic context** — whether volcano is active / under elevated alert influences PM₂.₅ via vog, SO₂, aerosol emissions. Use this to flag or weight episodes. |
| **PurpleAir (Community Sensors)** | `sensors` endpoint with bounding box / sensor index / history | `sensor_index`, `latitude`, `longitude`, `pm2.5_atm` (or corrected estimates, e.g., `pm2.5_cf_1`), `last_seen`, possibly temperature, humidity, internal sensor metadata (if available) | **Supplemental / validation** — test existing correction models (humidity, bias) and see if adjusted PurpleAir readings align with AQS in Hawaiʻi. Also gives spatial coverage. |



## Planned Features  
- Automated pipelines to pull and refresh data from multiple APIs  
- Preprocessing and cleaning routines to harmonize pollutants, time zones, and units  
- Evaluation of correction models for PurpleAir sensors under Hawaiian meteorological and volcanic conditions  
- Exploratory analysis of volcanic vs. non-volcanic air quality events  
- Machine learning models for short-term PM₂.₅ forecasting  
- Visualization dashboards and reporting to communicate findings  


## Current Status  
This repository is under active development. Initial setup includes a reproducible project structure, version control, and secure handling of API keys.  



---

_Disclaimer: This README was drafted with assistance from ChatGPT (OpenAI) for formatting, overall structure, and typo revision. It will be revised and expanded as the project progresses._


