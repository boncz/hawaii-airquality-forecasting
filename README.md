# Hawaiʻi Air Quality Forecasting  

This project explores methods for forecasting fine particulate matter (PM₂.₅) in Hawaiʻi, with a focus on the unique meteorological and volcanic context of the islands. The aim is to combine regulatory air monitoring data, near real-time feeds, and weather forecasts into an integrated system that can provide short-term air quality predictions.  

## Data Sources  
- EPA AQS — historical air quality data (PM₂.₅ and related pollutants)  
- AirNow — near real-time PM₂.₅ observations  
- Open-Meteo — meteorological forecasts (wind, humidity, boundary layer height, etc.)  
- USGS Volcano API — eruption and alert status for Kīlauea and Mauna Loa  
- PurpleAir (optional) — local low-cost sensor data for supplemental coverage  

## Planned Features  
- Automated pipelines to pull and refresh data from multiple APIs  
- Preprocessing and cleaning routines for harmonizing pollutants, time zones, and units  
- Exploratory analysis of volcanic vs. non-volcanic air quality events  
- Machine learning models for short-term PM₂.₅ forecasting  
- Visualization dashboards and reporting for communicating results  

## Current Status  
This repository is under active development. Initial setup includes a reproducible project structure, version control, and secure handling of API keys.  

---

*Disclaimer: This README was generated with AI as a placeholder and will be updated as the project progresses.*  
