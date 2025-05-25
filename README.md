
# Project DSI321: Real-Time Data Pipeline with Visualization
## Bangkok Air Quality Dashboard with Streamlit, Prefect & LakeFS

### Project Overview
This project is part of the DSI321: Big Data Infrastructure course, which focuses on building scalable data pipelines using modern tools. Our project implements a near real-time air quality monitoring system for Bangkok, utilizing hourly PM2.5 and AQI data collected from the Air4Thai API. The system forecasts both current and future air pollution levels, making it useful for public health awareness, urban planning, and environmental studies.

We employ a modern data architecture comprising a combination of Prefect (workflow orchestration), LakeFS (data versioning), Streamlit (interactive data visualization), and Docker (for reproducible deployment). Furthermore, we apply ARIMA time-series forecasting to predict air quality levels 6 hours in advance. All components are containerized for ease of deployment and reproducibility.

### Key Features:
- **Real-Time Data Ingestion from LakeFS**  
  Air quality data (PM2.5 and AQI) is stored in a version-controlled Parquet format using LakeFS. This ensures reproducibility and complete tracking of data updates.
* Real-time ingestion of AQI and PM2.5 data from Bangkok monitoring stations  
* Forecasting of **both PM2.5 and AQI values** for each station using ARIMA
* Interactive dashboard to display live readings, forecasts, and pollution trends  
* Geographic visualization with AQI heatmaps across Bangkok districts  
* Fully containerized setup using Docker and Docker Compose  
* Reproducible and version-controlled data pipelines using LakeFS  
* Automated scheduling of ingestion and forecasting flows using Prefect 

### Tools & Technologies
- **Interactive Visualization via Streamlit**  
  The dashboard features:
  - A color-coded interactive map showing AQI levels by district
  - Time-series charts for AQI trends over recent hours
  - A dropdown to select specific districts
This project leverages modern open-source tools:
- **Data Version Control with LakeFS**  
  LakeFS provides Git-like version control for data, enabling rollback, reproducibility, and auditability—crucial for time-sensitive environmental monitoring.
- **Prefect**: Python-based workflow orchestration and scheduling
- **LakeFS**: Git-like version control system for data lakes
- **Streamlit**: Framework for creating interactive dashboards in Python
- **Docker**: Containerization platform to ensure consistent environments
- **JupyterLab**: Notebook interface for data exploration and testing
- **ARIMA**: Statistical time-series forecasting model for AQI and PM2.5
- **Reproducible Deployment with Docker**  
  All components (Streamlit, Prefect, LakeFS) are containerized for easy setup, consistent environments, and fast deployment.

## Tech Stack Summary

| Layer                 | Tools                          |
| --------------------- | ------------------------------ |
| **Orchestration**     | Prefect                        |
| **Containerization**  | Docker                         |
| **Data Versioning**   | LakeFS                         |
| **Visualization**     | Streamlit                      |
| **Forecasting Model** | ARIMA                          |
| **Notebook IDE**      | JupyterLab                     |
| **Data Source**       | [Air4Thai PM2.5 API](http://air4thai.pcd.go.th/services/getNewAQI_JSON.php)|

## Data Schema
The following table describes the structure of the **processed dataset** used for forecasting and visualization in the Streamlit dashboard. This schema is a refined version of the full dataset stored in LakeFS.

| Column       | Data Type | Description |
|--------------|-----------|-------------|
| `timestamp`  | datetime  | Timestamp of the measurement |
| `stationID`  | string    | Unique station identifier |
| `nameTH`     | string    | Station name in Thai |
| `areaTH`     | string    | Area name in Thai |
| `district`   | string    | District name |
| `lat`        | float     | Latitude |
| `long`       | float     | Longitude |
| `AQI.aqi`    | int       | Air Quality Index (0–500) |
| `PM25.value` | float     | PM2.5 concentration (µg/m³) |
| `year`       | int       | Year of the measurement |
| `month`      | int       | Month of the measurement |
| `day`        | int       | Day of the measurement |
| `hour`       | int       | Hour of the measurement (0–23) |

