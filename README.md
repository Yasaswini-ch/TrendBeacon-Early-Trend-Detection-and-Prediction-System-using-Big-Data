# TrendBeacon: Early Trend Detection and Prediction System using Big Data

TrendBeacon is a production-style academic and research project for detecting emerging topics from large-scale text streams before they become fully viral. The system combines ingestion, preprocessing, keyword frequency analysis, growth-rate signals, anomaly detection, forecasting, and dashboard visualization.

## Project Goal

The system is designed to:

1. Collect large-scale data from multiple sources such as Twitter, Reddit, and news feeds.
2. Store raw and processed data in a Hadoop-style data lake layout.
3. Process text using Apache Spark and Python.
4. Detect early trend signals using growth rate and anomaly detection.
5. Predict future trending topics using time-series forecasting.
6. Visualize current and predicted trends in a Streamlit dashboard.

## Core Idea

The project distinguishes between:

- Emerging trends: topics showing sustained growth over a longer horizon such as 24 hours.
- Viral trends: topics showing sudden short-term spikes, often in the 1-hour window.

Early detection is based on multiple signals:

- Frequency changes across rolling windows
- Growth rate
- Z-score based anomaly intensity
- Velocity
- Acceleration
- Source diversity

## High-Level Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                             │
│   Twitter API / Reddit API / News RSS / Kaggle Datasets (simulate)  │
└─────────────────┬───────────────────────────────────────────────────┘
                  │ raw JSON/CSV streams
                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       DATA INGESTION LAYER                          │
│  /data_ingestion                                                    │
│  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────┐  │
│  │ twitter_ingest.py │  │ reddit_ingest.py  │  │ news_ingest.py  │  │
│  └─────────┬─────────┘  └─────────┬─────────┘  └────────┬────────┘  │
│            └──────────────────────┬───────────────────────────────┘ │
│                                   ▼                                 │
│                     ┌─────────────────────────┐                     │
│                     │ Kafka (optional)        │                     │
│                     │ or batch file writer    │                     │
│                     └────────────┬────────────┘                     │
└──────────────────────────────────┼──────────────────────────────────┘
                                   │ raw data
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER (HDFS)                        │
│  /hdfs_storage                                                      │
│                                                                     │
│  hdfs://data/raw/        ← raw ingested data (partitioned by date)  │
│  hdfs://data/processed/  ← cleaned, normalized data                 │
│  hdfs://data/features/   ← engineered features                      │
│  hdfs://data/trends/     ← detected trends output                   │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PROCESSING LAYER (PySpark)                       │
│  /data_processing                                                   │
│                                                                     │
│  ┌──────────────────────┐   ┌──────────────────────┐                │
│  │ text_preprocessor.py │   │ keyword_extractor.py │                │
│  └──────────────────────┘   └──────────────────────┘                │
│  ┌──────────────────────┐   ┌──────────────────────┐                │ 
│  │ frequency_analyzer.py│   │ trend_detector.py    │                │
│  │                      │   │ growth + anomalies   │                │ 
│  └──────────────────────┘   └──────────────────────┘                │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   FEATURE ENGINEERING LAYER                         │
│  /features                                                          │
│                                                                     │
│  • Keyword frequency over time windows (1h, 6h, 24h)                │
│  • Growth Rate = (freq_now - freq_past) / freq_past                 │
│  • Z-score anomaly detection on frequency spikes                    │
│  • Velocity and acceleration of keyword mention counts              │
│  • Sentiment score per keyword cluster                              │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PREDICTION LAYER                             │
│  /model                                                             │
│                                                                     │
│  ┌──────────────────────┐   ┌──────────────────────┐                │
│  │ prophet_model.py     │   │ anomaly_detector.py  │                │
│  │ time-series forecast │   │ IsolationForest/Z    │                │
│  └──────────────────────┘   └──────────────────────┘                │
│  ┌──────────────────────┐                                           │ 
│  │ trend_classifier.py  │  emerging vs viral classifier             │
│  └──────────────────────┘                                           │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      VISUALIZATION LAYER                            │
│  /dashboard                                                         │
│                                                                     │
│  Streamlit App:                                                     │
│  • Live trending topics table                                       │
│  • Predicted trends for next 24h / 7d                               │
│  • Keyword frequency time-series charts                             │
│  • Anomaly spike markers on charts                                  │
│  • Emerging vs Viral trend badges                                   │
└─────────────────────────────────────────────────────────────────────┘
```

## Folder Structure

```text
config/
dashboard/
dashboard/pages/
data/
data_ingestion/
data_processing/
features/
model/
tests/
utils/
requirements.txt
README.md
```

## Important Modules

- `data_ingestion/ingest_runner.py`
  Runs all data sources.

- `data_ingestion/data_generator.py`
  Generates synthetic datasets for demo mode.

- `data_processing/text_preprocessor.py`
  Normalizes raw source records into a common schema.

- `data_processing/frequency_analyzer.py`
  Builds rolling keyword frequency windows such as `1h`, `6h`, `24h`, and `7d`.

- `data_processing/trend_detector.py`
  Computes growth rate, z-score, anomaly flags, velocity, acceleration, and trend labels.

- `features/feature_builder.py`
  Produces a wide feature table for forecasting and downstream analytics.

- `model/prophet_model.py`
  Forecasts future trend intensity. Uses Prophet when available and includes fallback behavior.

- `dashboard/app.py`
  Streamlit home page.

- `dashboard/pages/current_trends.py`
  Current trend visualization page.

- `dashboard/pages/predicted_trends.py`
  Predicted trend visualization page.

## Data Storage Layout

The project uses a Hadoop-style directory layout locally:

```text
data/hdfs/raw/
data/hdfs/processed/
data/hdfs/features/
data/hdfs/trends/
data/hdfs/trends/predictions/
```

Even in local demo mode, the layout mirrors what a bigger HDFS deployment would look like.

## Setup

### 1. Create and activate a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

## Running the Project

### Demo mode without APIs

The simplest way to run the project is with simulated data.

### 1. Ingest synthetic data

```powershell
python -m data_ingestion.ingest_runner
```

### 2. Run processing

```powershell
python -m data_processing.run_pipeline
```

### 3. Run forecasting

```powershell
python -m model.run_forecasting
```

### 4. Launch Streamlit dashboard

```powershell
streamlit run dashboard/app.py
```

## Dashboard Pages

The Streamlit dashboard uses a custom top navbar:

- `Home`: dashboard landing page
- `Current Trends`: live trend intelligence page
- `Predicted Trends`: forecast and confidence page

## APIs

You do not need APIs for demo mode.

### Optional APIs for live data

- Twitter/X
  Set `TWITTER_BEARER_TOKEN`

- Reddit
  Set `REDDIT_CLIENT_ID`
  Set `REDDIT_CLIENT_SECRET`

- News
  RSS feeds can be used without an API key

The current codebase is already configured to work with synthetic data when:

```yaml
ingestion:
  simulate: true
```

## Configuration

Main configuration file:

- `config/config.yaml`

Key sections:

- `spark`
- `hdfs`
- `ingestion`
- `processing`
- `features`
- `models`
- `dashboard`

## Research Contributions

This project is suitable for academic, capstone, or research presentation use because it includes:

- Big data style pipeline design
- Trend detection across rolling time windows
- Growth-rate based early signal detection
- Z-score based anomaly detection
- Emerging vs viral trend classification
- Forecasting of future trend likelihood
- Dashboard-based explanation and monitoring

## Emerging vs Viral Trends

### Emerging

- Gradual but statistically meaningful increase
- Usually more visible in 24-hour windows
- Strong candidate for early intervention or early insight

### Viral

- Sharp short-term spike
- Usually more visible in 1-hour windows
- Often has higher anomaly intensity

## Fake vs Organic Trend Detection

The current codebase includes the foundation for this idea through:

- source diversity
- anomaly intensity
- cross-source spread

Possible future extension:

- low source diversity + very high short-window spike = suspicious amplification
- high source diversity + steady multi-window growth = organic emerging trend

## Current Practical Notes

### Local Windows mode

This project has been adapted to run in local Windows environments, but full Spark + Hadoop filesystem behavior on Windows can still be finicky because of `winutils` and `HADOOP_HOME` behavior.

To keep the dashboard demonstrable:

- synthetic raw data is supported
- local parquet generation is supported
- the dashboard can be populated even when full Hadoop behavior is not available

### Streamlit behavior

If Streamlit is already running and you change code:

1. save files
2. refresh the browser
3. restart Streamlit if needed

## Suggested Resume Description

Built a Big Data early trend detection and prediction system using Python, PySpark, time-window frequency analysis, anomaly detection, forecasting, and Streamlit dashboarding to identify emerging and viral topics from multi-source social and news data.

## Suggested Research Abstract Angle

This work proposes a scalable early trend detection architecture that combines rolling-window keyword analysis, growth-rate estimation, anomaly detection, and forecasting to identify emerging social and news topics before mainstream viral escalation.

## Next Improvements

- full end-to-end Spark pipeline stabilization on Windows
- stronger forecasting evaluation metrics
- sentiment analysis enrichment
- fake vs organic trend classifier
- Kafka streaming mode
- Hive or MongoDB query layer
- model comparison between Prophet, ARIMA, and ML classifiers

## License

Add your preferred academic or open-source license here.
