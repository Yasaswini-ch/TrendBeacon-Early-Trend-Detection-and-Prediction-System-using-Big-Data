# TrendBeacon: Early Trend Detection and Prediction System

TrendBeacon is a research-style big data project for detecting emerging topics from high-volume text streams before they become fully viral. It combines multi-source ingestion, Spark-based preprocessing, rolling keyword frequency analysis, growth and anomaly signals, forecasting, and a Streamlit dashboard for exploration.

## Overview

The project is built to answer a simple question:

Can we identify a topic while it is still emerging, instead of only after it has already gone viral?

TrendBeacon does this by:

1. Ingesting text from Twitter/X, Reddit, and news feeds.
2. Normalizing records into a shared schema.
3. Computing keyword frequencies over rolling windows such as `1h`, `6h`, `24h`, and `7d`.
4. Measuring growth rate, z-score spikes, anomaly signals, and other trend indicators.
5. Forecasting future keyword momentum.
6. Displaying current and predicted trends in a Streamlit dashboard.

## What Has Been Used

### Languages and Frameworks

- Python
- PySpark
- Streamlit
- Plotly

### Data and Processing Libraries

- pandas
- NumPy
- SciPy
- scikit-learn
- PyArrow

### NLP and Text Processing

- NLTK
- spaCy

### Forecasting and Modeling

- Prophet
- Isolation Forest style anomaly support

### Data Sources

- Twitter/X API via `tweepy`
- Reddit API via `praw`
- RSS/news feeds via `feedparser`
- Synthetic demo data via `data_ingestion/data_generator.py`

### Storage and Pipeline Style

- Hadoop-style local data lake layout under `data/hdfs/`
- Parquet outputs for processed, feature, trend, and forecast data
- Config-driven pipeline behavior via `config/config.yaml`

## How The Project Works

### 1. Ingestion Layer

Modules in [data_ingestion](c:/Opensource/EarlyTrend/data_ingestion) collect data from:

- Twitter/X
- Reddit
- News RSS feeds
- Synthetic generators when API access is not available

The main orchestrator is [data_ingestion/ingest_runner.py](c:/Opensource/EarlyTrend/data_ingestion/ingest_runner.py).

Raw outputs are written into a Hadoop-style folder layout such as:

```text
data/hdfs/raw/source=twitter/
data/hdfs/raw/source=reddit/
data/hdfs/raw/source=news/
```

### 2. Preprocessing Layer

[data_processing/text_preprocessor.py](c:/Opensource/EarlyTrend/data_processing/text_preprocessor.py) cleans and standardizes the incoming text by:

- normalizing schema
- cleaning text
- tokenizing text
- filtering stopwords and short tokens

The processed output is stored under:

```text
data/hdfs/processed/
```

### 3. Frequency Analysis

[data_processing/frequency_analyzer.py](c:/Opensource/EarlyTrend/data_processing/frequency_analyzer.py) converts processed tokens into rolling keyword statistics across multiple windows.

This is where the project starts measuring:

- how often a keyword appears
- how fast it is growing
- whether short-term and long-term windows tell different stories

### 4. Trend Detection

[data_processing/trend_detector.py](c:/Opensource/EarlyTrend/data_processing/trend_detector.py) labels keywords using the project’s early-warning logic.

Important signals include:

- frequency
- growth rate
- z-score anomaly strength
- velocity
- acceleration
- source diversity

The project distinguishes between:

- `emerging` trends: gradual and sustained growth, usually stronger in longer windows like `24h`
- `viral` trends: sudden spikes, usually stronger in short windows like `1h`
- `stable` and `declining` topics where appropriate

Trend outputs are written to:

```text
data/hdfs/trends/
```

### 5. Feature Engineering

[features/feature_builder.py](c:/Opensource/EarlyTrend/features/feature_builder.py) creates model-ready features from frequency and trend outputs.

These features support:

- dashboard summaries
- anomaly interpretation
- forecasting inputs

Feature outputs are written to:

```text
data/hdfs/features/
```

### 6. Forecasting

[model/prophet_model.py](c:/Opensource/EarlyTrend/model/prophet_model.py) forecasts future trend intensity for top keywords. The forecasting entrypoint is [model/run_forecasting.py](c:/Opensource/EarlyTrend/model/run_forecasting.py).

Forecast outputs are written to:

```text
data/hdfs/trends/predictions/
```

### 7. Dashboard

The Streamlit dashboard lives in [dashboard](c:/Opensource/EarlyTrend/dashboard) and contains:

- [dashboard/app.py](c:/Opensource/EarlyTrend/dashboard/app.py): home page
- [dashboard/pages/current_trends.py](c:/Opensource/EarlyTrend/dashboard/pages/current_trends.py): current trend monitoring
- [dashboard/pages/predicted_trends.py](c:/Opensource/EarlyTrend/dashboard/pages/predicted_trends.py): forecast view

The dashboard shows:

- current trending topics
- trend label distribution
- growth-rate leaderboards
- anomaly highlights
- forecast tables and charts

For deployment-friendly behavior, the dashboard also includes:

- [dashboard/demo_data.py](c:/Opensource/EarlyTrend/dashboard/demo_data.py): bundled sample datasets
- [dashboard/bootstrap_data.py](c:/Opensource/EarlyTrend/dashboard/bootstrap_data.py): startup generation of sample parquet outputs when real outputs are missing

## High-Level Flow

```text
Data Sources
  -> Ingestion
  -> Preprocessing
  -> Frequency Analysis
  -> Trend Detection
  -> Feature Engineering
  -> Forecasting
  -> Streamlit Dashboard
```

## Project Structure

```text
TrendBeacon/
├── config/
│   ├── config.yaml
│   ├── config_loader.py
│   └── logging_config.py
├── dashboard/
│   ├── app.py
│   ├── bootstrap_data.py
│   ├── demo_data.py
│   ├── theme.py
│   ├── components/
│   │   ├── trend_chart.py
│   │   └── trend_table.py
│   └── pages/
│       ├── current_trends.py
│       └── predicted_trends.py
├── data/
│   └── hdfs/
│       ├── raw/
│       ├── processed/
│       ├── features/
│       └── trends/
├── data_ingestion/
│   ├── data_generator.py
│   ├── ingest_runner.py
│   ├── news_ingest.py
│   ├── reddit_ingest.py
│   └── twitter_ingest.py
├── data_processing/
│   ├── frequency_analyzer.py
│   ├── keyword_extractor.py
│   ├── run_pipeline.py
│   ├── text_preprocessor.py
│   └── trend_detector.py
├── features/
│   ├── feature_builder.py
│   └── growth_rate.py
├── model/
│   ├── anomaly_detector.py
│   ├── prophet_model.py
│   └── run_forecasting.py
├── tests/
├── utils/
│   ├── data_validator.py
│   ├── hdfs_utils.py
│   ├── schema_definitions.py
│   └── spark_session.py
├── requirements.txt
├── runtime.txt
└── README.md
```

## Important Modules

- [config/config.yaml](c:/Opensource/EarlyTrend/config/config.yaml): central project configuration
- [data_ingestion/data_generator.py](c:/Opensource/EarlyTrend/data_ingestion/data_generator.py): synthetic demo data generation
- [data_ingestion/ingest_runner.py](c:/Opensource/EarlyTrend/data_ingestion/ingest_runner.py): ingestion orchestrator
- [data_processing/run_pipeline.py](c:/Opensource/EarlyTrend/data_processing/run_pipeline.py): main batch pipeline
- [data_processing/trend_detector.py](c:/Opensource/EarlyTrend/data_processing/trend_detector.py): trend labeling logic
- [features/feature_builder.py](c:/Opensource/EarlyTrend/features/feature_builder.py): feature table creation
- [model/prophet_model.py](c:/Opensource/EarlyTrend/model/prophet_model.py): forecasting logic
- [dashboard/app.py](c:/Opensource/EarlyTrend/dashboard/app.py): Streamlit home page

## Data Layout

The project uses a local Hadoop-style storage layout:

```text
data/hdfs/raw/
data/hdfs/processed/
data/hdfs/features/
data/hdfs/trends/
data/hdfs/trends/predictions/
data/hdfs/models/
```

This makes the project feel closer to a real distributed data pipeline, even when running locally.

## Configuration

The main configuration file is [config/config.yaml](c:/Opensource/EarlyTrend/config/config.yaml).

Important sections:

- `spark`
- `hdfs`
- `ingestion`
- `processing`
- `features`
- `models`
- `dashboard`
- `logging`

## Setup

### 1. Create a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Git Bash:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

## Running The Project

### Demo run with synthetic data

1. Run ingestion:

```powershell
python -m data_ingestion.ingest_runner
```

2. Run the processing pipeline:

```powershell
python -m data_processing.run_pipeline
```

3. Run forecasting:

```powershell
python -m model.run_forecasting
```

4. Launch the dashboard:

```powershell
streamlit run dashboard/app.py
```

## Dashboard Pages

- `Home`: project overview and data status
- `Current Trends`: active trend board with filters, charts, and anomaly alerts
- `Predicted Trends`: forecast table, bubble view, and keyword forecast track

## Optional API Configuration

You do not need API keys for demo mode.

If you want live ingestion, add values in `.env` for:

- `TWITTER_BEARER_TOKEN`
- `REDDIT_CLIENT_ID`
- `REDDIT_CLIENT_SECRET`

The project loads environment variables through [config/config_loader.py](c:/Opensource/EarlyTrend/config/config_loader.py).

## Deployment Notes

For Streamlit deployment:

- [runtime.txt](c:/Opensource/EarlyTrend/runtime.txt) pins Python `3.11`
- the dashboard can generate sample parquet outputs on startup
- if real pipeline outputs are absent, the deployed app still remains usable

## Research Value

This project is suitable for:

- academic mini projects
- capstone projects
- research demos
- portfolio projects focused on big data engineering and trend intelligence

It demonstrates:

- multi-source ingestion
- Spark-based text processing
- rolling-window analytics
- early trend detection
- anomaly-aware labeling
- time-series forecasting
- dashboard-based monitoring

## Suggested Resume Line

Built a big data early trend detection system using Python, PySpark, rolling-window keyword analytics, anomaly signals, forecasting, and Streamlit dashboards to identify emerging and viral topics from multi-source social and news data.

## Future Improvements

- stronger forecast evaluation metrics
- sentiment enrichment
- better fake-vs-organic amplification detection
- true cloud/object storage integration
- Kafka-driven streaming mode
- additional forecasting model comparison

## License

Add your preferred open-source or academic-use license here.
