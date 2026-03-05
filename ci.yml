# Modern Data Engineering Project (Market-Ready): API → DuckDB Lakehouse (Bronze/Silver/Gold) + Data Quality + CI

This repo is an **end-to-end data engineering project** that matches what companies want today:
- **Ingestion from a real API** (Open-Meteo, no API key)
- **Bronze/Silver/Gold layers** in a warehouse (**DuckDB** = lightweight, local "lakehouse")
- **Transformations** via SQL (analytics-friendly models)
- **Data quality checks** with **pytest**
- **GitHub Actions CI** to run tests on every push

## What it builds
- `bronze_weather_raw` (raw hourly API data)
- `silver_weather_hourly` (cleaned, typed, deduped)
- `gold_weather_daily` (daily aggregates + KPIs)

## Quick start
### 1) Create venv + install
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

pip install -r requirements.txt
```

### 2) Run the pipeline (example: Cincinnati, OH)
```bash
python -m src.run_pipeline --lat 39.1031 --lon -84.5120 --days 14 --db data/warehouse.duckdb
```

### 3) Query the warehouse
```bash
python -m src.query --db data/warehouse.duckdb --sql "select * from gold_weather_daily order by day desc limit 10;"
```

### 4) Run quality tests
```bash
pytest -q
```

## Why this is “best project in the market”
Recruiters love projects that show:
- ingestion + orchestration (`run_pipeline`)
- warehouse modeling (Bronze/Silver/Gold)
- SQL analytics (`gold_weather_daily`)
- testing/quality gates (`pytest`)
- CI automation (GitHub Actions)

## Repo structure
```
.
├── src/
│   ├── run_pipeline.py
│   ├── extract_open_meteo.py
│   ├── warehouse.py
│   ├── transforms.sql
│   ├── query.py
│   └── __init__.py
├── tests/
│   └── test_quality.py
├── data/                 # created at runtime (local warehouse)
├── .github/workflows/ci.yml
├── requirements.txt
└── README.md
```

## Notes
- Open-Meteo terms apply; this is for learning/portfolio use.
- You can swap DuckDB for Snowflake/BigQuery later; the pattern stays the same.
