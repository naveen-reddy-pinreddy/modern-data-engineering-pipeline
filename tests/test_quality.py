"""
Quality gates recruiters love:
- no null timestamps in silver
- gold has reasonable row counts
- key numeric fields are not all null
"""
from __future__ import annotations

import os
import subprocess
import duckdb


DB_PATH = "data/warehouse.duckdb"


def _ensure_pipeline_ran():
    if os.path.exists(DB_PATH):
        return
    # Run pipeline for Cincinnati quickly (small days)
    subprocess.check_call(
        ["python", "-m", "src.run_pipeline", "--lat", "39.1031", "--lon", "-84.5120", "--days", "7", "--db", DB_PATH]
    )


def test_silver_has_no_null_ts():
    _ensure_pipeline_ran()
    con = duckdb.connect(DB_PATH)
    try:
        nulls = con.execute("SELECT COUNT(*) FROM silver_weather_hourly WHERE ts IS NULL").fetchone()[0]
        assert nulls == 0
    finally:
        con.close()


def test_gold_has_rows():
    _ensure_pipeline_ran()
    con = duckdb.connect(DB_PATH)
    try:
        n = con.execute("SELECT COUNT(*) FROM gold_weather_daily").fetchone()[0]
        assert n > 0
    finally:
        con.close()


def test_gold_metrics_not_null():
    _ensure_pipeline_ran()
    con = duckdb.connect(DB_PATH)
    try:
        row = con.execute(
            """
            SELECT
                SUM(CASE WHEN avg_temp_c IS NULL THEN 1 ELSE 0 END) AS null_avg_temp,
                COUNT(*) AS total_rows
            FROM gold_weather_daily;
            """
        ).fetchone()
        null_avg_temp, total_rows = row
        assert total_rows > 0
        assert null_avg_temp < total_rows  # not all null
    finally:
        con.close()
