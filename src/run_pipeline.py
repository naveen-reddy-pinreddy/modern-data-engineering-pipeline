"""
Orchestrator: one command runs the whole pipeline.
This is what recruiters want to see: extraction + load + transform + basic observability.
"""
from __future__ import annotations

import argparse
import pathlib
import time

from .extract_open_meteo import ExtractParams, fetch_open_meteo_hourly
from .warehouse import connect, load_to_bronze, apply_transforms


def read_transforms() -> str:
    path = pathlib.Path(__file__).with_name("transforms.sql")
    return path.read_text(encoding="utf-8")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--lat", type=float, required=True, help="Latitude, e.g., 39.1031")
    p.add_argument("--lon", type=float, required=True, help="Longitude, e.g., -84.5120")
    p.add_argument("--days", type=int, default=14, help="Days of history to fetch")
    p.add_argument("--timezone", type=str, default="UTC", help="Timezone, e.g., UTC or America/New_York")
    p.add_argument("--db", type=str, default="data/warehouse.duckdb", help="DuckDB file path")
    args = p.parse_args()

    start = time.time()

    params = ExtractParams(latitude=args.lat, longitude=args.lon, days=args.days, timezone=args.timezone)
    df = fetch_open_meteo_hourly(params)

    con = connect(args.db)
    try:
        inserted = load_to_bronze(con, df)
        apply_transforms(con, read_transforms())
        # Simple metrics
        daily_rows = con.execute("SELECT COUNT(*) FROM gold_weather_daily").fetchone()[0]
        latest_day = con.execute("SELECT MAX(day) FROM gold_weather_daily").fetchone()[0]
    finally:
        con.close()

    elapsed = round(time.time() - start, 2)
    print(f"✅ Pipeline complete in {elapsed}s")
    print(f"   Inserted into bronze: {inserted} rows")
    print(f"   Gold daily rows:      {daily_rows}")
    print(f"   Latest day in gold:   {latest_day}")


if __name__ == "__main__":
    main()
