"""
Warehouse layer: DuckDB for Bronze/Silver/Gold.
- Bronze: raw append-only table
- Silver/Gold: rebuilt views or tables via SQL transforms
"""
from __future__ import annotations

import os
import duckdb
import pandas as pd
from typing import Optional


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    con = duckdb.connect(db_path)
    # Good defaults
    con.execute("PRAGMA threads=4;")
    return con


def ensure_bronze_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze_weather_raw (
            time TEXT,
            temperature_2m DOUBLE,
            relative_humidity_2m DOUBLE,
            precipitation DOUBLE,
            wind_speed_10m DOUBLE,
            latitude DOUBLE,
            longitude DOUBLE,
            timezone TEXT,
            extracted_at_utc TEXT
        );
        """
    )


def load_to_bronze(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """
    Load a pandas DataFrame into bronze table.
    Returns number of rows inserted.
    """
    ensure_bronze_table(con)

    # Register df as a DuckDB view, then insert-select.
    con.register("incoming_df", df)
    con.execute(
        """
        INSERT INTO bronze_weather_raw
        SELECT
            time,
            CAST(temperature_2m AS DOUBLE),
            CAST(relative_humidity_2m AS DOUBLE),
            CAST(precipitation AS DOUBLE),
            CAST(wind_speed_10m AS DOUBLE),
            CAST(latitude AS DOUBLE),
            CAST(longitude AS DOUBLE),
            CAST(timezone AS TEXT),
            CAST(extracted_at_utc AS TEXT)
        FROM incoming_df;
        """
    )
    return con.execute("SELECT COUNT(*) FROM incoming_df").fetchone()[0]


def apply_transforms(con: duckdb.DuckDBPyConnection, transforms_sql: str) -> None:
    """
    Apply transformation SQL (creates/replaces silver and gold models).
    """
    con.execute(transforms_sql)
