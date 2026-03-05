"""
Extraction layer: pulls hourly weather data from Open-Meteo (no API key).
Docs: https://open-meteo.com/

We keep extraction simple, deterministic, and parameterized.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Dict, Any, List, Optional

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta


@dataclass(frozen=True)
class ExtractParams:
    latitude: float
    longitude: float
    days: int = 14
    timezone: str = "UTC"


HOURLY_FIELDS: List[str] = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
]


def _date_range(days: int) -> tuple[str, str]:
    """
    Return [start_date, end_date] inclusive in ISO date.
    We fetch days ending at *today*.
    """
    today = dt.date.today()
    start = today - dt.timedelta(days=days)
    return start.isoformat(), today.isoformat()


def fetch_open_meteo_hourly(params: ExtractParams) -> pd.DataFrame:
    """
    Fetch hourly data and return as a normalized DataFrame.
    Columns: time (string), each hourly field
    """
    start_date, end_date = _date_range(params.days)

    url = "https://api.open-meteo.com/v1/forecast"
    query = {
        "latitude": params.latitude,
        "longitude": params.longitude,
        "hourly": ",".join(HOURLY_FIELDS),
        "start_date": start_date,
        "end_date": end_date,
        "timezone": params.timezone,
    }

    resp = requests.get(url, params=query, timeout=30)
    resp.raise_for_status()
    payload: Dict[str, Any] = resp.json()

    hourly = payload.get("hourly")
    if not hourly or "time" not in hourly:
        raise ValueError(f"Unexpected API response: missing hourly/time keys. Keys={list(payload.keys())}")

    df = pd.DataFrame(hourly)

    # Ensure consistent column order
    cols = ["time"] + [c for c in HOURLY_FIELDS if c in df.columns]
    df = df[cols].copy()

    # Add lineage / metadata
    df["latitude"] = params.latitude
    df["longitude"] = params.longitude
    df["timezone"] = params.timezone
    df["extracted_at_utc"] = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    return df
