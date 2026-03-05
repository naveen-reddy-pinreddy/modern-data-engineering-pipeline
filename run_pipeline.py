-- Bronze → Silver → Gold
-- Rebuild as views so it's fast to iterate in a portfolio repo.
-- In real production, you'd often materialize (tables/incremental) and orchestrate with Airflow/dbt.

-- SILVER: typed, deduped hourly data
CREATE OR REPLACE VIEW silver_weather_hourly AS
WITH base AS (
    SELECT
        -- Parse ISO timestamp text into TIMESTAMP
        TRY_STRPTIME(time, '%Y-%m-%dT%H:%M') AS ts,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        wind_speed_10m,
        latitude,
        longitude,
        timezone,
        extracted_at_utc
    FROM bronze_weather_raw
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ts, latitude, longitude, timezone
            ORDER BY extracted_at_utc DESC
        ) AS rn
    FROM base
    WHERE ts IS NOT NULL
)
SELECT
    ts,
    DATE(ts) AS day,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    wind_speed_10m,
    latitude,
    longitude,
    timezone,
    extracted_at_utc
FROM dedup
WHERE rn = 1;

-- GOLD: daily aggregates (analytics-ready KPI table)
CREATE OR REPLACE VIEW gold_weather_daily AS
SELECT
    day,
    latitude,
    longitude,
    timezone,
    COUNT(*) AS hourly_points,
    AVG(temperature_2m) AS avg_temp_c,
    MIN(temperature_2m) AS min_temp_c,
    MAX(temperature_2m) AS max_temp_c,
    AVG(relative_humidity_2m) AS avg_humidity,
    SUM(precipitation) AS total_precip_mm,
    AVG(wind_speed_10m) AS avg_wind_speed
FROM silver_weather_hourly
GROUP BY 1,2,3,4;
