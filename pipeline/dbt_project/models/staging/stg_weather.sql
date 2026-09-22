{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        city,
        CAST(date AS DATE) AS metric_date,
        CAST(hour AS INTEGER) AS metric_hour,
        CAST(datetime_utc AS TIMESTAMP) AS measured_at,
        CAST(temperature_2m AS DOUBLE) AS temperature_c,
        CAST(relative_humidity_2m AS DOUBLE) AS relative_humidity_pct,
        CAST(precipitation AS DOUBLE) AS precipitation_mm,
        CAST(wind_speed_10m AS DOUBLE) AS wind_speed_kmh,
        CAST(weather_code AS INTEGER) AS weather_code,
        source_file,
        silver_processed_at
    FROM delta_scan('{{ env_var("DELTA_DATA_PATH", "../../data/delta") }}/silver_weather')
)

SELECT * FROM source_data
