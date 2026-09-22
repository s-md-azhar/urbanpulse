{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        city,
        CAST(date AS DATE) AS metric_date,
        CAST(hour AS INTEGER) AS metric_hour,
        CAST(datetime_utc AS TIMESTAMP) AS measured_at,
        CAST(pm10 AS DOUBLE) AS pm10_ugm3,
        CAST(pm2_5 AS DOUBLE) AS pm2_5_ugm3,
        CAST(carbon_monoxide AS DOUBLE) AS carbon_monoxide_ugm3,
        CAST(nitrogen_dioxide AS DOUBLE) AS nitrogen_dioxide_ugm3,
        CAST(sulphur_dioxide AS DOUBLE) AS sulphur_dioxide_ugm3,
        CAST(ozone AS DOUBLE) AS ozone_ugm3,
        CAST(european_aqi AS DOUBLE) AS european_aqi,
        CAST(us_aqi AS DOUBLE) AS us_aqi,
        source_file,
        silver_processed_at
    FROM delta_scan('{{ env_var("DELTA_DATA_PATH", "../../data/delta") }}/silver_air_quality')
)

SELECT * FROM source_data
