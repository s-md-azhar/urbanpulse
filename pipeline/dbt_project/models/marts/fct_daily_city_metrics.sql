{{ config(materialized='table') }}

WITH daily_weather AS (
    SELECT
        city,
        metric_date,
        ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
        ROUND(MIN(temperature_c), 2) AS min_temperature_c,
        ROUND(MAX(temperature_c), 2) AS max_temperature_c,
        ROUND(AVG(relative_humidity_pct), 2) AS avg_humidity_pct,
        ROUND(SUM(precipitation_mm), 2) AS total_precipitation_mm,
        ROUND(AVG(wind_speed_kmh), 2) AS avg_wind_speed_kmh,
        ROUND(MAX(wind_speed_kmh), 2) AS max_wind_speed_kmh,
        COUNT(*) AS hourly_weather_records
    FROM {{ ref('stg_weather') }}
    GROUP BY city, metric_date
),

daily_aq AS (
    SELECT
        city,
        metric_date,
        ROUND(AVG(pm2_5_ugm3), 2) AS avg_pm2_5,
        ROUND(MAX(pm2_5_ugm3), 2) AS max_pm2_5,
        ROUND(AVG(pm10_ugm3), 2) AS avg_pm10,
        ROUND(MAX(pm10_ugm3), 2) AS max_pm10,
        ROUND(AVG(carbon_monoxide_ugm3), 2) AS avg_carbon_monoxide,
        ROUND(AVG(nitrogen_dioxide_ugm3), 2) AS avg_nitrogen_dioxide,
        ROUND(AVG(sulphur_dioxide_ugm3), 2) AS avg_sulphur_dioxide,
        ROUND(AVG(ozone_ugm3), 2) AS avg_ozone,
        ROUND(AVG(us_aqi), 1) AS avg_us_aqi,
        ROUND(MAX(us_aqi), 1) AS max_us_aqi,
        COUNT(*) AS hourly_aq_records
    FROM {{ ref('stg_air_quality') }}
    GROUP BY city, metric_date
),

joined_metrics AS (
    SELECT
        md5(w.city || '_' || CAST(w.metric_date AS VARCHAR)) AS daily_metric_id,
        w.city,
        w.metric_date,
        w.avg_temperature_c,
        w.min_temperature_c,
        w.max_temperature_c,
        w.avg_humidity_pct,
        w.total_precipitation_mm,
        w.avg_wind_speed_kmh,
        w.max_wind_speed_kmh,
        aq.avg_pm2_5,
        aq.max_pm2_5,
        aq.avg_pm10,
        aq.max_pm10,
        aq.avg_carbon_monoxide,
        aq.avg_nitrogen_dioxide,
        aq.avg_sulphur_dioxide,
        aq.avg_ozone,
        aq.avg_us_aqi,
        aq.max_us_aqi,
        CASE
            WHEN aq.avg_us_aqi <= 50 THEN 'Good'
            WHEN aq.avg_us_aqi <= 100 THEN 'Moderate'
            WHEN aq.avg_us_aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN aq.avg_us_aqi <= 200 THEN 'Unhealthy'
            WHEN aq.avg_us_aqi <= 300 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END AS aqi_category,
        w.hourly_weather_records,
        aq.hourly_aq_records
    FROM daily_weather w
    INNER JOIN daily_aq aq
        ON w.city = aq.city
        AND w.metric_date = aq.metric_date
),

with_lags AS (
    SELECT
        *,
        LAG(avg_us_aqi, 1) OVER (PARTITION BY city ORDER BY metric_date) AS lag_1d_aqi,
        LAG(avg_us_aqi, 2) OVER (PARTITION BY city ORDER BY metric_date) AS lag_2d_aqi,
        LAG(avg_us_aqi, 3) OVER (PARTITION BY city ORDER BY metric_date) AS lag_3d_aqi,
        ROUND(AVG(avg_us_aqi) OVER (PARTITION BY city ORDER BY metric_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS rolling_7d_avg_aqi
    FROM joined_metrics
)

SELECT * FROM with_lags
