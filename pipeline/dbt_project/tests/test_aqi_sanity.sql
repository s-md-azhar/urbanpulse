-- Custom Data Quality Test: test_aqi_sanity
-- Returns rows where AQI is outside physically realistic atmospheric bounds [0, 500]
-- or where max_us_aqi is less than avg_us_aqi.

SELECT
    daily_metric_id,
    city,
    metric_date,
    avg_us_aqi,
    max_us_aqi
FROM {{ ref('fct_daily_city_metrics') }}
WHERE
    avg_us_aqi < 0.0
    OR avg_us_aqi > 500.0
    OR max_us_aqi < 0.0
    OR max_us_aqi > 500.0
    OR max_us_aqi < avg_us_aqi
