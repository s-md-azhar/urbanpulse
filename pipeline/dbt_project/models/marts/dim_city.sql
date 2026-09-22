{{ config(materialized='table') }}

WITH city_data AS (
    SELECT 'delhi' AS city, 'Delhi' AS city_name, 'National Capital Territory' AS state, 28.6139 AS latitude, 77.2090 AS longitude, 'Tier-1 Megacity' AS population_tier, 'Asia/Kolkata' AS timezone
    UNION ALL
    SELECT 'mumbai', 'Mumbai', 'Maharashtra', 19.0760, 72.8777, 'Tier-1 Megacity', 'Asia/Kolkata'
    UNION ALL
    SELECT 'bengaluru', 'Bengaluru', 'Karnataka', 12.9716, 77.5946, 'Tier-1 Tech Hub', 'Asia/Kolkata'
    UNION ALL
    SELECT 'kolkata', 'Kolkata', 'West Bengal', 22.5726, 88.3639, 'Tier-1 Metro', 'Asia/Kolkata'
    UNION ALL
    SELECT 'chennai', 'Chennai', 'Tamil Nadu', 13.0827, 80.2707, 'Tier-1 Coastal Hub', 'Asia/Kolkata'
    UNION ALL
    SELECT 'hyderabad', 'Hyderabad', 'Telangana', 17.3850, 78.4867, 'Tier-1 Tech Hub', 'Asia/Kolkata'
    UNION ALL
    SELECT 'pune', 'Pune', 'Maharashtra', 18.5204, 73.8567, 'Tier-2 Emerging Metro', 'Asia/Kolkata'
    UNION ALL
    SELECT 'ahmedabad', 'Ahmedabad', 'Gujarat', 23.0225, 72.5714, 'Tier-2 Industrial Hub', 'Asia/Kolkata'
)

SELECT
    md5(city) AS city_id,
    city,
    city_name,
    state,
    latitude,
    longitude,
    population_tier,
    timezone,
    CURRENT_TIMESTAMP AS created_at
FROM city_data
