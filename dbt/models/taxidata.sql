{{ config(materialized='table') }}

WITH all_trips AS (
    SELECT
        *,
        'yellow_taxi' AS vehicle_type
    FROM yellow_tripdata

    UNION ALL

    SELECT
        *,
        'green_taxi' AS vehicle_type
    FROM green_tripdata
)

SELECT
    t.*,
    (t.trip_distance * v.co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,
    CASE 
        WHEN DATE_DIFF('second', t.pickup_datetime, t.dropoff_datetime) > 0
        THEN t.trip_distance / (DATE_DIFF('second', t.pickup_datetime, t.dropoff_datetime) / 3600.0)
        ELSE NULL
    END AS avg_mph,
    EXTRACT(HOUR FROM t.pickup_datetime) AS hour_of_day,
    EXTRACT(DOW FROM t.pickup_datetime) AS day_of_week,
    EXTRACT(WEEK FROM t.pickup_datetime) AS week_of_year,
    EXTRACT(MONTH FROM t.pickup_datetime) AS month_of_year
FROM all_trips t
LEFT JOIN co2data v
  ON t.vehicle_type = v.vehicle_type