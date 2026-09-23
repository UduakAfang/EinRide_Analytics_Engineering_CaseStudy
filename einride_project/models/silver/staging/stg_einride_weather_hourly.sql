SELECT
    region,
    (CAST(observed_at AS TIMESTAMP))    AS observed_at,
    hour_of_day,
    weather_condition,
    air_temp_c,
    wind_speed_ms,
    precipitation_mm,
    road_condition,
    consumption_factor,
    ingested_by                         AS source

FROM {{source ('bronze', 'weather_hourly')}}
