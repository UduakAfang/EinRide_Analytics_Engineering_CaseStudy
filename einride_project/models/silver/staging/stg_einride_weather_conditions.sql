SELECT
    key                                         AS weather_condition,
    weather_conditions.consumption_factor       AS consumption_factor,
    weather_conditions.regen_efficiency         AS regen_efficiency,
    weather_conditions.rolling_resistance       AS rolling_resistance

FROM {{source ('bronze', 'weather_mapping')}}
