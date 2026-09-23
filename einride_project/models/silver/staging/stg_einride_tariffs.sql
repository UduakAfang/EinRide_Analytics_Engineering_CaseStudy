SELECT
    price_zone,
    hour_of_day,
    day_type,
    price_sek_per_kwh

FROM {{source ('bronze', 'tariffs')}}
