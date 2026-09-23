SELECT
    depot_id,
    depot_name,
    latitude,
    longitude,
    region,
    charger_count,
    max_charger_power_kw

FROM {{source ('bronze', 'depots')}}
