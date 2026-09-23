SELECT
    key                                     AS vehicle_type,
    vehicle_types.ping_interval_seconds     AS ping_interval_seconds,
    vehicle_types.has_driver                AS has_driver

FROM {{source ('bronze', 'vehicle_types')}}
