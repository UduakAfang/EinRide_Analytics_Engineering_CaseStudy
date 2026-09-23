SELECT
    key                                     AS vehicle_type,
    EXPLODE(vehicle_types.data_sources)     AS data_source

FROM {{source ('bronze', 'vehicle_types')}}
