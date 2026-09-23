SELECT
    pod_id                          AS vehicle_id,
    home_depot_id,
    vehicle_type,
    oem,
    sensor_suite,
    battery_chemistry,
    battery_capacity_kwh,
    initial_state_of_health_pct,
    payload_capacity_kg,
    max_range_km,
    autonomy_level,
    (CAST(deployment_date AS DATE)) AS deployment_date

FROM {{source ('bronze', 'autonomous_pods')}}
