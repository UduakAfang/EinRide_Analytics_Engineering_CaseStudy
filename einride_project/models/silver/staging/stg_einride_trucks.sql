SELECT
    truck_id                        AS vehicle_id,
    vin,
    home_depot_id,
    vehicle_type,
    oem,
    model                           AS vehicle_model,
    battery_chemistry,
    battery_capacity_kwh,
    initial_state_of_health_pct,
    payload_capacity_kg,
    (CAST(purchase_date AS DATE))   AS purchase_date

FROM {{source ('bronze', 'trucks')}}
