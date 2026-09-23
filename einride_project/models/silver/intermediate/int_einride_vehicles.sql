-- UNION ALL because a truck is never a pod. If a vehicle_id turns up twice
-- the source files disagree, and I want the test to say so.

SELECT
    vehicle_id,
    home_depot_id,
    vehicle_type,
    oem,
    battery_chemistry,
    battery_capacity_kwh,
    initial_state_of_health_pct,
    payload_capacity_kg,
    vin,
    vehicle_model,
    CAST(NULL AS DOUBLE)    AS max_range_km,
    CAST(NULL AS STRING)    AS autonomy_level,
    purchase_date,
    CAST(NULL AS DATE)      AS deployment_date,
    purchase_date           AS in_service_date

FROM {{ ref('stg_einride_trucks') }}

UNION ALL

SELECT
    vehicle_id,
    home_depot_id,
    vehicle_type,
    oem,
    battery_chemistry,
    battery_capacity_kwh,
    initial_state_of_health_pct,
    payload_capacity_kg,
    CAST(NULL AS STRING)    AS vin,
    CAST(NULL AS STRING)    AS vehicle_model,
    max_range_km,
    autonomy_level,
    CAST(NULL AS DATE)      AS purchase_date,
    deployment_date,
    deployment_date         AS in_service_date

FROM {{ ref('stg_einride_autonomous_pods') }}
