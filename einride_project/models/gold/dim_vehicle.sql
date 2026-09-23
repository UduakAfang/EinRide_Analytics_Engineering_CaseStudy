-- One row per vehicle. Trucks and pods together, because a fleet manager asks
-- "how many vehicles" and does not want two answers.
--
-- Built from the intermediate model rather than the OBT. The OBT only knows
-- vehicles that reported in the window -- a truck in the workshop all morning
-- would simply not exist. A fleet list has to list the fleet.

SELECT
    v.vehicle_id,
    v.vehicle_type,
    v.oem,
    v.vehicle_model,
    v.vin,
    v.autonomy_level,

    v.battery_chemistry,
    v.battery_capacity_kwh,
    v.initial_state_of_health_pct,
    bs.degradation_rate_per_cycle,
    bs.cycle_life,
    bs.max_charge_rate_c,

    v.payload_capacity_kg,
    v.max_range_km,

    v.home_depot_id,
    d.depot_name                                    AS home_depot_name,
    d.region                                        AS home_depot_region,

    vt.has_driver,
    vt.ping_interval_seconds,

    v.purchase_date,
    v.deployment_date,
    v.in_service_date,
    DATEDIFF(CURRENT_DATE(), v.in_service_date) / 365.25   AS age_years

FROM {{ ref('int_einride_vehicles') }} v

LEFT JOIN {{ ref('stg_einride_depots') }} d
       ON v.home_depot_id = d.depot_id

LEFT JOIN {{ ref('stg_einride_vehicle_types') }} vt
       ON v.vehicle_type = vt.vehicle_type

LEFT JOIN {{ ref('stg_einride_battery_specs') }} bs
       ON v.battery_chemistry = bs.battery_chemistry
