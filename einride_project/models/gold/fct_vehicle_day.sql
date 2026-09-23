-- One row per vehicle per day, for two years.
--
-- Straight from daily_rollup, not from the OBT. The OBT covers six hours of
-- pings; this covers 24 months of end-of-day readings. They answer different
-- questions and forcing one through the other would throw away nearly all of
-- this one.
--
-- Battery degradation lives here. It is a slow curve and you cannot see it in
-- a morning's telemetry.

SELECT
    {{ dbt_utils.generate_surrogate_key(['r.vehicle_id', 'r.rollup_date']) }}
                                                    AS vehicle_day_id,
    r.vehicle_id,
    r.rollup_date,

    v.vehicle_type,
    v.oem,
    v.battery_chemistry,
    v.battery_capacity_kwh,
    v.initial_state_of_health_pct,
    v.home_depot_id,
    v.home_depot_region,

    r.end_of_day_soc_pct,
    r.state_of_health_pct,
    v.initial_state_of_health_pct - r.state_of_health_pct
                                                    AS soh_lost_pct,

    r.cumulative_energy_kwh,
    r.cumulative_distance_km,

    -- Lifetime meters, so a day is the difference between two readings. Never
    -- a sum, and never MAX minus MIN either -- a meter reset would give a
    -- plausible wrong number instead of an obviously wrong negative one.
    r.cumulative_distance_km - LAG(r.cumulative_distance_km) OVER (
        PARTITION BY r.vehicle_id ORDER BY r.rollup_date)
                                                    AS distance_km,
    r.cumulative_energy_kwh - LAG(r.cumulative_energy_kwh) OVER (
        PARTITION BY r.vehicle_id ORDER BY r.rollup_date)
                                                    AS energy_kwh,

    r.full_cycle_equivalents,
    r.charge_sessions_total,
    r.fast_charge_sessions_total,
    r.fast_charge_sessions_total / NULLIF(r.charge_sessions_total, 0)
                                                    AS fast_charge_share

FROM {{ ref('stg_einride_daily_rollup') }} r

LEFT JOIN {{ ref('dim_vehicle') }} v
       ON r.vehicle_id = v.vehicle_id
