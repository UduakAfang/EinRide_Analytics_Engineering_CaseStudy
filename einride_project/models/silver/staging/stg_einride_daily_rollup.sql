SELECT
    vehicle_id,
    (CAST(date AS DATE))            AS rollup_date,
    end_of_day_soc_pct,
    state_of_health_pct,
    cumulative_energy_kwh,
    cumulative_distance_km,
    full_cycle_equivalents,
    charge_sessions_total,
    fast_charge_sessions_total,
    ingested_by                     AS source

FROM {{source ('bronze', 'daily_rollup')}}
