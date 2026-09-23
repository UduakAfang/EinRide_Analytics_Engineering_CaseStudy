-- One row per charging session.
--
-- Cost and carbon are computed here rather than in the intermediate, because
-- both depend on where and when the vehicle charged and that is a join, not a
-- reading. Price and carbon come from the OBT at the session's first ping.

SELECT
    c.charge_session_id,
    c.vehicle_id,

    v.vehicle_type,
    v.oem,
    v.battery_chemistry,
    v.battery_capacity_kwh,
    v.home_depot_id,
    v.home_depot_region,

    c.charge_started_at,
    c.charge_ended_at,
    DATE(c.charge_started_at)                       AS charge_date,
    HOUR(c.charge_started_at)                       AS charge_start_hour,
    c.charge_duration_minutes,

    c.soc_start_pct,
    c.soc_end_pct,
    c.soc_end_pct - c.soc_start_pct                 AS soc_gained_pct,
    c.energy_added_kwh,
    c.peak_charge_power_kw,

    -- Fast charging is the one that ages the battery. 150 kW is where the
    -- depot chargers stop and the highway chargers start.
    c.peak_charge_power_kw >= 150                   AS is_fast_charge,

    ROUND(c.energy_added_kwh * p.price_sek_per_kwh, 2)
                                                    AS charge_cost_sek,
    ROUND(c.energy_added_kwh * p.carbon_intensity_gco2_per_kwh / 1000.0, 2)
                                                    AS charge_carbon_kgco2,

    c.latitude,
    c.longitude,
    c.ping_count,
    c.is_complete

FROM {{ ref('int_einride_charging_sessions') }} c

LEFT JOIN {{ ref('dim_vehicle') }} v
       ON c.vehicle_id = v.vehicle_id

-- One row of the OBT per session, taken at the first ping, purely for the
-- price and carbon that were in force when charging started.
LEFT JOIN {{ ref('obt_telemetry_enriched') }} p
       ON c.charge_session_id = p.charge_session_id
      AND c.charge_started_at = p.event_time
