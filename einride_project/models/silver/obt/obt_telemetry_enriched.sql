{{ config(
    materialized = 'incremental',
    unique_key   = 'event_id',
    incremental_strategy = 'merge',
    on_schema_change = 'append_new_columns'
) }}

-- The one big table. One row per telemetry event, same grain as staging, with
-- every reference table already hung off it.
--
-- Nothing is aggregated here. Twelve joins and not one GROUP BY -- that is the
-- whole job. Gold aggregates; this makes sure gold never has to join to bronze
-- again.
--
-- Incremental because it is the only model big enough to care. Everything else
-- rebuilds in seconds.

SELECT
    t.event_id,
    t.event_time,
    DATE(t.event_time)                              AS event_date,
    HOUR(t.event_time)                              AS event_hour,
    t.trigger_type,
    t.source,

    -- vehicle
    t.vehicle_id,
    t.vehicle_type,
    t.oem,
    v.vehicle_model,
    v.vin,
    v.battery_chemistry,
    v.battery_capacity_kwh,
    v.payload_capacity_kg,
    v.in_service_date,
    v.autonomy_level,
    vt.has_driver,
    vt.ping_interval_seconds,
    bs.degradation_rate_per_cycle,
    bs.cycle_life,
    bs.max_charge_rate_c,

    -- home depot
    v.home_depot_id,
    hd.depot_name                                   AS home_depot_name,
    hd.region                                       AS home_depot_region,

    -- driver. Null for a pod, and that is correct, not missing.
    t.driver_id,
    d.driver_name,
    d.license_class,

    -- route and its two ends
    t.route_id,
    r.route_name,
    r.distance_km                                   AS route_distance_km,
    r.elevation_gain_m,
    r.route_origin_depot_id,
    ro.depot_name                                   AS route_origin_depot_name,
    ro.region                                       AS route_origin_region,
    r.route_dest_depot_id,
    rd.depot_name                                   AS route_dest_depot_name,

    -- customer. Null on a deadhead, and that is the point of the column.
    t.customer_id,
    c.customer_name,
    c.industry,
    c.sla_otif_target_pct,
    c.sustainability_reporting_required,
    c.diesel_baseline_gco2_per_tonkm,

    -- the readings
    t.latitude,
    t.longitude,
    t.speed_kmh,
    t.state_of_charge_pct,
    t.state_of_health_pct,
    t.energy_meter_kwh,
    t.distance_meter_km,
    t.battery_temp_c,
    t.ambient_temp_c,
    t.cargo_weight_kg,
    t.is_charging,
    t.charge_power_kw,
    t.charge_session_id,
    t.alerts_json,

    -- autonomy. Null on a truck.
    t.perception_objects_detected,
    t.perception_closest_object_distance_m,
    t.safety_system_health,
    t.safety_fallback_mode_active,
    t.safety_emergency_stop_triggered,
    t.safety_disengagement_count,
    t.safety_teleoperation_active,
    t.mission_status,
    t.safety_confidence_score,

    -- weather, price and carbon. All three are regional facts, pulled hourly
    -- and joined here rather than repeated on every ping.
    w.weather_condition,
    w.air_temp_c                                    AS regional_air_temp_c,
    w.wind_speed_ms,
    w.precipitation_mm,
    w.road_condition,
    wc.consumption_factor,
    wc.regen_efficiency,
    wc.rolling_resistance,

    tf.price_sek_per_kwh,
    gc.carbon_intensity_gco2_per_kwh,

    t.ingested_at

FROM {{ ref('stg_einride_telemetry') }} t

LEFT JOIN {{ ref('int_einride_vehicles') }} v
       ON t.vehicle_id = v.vehicle_id

LEFT JOIN {{ ref('stg_einride_vehicle_types') }} vt
       ON t.vehicle_type = vt.vehicle_type

LEFT JOIN {{ ref('stg_einride_battery_specs') }} bs
       ON v.battery_chemistry = bs.battery_chemistry

LEFT JOIN {{ ref('stg_einride_depots') }} hd
       ON v.home_depot_id = hd.depot_id

LEFT JOIN {{ ref('stg_einride_drivers') }} d
       ON t.driver_id = d.driver_id

LEFT JOIN {{ ref('stg_einride_routes') }} r
       ON t.route_id = r.route_id

LEFT JOIN {{ ref('stg_einride_depots') }} ro
       ON r.route_origin_depot_id = ro.depot_id

LEFT JOIN {{ ref('stg_einride_depots') }} rd
       ON r.route_dest_depot_id = rd.depot_id

LEFT JOIN {{ ref('stg_einride_customers') }} c
       ON t.customer_id = c.customer_id

-- The generator picks weather from the route's origin region, so that is the
-- region to join on. A parked vehicle has no route, so it falls back to its
-- home depot -- otherwise a third of the table would have no weather at all.
LEFT JOIN {{ ref('stg_einride_weather_hourly') }} w
       ON w.region = COALESCE(ro.region, hd.region)
      AND w.observed_at = DATE_TRUNC('HOUR', t.event_time)

LEFT JOIN {{ ref('stg_einride_weather_conditions') }} wc
       ON w.weather_condition = wc.weather_condition

-- Swedish power is priced by zone and hour, and cheaper at the weekend. The
-- depot region doubles as the price zone -- SE1 to SE4 are the same four.
LEFT JOIN {{ ref('stg_einride_tariffs') }} tf
       ON tf.price_zone = COALESCE(ro.region, hd.region)
      AND tf.hour_of_day = HOUR(t.event_time)
      AND tf.day_type = CASE WHEN DAYOFWEEK(t.event_time) IN (1, 7)
                             THEN 'weekend' ELSE 'weekday' END

LEFT JOIN {{ ref('stg_einride_grid_carbon_intensity') }} gc
       ON gc.price_zone = COALESCE(ro.region, hd.region)
      AND gc.hour_of_day = HOUR(t.event_time)

{% if is_incremental() %}
-- Only what arrived since the last run. The one-hour overlap is deliberate:
-- a late ping is common and the merge on event_id makes a re-read harmless.
WHERE t.event_time >= (SELECT MAX(event_time) - INTERVAL 1 HOUR FROM {{ this }})
{% endif %}
