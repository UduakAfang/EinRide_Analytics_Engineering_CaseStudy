-- One row per trip.
--
-- The work already happened in int_einride_trip_sessions. This is the publish
-- layer: keep the measures, add the labels a business user needs, name things
-- the way they would say them.
--
-- Efficiency is the number this table exists for. kWh per km is how an EV
-- fleet is judged, and it is only meaningful per trip -- averaged over a day
-- it hides the cold morning run that cost half a battery.

SELECT
    t.trip_id,
    t.vehicle_id,
    t.route_id,
    t.shipment_id,
    t.customer_id,
    t.driver_id,

    v.vehicle_type,
    v.oem,
    v.home_depot_id,
    v.home_depot_region,

    r.route_name,
    r.route_origin_depot_id,
    r.route_dest_depot_id,
    r.distance_km                                   AS route_distance_km,

    t.trip_started_at,
    t.trip_ended_at,
    DATE(t.trip_started_at)                         AS trip_date,
    t.trip_duration_minutes,

    t.trip_distance_km,
    t.trip_energy_kwh,

    -- Guarded. A trip of zero kilometres is a vehicle that reported twice
    -- without moving, and dividing by it would give infinity, not an error.
    ROUND(t.trip_energy_kwh / NULLIF(t.trip_distance_km, 0), 3)
                                                    AS kwh_per_km,
    ROUND(t.trip_distance_km / NULLIF(t.trip_duration_minutes, 0) * 60, 1)
                                                    AS avg_speed_kmh_derived,

    t.soc_start_pct,
    t.soc_end_pct,
    t.soc_start_pct - t.soc_end_pct                 AS soc_used_pct,

    t.avg_speed_kmh,
    t.max_speed_kmh,
    t.ping_count,
    t.has_ignition_off

FROM {{ ref('int_einride_trip_sessions') }} t

LEFT JOIN {{ ref('dim_vehicle') }} v
       ON t.vehicle_id = v.vehicle_id

LEFT JOIN {{ ref('stg_einride_routes') }} r
       ON t.route_id = r.route_id
