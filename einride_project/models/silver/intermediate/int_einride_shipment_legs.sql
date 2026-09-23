-- One row per leg. A leg is one vehicle going from one depot to another,
-- loaded or empty.
--
-- The point of this model is the gap between what was planned and what
-- happened. The plan comes from the shipment record. What happened comes from
-- the telemetry, through the trip it turned into.
--
-- Deadheads are in here. They are empty runs nobody ordered, so they have no
-- shipment_id and no customer, but they burn real kilometres and real energy
-- and a fleet that ignores them is lying to itself about utilisation.

SELECT
    s.dispatch_id,
    s.shipment_id,
    s.customer_id,
    s.vehicle_id,
    s.vehicle_type,
    s.route_id,
    s.shipment_origin_depot_id,
    s.shipment_dest_depot_id,
    s.is_deadhead,

    -- Two plans, not one. planned_distance_km is what was promised for this
    -- leg. distance_km is the route's book figure. They should agree, and when
    -- they do not I want to see it rather than pick one.
    s.planned_distance_km,
    r.distance_km                                       AS route_distance_km,
    r.elevation_gain_m,

    t.trip_id,
    t.trip_distance_km                                  AS actual_distance_km,
    t.trip_energy_kwh                                   AS actual_energy_kwh,

    t.trip_distance_km - s.planned_distance_km          AS distance_variance_km,

    -- Guarded because a plan of zero is possible and would blow up the query.
    ROUND(100.0 * (t.trip_distance_km - s.planned_distance_km)
          / NULLIF(s.planned_distance_km, 0), 1)        AS distance_variance_pct,

    s.cargo_weight_kg,
    s.cargo_weight_kg / 1000.0 * s.planned_distance_km  AS planned_tonne_km,
    s.cargo_weight_kg / 1000.0 * t.trip_distance_km     AS actual_tonne_km,

    s.dispatched_at,
    s.promised_arrival_at,
    s.actual_arrival_at,

    (UNIX_TIMESTAMP(s.actual_arrival_at)
       - UNIX_TIMESTAMP(s.dispatched_at)) / 60.0        AS actual_transit_minutes,
    (UNIX_TIMESTAMP(s.promised_arrival_at)
       - UNIX_TIMESTAMP(s.dispatched_at)) / 60.0        AS promised_transit_minutes,
    (UNIX_TIMESTAMP(s.actual_arrival_at)
       - UNIX_TIMESTAMP(s.promised_arrival_at)) / 60.0  AS lateness_minutes,

    s.delivered_on_time,
    s.actual_arrival_at IS NOT NULL                     AS is_arrived,
    t.trip_id IS NOT NULL                               AS has_telemetry

FROM {{ ref('stg_einride_shipments') }} s

LEFT JOIN {{ ref('stg_einride_routes') }} r
       ON s.route_id = r.route_id

-- LEFT, not INNER. A leg dispatched near the end of the window has no closed
-- trip yet, and dropping it would quietly understate how much was dispatched.
--
-- There is no shared key here. Telemetry never carries dispatch_id, and a
-- deadhead carries no shipment_id either, so the only honest match is the same
-- vehicle on the same route inside the leg's own window.
LEFT JOIN {{ ref('int_einride_trip_sessions') }} t
       ON s.vehicle_id = t.vehicle_id
      AND s.route_id = t.route_id
      AND t.trip_started_at >= s.dispatched_at
      AND t.trip_started_at < COALESCE(s.actual_arrival_at,
                                       s.dispatched_at + INTERVAL 24 HOURS)

-- A vehicle can run the same route twice in one window, and without this the
-- join would fan out and turn one leg into two. Earliest trip inside the
-- window wins, which is the one the leg actually started.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.dispatch_id ORDER BY t.trip_started_at) = 1
