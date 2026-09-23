-- One row per leg, loaded or empty.
--
-- This does not come from the OBT and should not. A leg dispatched with no
-- telemetry behind it still happened, and the OBT would drop it. Utilisation
-- is a question about what was dispatched, not about what reported.

SELECT
    l.dispatch_id,
    l.shipment_id,
    l.trip_id,
    l.vehicle_id,
    l.customer_id,
    l.route_id,

    v.vehicle_type,
    v.oem,
    v.home_depot_id,

    c.customer_name,
    c.industry,
    c.sla_tier,
    c.sla_otif_target_pct,
    c.sla_penalty_rate,

    l.shipment_origin_depot_id,
    l.shipment_dest_depot_id,
    l.is_deadhead,

    l.dispatched_at,
    DATE(l.dispatched_at)                           AS dispatch_date,
    l.promised_arrival_at,
    l.actual_arrival_at,
    l.promised_transit_minutes,
    l.actual_transit_minutes,
    l.lateness_minutes,
    l.delivered_on_time,
    l.is_arrived,

    l.planned_distance_km,
    l.route_distance_km,
    l.actual_distance_km,
    l.distance_variance_km,
    l.distance_variance_pct,
    l.actual_energy_kwh,

    l.cargo_weight_kg,
    l.planned_tonne_km,
    l.actual_tonne_km,

    -- The sustainability number every customer contract asks for. Diesel would
    -- have emitted this much; the EV leg emitted whatever the grid charged it
    -- with, which is on fct_charging_session, not here.
    ROUND(l.actual_tonne_km * c.diesel_baseline_gco2_per_tonkm / 1000.0, 2)
                                                    AS diesel_baseline_kgco2,

    l.has_telemetry

FROM {{ ref('int_einride_shipment_legs') }} l

LEFT JOIN {{ ref('dim_vehicle') }} v
       ON l.vehicle_id = v.vehicle_id

LEFT JOIN {{ ref('dim_customer') }} c
       ON l.customer_id = c.customer_id
