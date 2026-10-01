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

    c.customer_key,
    c.customer_name,
    c.industry,
    c.sla_tier_derived,
    c.sla_otif_target_pct,
    c.sla_penalty_rate_derived,

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

    -- What diesel would have emitted on this leg, by the customer's own factor.
    -- actual_kgco2 below is what the electric leg did emit. The difference is
    -- the number the sustainability page is about.
    ROUND(l.actual_tonne_km * c.diesel_baseline_gco2_per_tonkm / 1000.0, 2)
                                                    AS diesel_baseline_kgco2,

    -- How full the truck was. Zero on a deadhead, which is the point.
    ROUND(100.0 * l.cargo_weight_kg / NULLIF(v.payload_capacity_kg, 0), 1)
                                                    AS fill_rate_pct,

    -- Energy priced and carbon-counted at the origin zone and the hour the leg
    -- left. Strictly the energy was bought when the truck last charged, which
    -- can be a different hour. This is the simple version, and it says so.
    r.route_origin_region                           AS price_zone,
    ROUND(l.actual_energy_kwh * p.price_sek_per_kwh, 2)
                                                    AS energy_cost_sek,
    ROUND(l.actual_energy_kwh * p.price_sek_per_kwh
          / NULLIF(l.actual_distance_km, 0), 3)     AS energy_cost_per_km_sek,
    ROUND(l.actual_energy_kwh * p.carbon_intensity_gco2_per_kwh / 1000.0, 2)
                                                    AS actual_kgco2,

    l.has_telemetry

FROM {{ ref('int_einride_shipment_legs') }} l

LEFT JOIN {{ ref('dim_vehicle') }} v
       ON l.vehicle_id = v.vehicle_id

LEFT JOIN {{ ref('dim_route') }} r
       ON l.route_id = r.route_id

LEFT JOIN {{ ref('dim_energy_price') }} p
       ON p.price_zone = r.route_origin_region
      AND p.hour_of_day = HOUR(l.dispatched_at)
      AND p.day_type = {{ day_type('l.dispatched_at') }}

-- dim_customer is Type 2, so a customer has more than one row and the plain
-- equality join would match every version of them -- doubling the leg, silently.
-- The two window lines are what stop that, and they are also the point: a leg
-- gets the OTIF target that was in force the day it was dispatched.
--
-- coalesce, because valid_to is null on the current version and null compares
-- to nothing.
LEFT JOIN {{ ref('dim_customer') }} c
       ON l.customer_id = c.customer_id
      AND l.dispatched_at >= c.valid_from
      AND l.dispatched_at <  COALESCE(c.valid_to, TIMESTAMP '9999-12-31')
