CREATE OR REPLACE TABLE einride.gold.dim_customer AS
SELECT
    -- One row per contract version, so the key has to be too.
    md5(cast(concat(coalesce(cast(s.customer_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(s.dbt_valid_from as string), '_dbt_utils_surrogate_key_null_')) as string))
                                                    AS customer_key,
    s.customer_id,

    s.customer_name,
    s.industry,
    CAST(s.contract_start_date AS DATE)             AS contract_start_date,

    s.sla_otif_target_pct,
    CASE
        WHEN s.sla_otif_target_pct >= 99.5  THEN 'premium'
        WHEN s.sla_otif_target_pct >= 98.0 THEN 'standard'
        ELSE 'basic'
    END

                                                    AS sla_tier_derived,
    t.sla_penalty_rate                              AS sla_penalty_rate_derived,

    s.sustainability_reporting_required,
    s.diesel_baseline_gco2_per_tonkm,

    -- The window this version was true for. dbt writes both; valid_to is null
    -- on the version that is current.
    CASE WHEN LAG(s.dbt_valid_from) OVER (PARTITION BY s.customer_id ORDER BY s.dbt_valid_from) IS NULL
         THEN TIMESTAMP '1900-01-01' ELSE s.dbt_valid_from END AS valid_from,
    s.dbt_valid_to                                  AS valid_to,

    -- Same fact as "valid_to is null", kept as a column because
    -- WHERE is_current is much harder to get wrong.
    s.dbt_valid_to IS NULL                          AS is_current

FROM `einride`.`silver`.`snap_customers` s

LEFT JOIN `einride`.`silver`.`stg_einride_sla_tiers` t
       ON
    CASE
        WHEN s.sla_otif_target_pct >= 99.5  THEN 'premium'
        WHEN s.sla_otif_target_pct >= 98.0 THEN 'standard'
        ELSE 'basic'
    END
 = t.sla_tier;

CREATE OR REPLACE TABLE einride.gold.dim_driver AS
SELECT
    md5(cast(concat(coalesce(cast(s.driver_id as string), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(s.dbt_valid_from as string), '_dbt_utils_surrogate_key_null_')) as string))
                                                    AS driver_key,
    s.driver_id,

    s.driver_name,
    s.license_class,
    CAST(s.hire_date AS DATE)                       AS hire_date,

    -- Tenure is measured from today on purpose. It is the answer to "how
    -- experienced is this driver now", not a property of any past version.
    DATEDIFF(CURRENT_DATE(), CAST(s.hire_date AS DATE)) / 365.25
                                                    AS tenure_years,

    s.home_depot_id,
    dep.depot_name                                  AS home_depot_name,
    dep.region                                      AS home_depot_region,

    CASE WHEN LAG(s.dbt_valid_from) OVER (PARTITION BY s.driver_id ORDER BY s.dbt_valid_from) IS NULL
         THEN TIMESTAMP '1900-01-01' ELSE s.dbt_valid_from END AS valid_from,
    s.dbt_valid_to                                  AS valid_to,
    s.dbt_valid_to IS NULL                          AS is_current

FROM `einride`.`silver`.`snap_drivers` s

LEFT JOIN `einride`.`silver`.`stg_einride_depots` dep
       ON s.home_depot_id = dep.depot_id;

CREATE OR REPLACE TABLE einride.gold.fct_shipment AS
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

FROM `einride`.`silver`.`int_einride_shipment_legs` l

LEFT JOIN `einride`.`gold`.`dim_vehicle` v
       ON l.vehicle_id = v.vehicle_id

LEFT JOIN `einride`.`gold`.`dim_route` r
       ON l.route_id = r.route_id

LEFT JOIN `einride`.`gold`.`dim_energy_price` p
       ON p.price_zone = r.route_origin_region
      AND p.hour_of_day = HOUR(l.dispatched_at)
      AND p.day_type =
    -- Swedish tariffs are priced weekday or weekend. Spark counts Sunday as 1
    -- and Saturday as 7. Written once here so the tariff join, the date
    -- dimension and anything else can't disagree about what a weekend is.
    CASE WHEN DAYOFWEEK(l.dispatched_at) IN (1, 7) THEN 'weekend' ELSE 'weekday' END


-- dim_customer is Type 2, so a customer has more than one row and the plain
-- equality join would match every version of them -- doubling the leg, silently.
-- The two window lines are what stop that, and they are also the point: a leg
-- gets the OTIF target that was in force the day it was dispatched.
--
-- coalesce, because valid_to is null on the current version and null compares
-- to nothing.
LEFT JOIN `einride`.`gold`.`dim_customer` c
       ON l.customer_id = c.customer_id
      AND l.dispatched_at >= c.valid_from
      AND l.dispatched_at <  COALESCE(c.valid_to, TIMESTAMP '9999-12-31');

CREATE OR REPLACE TABLE einride.gold.fct_trip AS
SELECT
    t.trip_id,
    t.vehicle_id,
    t.route_id,
    t.shipment_id,
    t.customer_id,
    t.driver_id,
    dr.driver_key,
    dr.driver_name,

    -- What the driver was licensed for on the day of this trip, not today.
    dr.license_class,
    dr.home_depot_id                                AS driver_home_depot_id,

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

FROM `einride`.`silver`.`int_einride_trip_sessions` t

LEFT JOIN `einride`.`gold`.`dim_vehicle` v
       ON t.vehicle_id = v.vehicle_id

LEFT JOIN `einride`.`gold`.`dim_route` r
       ON t.route_id = r.route_id

-- dim_driver is Type 2, so one driver has several rows. Without the two window
-- lines this join matches every version of them and quietly doubles the trip --
-- no error, just every fleet total inflated.
--
-- With them, the trip picks up the licence class and depot that were true when
-- it ran. coalesce because valid_to is null on the current version.
LEFT JOIN `einride`.`gold`.`dim_driver` dr
       ON t.driver_id = dr.driver_id
      AND t.trip_started_at >= dr.valid_from
      AND t.trip_started_at <  COALESCE(dr.valid_to, TIMESTAMP '9999-12-31');

-- Check
SELECT COUNT(*) legs, COUNT(customer_key) with_customer, COUNT(diesel_baseline_kgco2) with_diesel FROM einride.gold.fct_shipment;
SELECT COUNT(*) trips, COUNT(driver_key) with_driver FROM einride.gold.fct_trip;
