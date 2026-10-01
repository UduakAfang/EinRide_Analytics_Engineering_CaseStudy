-- Not a model. `dbt compile` renders it; it never builds a table.
--
-- Written because the first 1,000 rows of the OBT were all pods, all parked,
-- and it looked like the joins had failed. This is the query that says whether
-- a null is the source's fault, the join's fault, or just what a parked
-- vehicle looks like.
--
-- Read it as: for each vehicle type, what share of rows actually carry a value.

SELECT
    vehicle_type,
    COUNT(*)                                                    AS rows,

    -- these come from the vehicle join. Should be 100% on both.
    ROUND(100.0 * COUNT(vehicle_model)        / COUNT(*), 1)    AS pct_model,
    ROUND(100.0 * COUNT(home_depot_name)      / COUNT(*), 1)    AS pct_home_depot,

    -- driver is null on a pod by design. Trucks should be high.
    ROUND(100.0 * COUNT(driver_name)          / COUNT(*), 1)    AS pct_driver,

    -- route is null whenever the vehicle is parked. Both types, both states.
    ROUND(100.0 * COUNT(route_name)           / COUNT(*), 1)    AS pct_route,
    ROUND(100.0 * COUNT(route_origin_region)  / COUNT(*), 1)    AS pct_route_region,

    -- customer is null on a deadhead. That is the column doing its job.
    ROUND(100.0 * COUNT(customer_name)        / COUNT(*), 1)    AS pct_customer,

    -- ambient is the one that is actually missing. Pods never send it.
    ROUND(100.0 * COUNT(ambient_temp_c)       / COUNT(*), 1)    AS pct_ambient,
    ROUND(100.0 * COUNT(regional_air_temp_c)  / COUNT(*), 1)    AS pct_regional_air,

    -- weather, price and carbon hang off COALESCE(route region, home region),
    -- so they should be 100% on both types. Anything less is a real gap.
    ROUND(100.0 * COUNT(weather_condition)    / COUNT(*), 1)    AS pct_weather,
    ROUND(100.0 * COUNT(price_sek_per_kwh)    / COUNT(*), 1)    AS pct_tariff,
    ROUND(100.0 * COUNT(carbon_intensity_gco2_per_kwh) / COUNT(*), 1) AS pct_carbon,

    -- movement, so the parked rows are visible rather than inferred
    ROUND(100.0 * SUM(CASE WHEN speed_kmh > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_moving,
    ROUND(100.0 * SUM(CASE WHEN is_charging   THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_charging

FROM {{ ref('obt_telemetry_enriched') }}

GROUP BY vehicle_type
