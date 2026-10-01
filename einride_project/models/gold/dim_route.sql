-- One row per route, with both ends named.
--
-- fct_shipment only had route_id, and fct_trip was reading names straight from
-- staging. Both read from here now, so a route has one name everywhere.

SELECT
    r.route_id,
    r.route_name,
    r.distance_km,
    r.elevation_gain_m,

    r.route_origin_depot_id,
    o.depot_name                                    AS route_origin_depot_name,
    o.region                                        AS route_origin_region,

    r.route_dest_depot_id,
    d.depot_name                                    AS route_dest_depot_name,
    d.region                                        AS route_dest_region

FROM {{ ref('stg_einride_routes') }} r

LEFT JOIN {{ ref('stg_einride_depots') }} o
       ON r.route_origin_depot_id = o.depot_id

LEFT JOIN {{ ref('stg_einride_depots') }} d
       ON r.route_dest_depot_id = d.depot_id
