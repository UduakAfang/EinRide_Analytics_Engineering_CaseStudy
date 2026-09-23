SELECT
    route_id,
    name AS route_name,
    origin AS route_origin_depot_id,
    dest AS route_dest_depot_id,
    distance_km,
    elevation_gain_m

FROM {{source ('bronze', 'routes')}}
